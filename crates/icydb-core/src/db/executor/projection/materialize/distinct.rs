//! Module: db::executor::projection::materialize::distinct
//! Responsibility: projected-row DISTINCT strategy, bounded state, and output windowing.
//! Does not own: row decoding, scalar expression evaluation, or source ordering.
//! Boundary: consumes compact projected rows plus optional canonical source boundaries.

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            budget::{charge_current_execution_budget, runtime_value_work},
            group::{GroupKey, KeyCanonicalError, StableHash, stable_hash_from_digest},
            projection::materialize::{plan::PreparedProjectionContract, row_view::RowView},
        },
        query::plan::ResolvedOrder,
    },
    error::InternalError,
    value::{Value, ValueHashWriter},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::collections::HashMap;

/// Planner/runtime strategy for projected-row DISTINCT pagination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::projection) enum ProjectionDistinctStrategy {
    /// Complete projected keys are contiguous in source order.
    OrderedAdjacent,
    /// Projected duplicates may recur and require a complete replayable build.
    GlobalReplay,
}

/// Select adjacent DISTINCT only when direct projection slots form the leading
/// source-order equivalence tuple. All expression and hidden-prefix shapes use
/// the complete replayable global build.
pub(in crate::db::executor::projection) fn projection_distinct_strategy(
    projection: &PreparedProjectionContract,
    resolved_order: &ResolvedOrder,
) -> ProjectionDistinctStrategy {
    if projection.projection_is_model_identity() {
        return ProjectionDistinctStrategy::OrderedAdjacent;
    }
    let Some(projected) = projection
        .retained_slot_direct_projection_slots()
        .or_else(|| projection.data_row_direct_projection_slots())
    else {
        return ProjectionDistinctStrategy::GlobalReplay;
    };
    let Some(order_slots) = resolved_order.direct_field_slots() else {
        return ProjectionDistinctStrategy::GlobalReplay;
    };

    let mut projected_slots = Vec::new();
    for projection in projected.projections() {
        let slot = projection.source_slot();
        if !projected_slots.contains(&slot) {
            projected_slots.push(slot);
        }
    }
    if direct_projection_is_leading_order_equivalence(&projected_slots, &order_slots) {
        ProjectionDistinctStrategy::OrderedAdjacent
    } else {
        ProjectionDistinctStrategy::GlobalReplay
    }
}

fn direct_projection_is_leading_order_equivalence(
    projected_slots: &[usize],
    order_slots: &[usize],
) -> bool {
    if projected_slots.is_empty() {
        return false;
    }
    let mut remaining = projected_slots.to_vec();

    for order_slot in order_slots {
        if !projected_slots.contains(order_slot) {
            return false;
        }
        if let Some(position) = remaining.iter().position(|slot| slot == order_slot) {
            remaining.remove(position);
        }
        if remaining.is_empty() {
            return true;
        }
    }

    false
}

/// Output window applied after projected-row deduplication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::projection) struct ProjectionDistinctWindow {
    offset: usize,
    limit: Option<usize>,
}

impl ProjectionDistinctWindow {
    #[must_use]
    pub(in crate::db::executor::projection) const fn new(
        offset: usize,
        limit: Option<usize>,
    ) -> Self {
        Self { offset, limit }
    }

    fn output_end(self, distinct_count: usize) -> usize {
        self.limit.map_or(distinct_count, |limit| {
            self.offset.saturating_add(limit).min(distinct_count)
        })
    }
}

/// One compact projected candidate plus the source row's canonical boundary.
pub(super) struct DistinctProjectedRow {
    row: RowView,
    boundary: Option<CursorBoundary>,
}

impl DistinctProjectedRow {
    #[must_use]
    pub(super) const fn new(row: RowView, boundary: Option<CursorBoundary>) -> Self {
        Self { row, boundary }
    }
}

/// Bounded DISTINCT diagnostics returned with one completed materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DistinctProjectionStats {
    pub(super) strategy: ProjectionDistinctStrategy,
    pub(super) candidate_rows: u64,
    pub(super) unique_rows: u64,
    pub(super) peak_retained_entries: u64,
    pub(super) peak_retained_backing_bytes: u64,
}

/// Projected DISTINCT output plus continuation proof owned by the strategy.
pub(super) struct DistinctProjectionPage {
    rows: Vec<RowView>,
    last_emitted_logical: Option<CursorBoundary>,
    has_more: bool,
    stats: DistinctProjectionStats,
}

impl DistinctProjectionPage {
    pub(super) fn into_parts(
        self,
    ) -> (
        Vec<RowView>,
        Option<CursorBoundary>,
        bool,
        DistinctProjectionStats,
    ) {
        (
            self.rows,
            self.last_emitted_logical,
            self.has_more,
            self.stats,
        )
    }
}

/// Canonical projected-row key set used only by the complete global build.
struct DistinctProjectionRowSet {
    buckets: HashMap<StableHash, Vec<Value>>,
    retained_backing_bytes: u64,
    entries: usize,
}

impl DistinctProjectionRowSet {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            retained_backing_bytes: 0,
            entries: 0,
        }
    }

    // Hash and compare through borrowed values, then own a canonical key only
    // for a genuinely new projected row.
    fn insert_row(&mut self, row: &RowView) -> Result<bool, InternalError> {
        if row
            .values()
            .iter()
            .any(value_requires_owned_canonical_lookup)
        {
            return self.insert_row_with_owned_canonicalization(row);
        }

        let hash =
            stable_hash_projected_row(row).map_err(KeyCanonicalError::into_internal_error)?;
        if self
            .buckets
            .get(&hash)
            .is_some_and(|bucket| bucket.iter().any(|key| projected_row_matches_key(row, key)))
        {
            return Ok(false);
        }

        let canonical = GroupKey::from_group_values(row.values().to_vec())
            .map_err(KeyCanonicalError::into_internal_error)?
            .into_canonical_value();
        let canonical_bytes = charge_distinct_key(&canonical)?;
        self.buckets.entry(hash).or_default().push(canonical);
        self.entries = self.entries.saturating_add(1);
        self.retained_backing_bytes = self.retained_backing_bytes.saturating_add(canonical_bytes);

        Ok(true)
    }

    fn insert_row_with_owned_canonicalization(
        &mut self,
        row: &RowView,
    ) -> Result<bool, InternalError> {
        let key = GroupKey::from_group_values(row.values().to_vec())
            .map_err(KeyCanonicalError::into_internal_error)?;
        let hash = key.hash();
        let canonical = key.into_canonical_value();
        if self
            .buckets
            .get(&hash)
            .is_some_and(|bucket| bucket.iter().any(|existing| existing == &canonical))
        {
            return Ok(false);
        }

        let canonical_bytes = charge_distinct_key(&canonical)?;
        self.buckets.entry(hash).or_default().push(canonical);
        self.entries = self.entries.saturating_add(1);
        self.retained_backing_bytes = self.retained_backing_bytes.saturating_add(canonical_bytes);

        Ok(true)
    }
}

struct GlobalDistinctAccumulator {
    distinct_rows: DistinctProjectionRowSet,
    output_rows: Vec<RowView>,
    output_row_backing_bytes: u64,
    last_emitted_logical: Option<CursorBoundary>,
    window: ProjectionDistinctWindow,
    candidate_rows: u64,
    peak_retained_backing_bytes: u64,
}

impl GlobalDistinctAccumulator {
    fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            distinct_rows: DistinctProjectionRowSet::new(),
            output_rows: Vec::with_capacity(window.limit.unwrap_or(0)),
            output_row_backing_bytes: 0,
            last_emitted_logical: None,
            window,
            candidate_rows: 0,
            peak_retained_backing_bytes: 0,
        }
    }

    fn consider_row(&mut self, candidate: DistinctProjectedRow) -> Result<(), InternalError> {
        self.candidate_rows = self.candidate_rows.saturating_add(1);
        if !self.distinct_rows.insert_row(&candidate.row)? {
            return Ok(());
        }

        let distinct_index = self.distinct_rows.entries.saturating_sub(1);
        let within_window = distinct_index >= self.window.offset
            && self
                .window
                .limit
                .is_none_or(|limit| distinct_index < self.window.offset.saturating_add(limit));
        if within_window {
            let row_bytes = charge_distinct_output_row(&candidate.row)?;
            self.output_row_backing_bytes = self.output_row_backing_bytes.saturating_add(row_bytes);
            self.last_emitted_logical = candidate.boundary;
            self.output_rows.push(candidate.row);
        }
        self.peak_retained_backing_bytes = self.peak_retained_backing_bytes.max(
            self.distinct_rows
                .retained_backing_bytes
                .saturating_add(self.output_row_backing_bytes),
        );

        Ok(())
    }

    fn finish(self, mut record_bounded_stop: impl FnMut()) -> DistinctProjectionPage {
        let distinct_count = self.distinct_rows.entries;
        let end = self.window.output_end(distinct_count);
        let has_more = end < distinct_count;
        if has_more {
            record_bounded_stop();
        }

        DistinctProjectionPage {
            rows: self.output_rows,
            last_emitted_logical: self.last_emitted_logical,
            has_more,
            stats: DistinctProjectionStats {
                strategy: ProjectionDistinctStrategy::GlobalReplay,
                candidate_rows: self.candidate_rows,
                unique_rows: u64::try_from(distinct_count).unwrap_or(u64::MAX),
                peak_retained_entries: u64::try_from(self.distinct_rows.entries)
                    .unwrap_or(u64::MAX),
                peak_retained_backing_bytes: self.peak_retained_backing_bytes,
            },
        }
    }
}

struct AdjacentDistinctGroup {
    canonical_key: Value,
    canonical_key_bytes: u64,
    output: Option<DistinctProjectedRow>,
    output_bytes: u64,
}

struct AdjacentDistinctAccumulator {
    current: Option<AdjacentDistinctGroup>,
    output_rows: Vec<RowView>,
    output_backing_bytes: u64,
    last_emitted_logical: Option<CursorBoundary>,
    window: ProjectionDistinctWindow,
    candidate_rows: u64,
    unique_rows: u64,
    peak_retained_entries: u64,
    peak_retained_backing_bytes: u64,
    has_more: bool,
}

impl AdjacentDistinctAccumulator {
    fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            current: None,
            output_rows: Vec::with_capacity(window.limit.unwrap_or(0)),
            output_backing_bytes: 0,
            last_emitted_logical: None,
            window,
            candidate_rows: 0,
            unique_rows: 0,
            peak_retained_entries: 0,
            peak_retained_backing_bytes: 0,
            has_more: false,
        }
    }

    fn consider_row(
        &mut self,
        candidate: DistinctProjectedRow,
        mut record_bounded_stop: impl FnMut(),
    ) -> Result<bool, InternalError> {
        self.candidate_rows = self.candidate_rows.saturating_add(1);
        if let Some(current) = self.current.as_mut()
            && projected_row_matches_canonical(&candidate.row, &current.canonical_key)?
        {
            if let Some(output) = current.output.as_mut() {
                output.boundary = candidate.boundary;
            }
            return Ok(true);
        }

        self.close_current_group();
        if self
            .window
            .limit
            .is_some_and(|limit| self.output_rows.len() >= limit)
        {
            self.unique_rows = self.unique_rows.saturating_add(1);
            self.has_more = true;
            record_bounded_stop();
            return Ok(false);
        }

        let canonical_key = canonical_projected_row(&candidate.row)?;
        let canonical_key_bytes = charge_distinct_key(&canonical_key)?;
        let distinct_index = usize::try_from(self.unique_rows).unwrap_or(usize::MAX);
        self.unique_rows = self.unique_rows.saturating_add(1);
        let (output, output_bytes) = if distinct_index >= self.window.offset {
            let output_bytes = charge_distinct_output_row(&candidate.row)?;
            (Some(candidate), output_bytes)
        } else {
            (None, 0)
        };
        self.current = Some(AdjacentDistinctGroup {
            canonical_key,
            canonical_key_bytes,
            output,
            output_bytes,
        });
        self.record_peak();

        Ok(true)
    }

    fn close_current_group(&mut self) {
        let Some(group) = self.current.take() else {
            return;
        };
        if let Some(output) = group.output {
            self.last_emitted_logical = output.boundary;
            self.output_rows.push(output.row);
            self.output_backing_bytes =
                self.output_backing_bytes.saturating_add(group.output_bytes);
        }
    }

    fn record_peak(&mut self) {
        let current_entries = usize::from(self.current.is_some());
        let current_backing = self.current.as_ref().map_or(0, |group| {
            group.canonical_key_bytes.saturating_add(group.output_bytes)
        });
        self.peak_retained_entries = self
            .peak_retained_entries
            .max(u64::try_from(current_entries).unwrap_or(u64::MAX));
        self.peak_retained_backing_bytes = self
            .peak_retained_backing_bytes
            .max(self.output_backing_bytes.saturating_add(current_backing));
    }

    fn finish(mut self) -> DistinctProjectionPage {
        self.close_current_group();
        DistinctProjectionPage {
            rows: self.output_rows,
            last_emitted_logical: self.last_emitted_logical,
            has_more: self.has_more,
            stats: DistinctProjectionStats {
                strategy: ProjectionDistinctStrategy::OrderedAdjacent,
                candidate_rows: self.candidate_rows,
                unique_rows: self.unique_rows,
                peak_retained_entries: self.peak_retained_entries,
                peak_retained_backing_bytes: self.peak_retained_backing_bytes,
            },
        }
    }
}

pub(super) fn collect_distinct_projected_rows<I>(
    strategy: ProjectionDistinctStrategy,
    window: ProjectionDistinctWindow,
    rows: impl IntoIterator<Item = I>,
    mut record_candidate_row: impl FnMut(),
    mut record_bounded_stop: impl FnMut(),
    mut project_row: impl FnMut(I) -> Result<DistinctProjectedRow, InternalError>,
) -> Result<DistinctProjectionPage, InternalError> {
    if matches!(window.limit, Some(0)) {
        return Ok(DistinctProjectionPage {
            rows: Vec::new(),
            last_emitted_logical: None,
            has_more: false,
            stats: DistinctProjectionStats {
                strategy,
                candidate_rows: 0,
                unique_rows: 0,
                peak_retained_entries: 0,
                peak_retained_backing_bytes: 0,
            },
        });
    }

    match strategy {
        ProjectionDistinctStrategy::OrderedAdjacent => {
            let mut accumulator = AdjacentDistinctAccumulator::new(window);
            for row in rows {
                let projected = project_row(row)?;
                record_candidate_row();
                if !accumulator.consider_row(projected, &mut record_bounded_stop)? {
                    break;
                }
            }
            Ok(accumulator.finish())
        }
        ProjectionDistinctStrategy::GlobalReplay => {
            let mut accumulator = GlobalDistinctAccumulator::new(window);
            for row in rows {
                let projected = project_row(row)?;
                record_candidate_row();
                accumulator.consider_row(projected)?;
            }
            Ok(accumulator.finish(record_bounded_stop))
        }
    }
}

fn canonical_projected_row(row: &RowView) -> Result<Value, InternalError> {
    GroupKey::from_group_values(row.values().to_vec())
        .map(GroupKey::into_canonical_value)
        .map_err(KeyCanonicalError::into_internal_error)
}

fn projected_row_matches_canonical(
    row: &RowView,
    canonical: &Value,
) -> Result<bool, InternalError> {
    if row
        .values()
        .iter()
        .any(value_requires_owned_canonical_lookup)
    {
        return canonical_projected_row(row).map(|row_key| row_key == *canonical);
    }

    Ok(projected_row_matches_key(row, canonical))
}

fn charge_distinct_key(key: &Value) -> Result<u64, InternalError> {
    let (bytes, nested_steps) = runtime_value_work(key);
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::GroupDistinctEntries, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
        bytes,
    )?;

    Ok(bytes)
}

fn charge_distinct_output_row(row: &RowView) -> Result<u64, InternalError> {
    let bytes = row.estimated_backing_bytes();
    let nested_steps = row.values().iter().fold(0_u64, |total, value| {
        total.saturating_add(runtime_value_work(value).1)
    });
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::NestedValueSteps,
        nested_steps,
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
        bytes,
    )?;

    Ok(bytes)
}

fn stable_hash_projected_row(row: &RowView) -> Result<StableHash, KeyCanonicalError> {
    let mut hash_writer = ValueHashWriter::new();
    hash_writer.write_list_prefix(row.values().len());
    for (index, value) in row.values().iter().enumerate() {
        hash_writer
            .write_list_value(value)
            .map_err(|_| KeyCanonicalError::projected_row_hashing_failed(index, value))?;
    }

    Ok(stable_hash_from_digest(hash_writer.finish()))
}

fn value_requires_owned_canonical_lookup(value: &Value) -> bool {
    match value {
        Value::Map(_) => true,
        Value::List(items) => items.iter().any(value_requires_owned_canonical_lookup),
        _ => false,
    }
}

fn projected_row_matches_key(row: &RowView, key: &Value) -> bool {
    let Value::List(key_values) = key else {
        return false;
    };
    if row.values().len() != key_values.len() {
        return false;
    }

    key_values
        .iter()
        .enumerate()
        .all(|(index, canonical)| value_matches_canonical_key(row.get(index), canonical))
}

fn value_matches_canonical_key(value: &Value, canonical: &Value) -> bool {
    match (value, canonical) {
        (Value::Decimal(value), Value::Decimal(canonical)) => value.normalize() == *canonical,
        (Value::List(values), Value::List(canonical_values)) => {
            values.len() == canonical_values.len()
                && values
                    .iter()
                    .zip(canonical_values)
                    .all(|(value, canonical)| value_matches_canonical_key(value, canonical))
        }
        (Value::Map(_), _) => false,
        _ => value == canonical,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        QueryError,
        cursor::CursorBoundarySlot,
        executor::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
        RuntimeBoundaryCode,
    };

    const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
    const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
        DiagnosticExecutionBudgetScope::Execution,
        DiagnosticExecutionLane::TrustedRead,
        0x2220_0007_0000_0001,
    );

    fn boundary(value: u64) -> CursorBoundary {
        CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Nat64(value))],
        }
    }

    fn text_candidate(value: &str, source_order: u64) -> DistinctProjectedRow {
        DistinctProjectedRow::new(
            RowView::owned(vec![Value::Text(value.to_string())]),
            Some(boundary(source_order)),
        )
    }

    fn collect(
        strategy: ProjectionDistinctStrategy,
        window: ProjectionDistinctWindow,
        rows: Vec<DistinctProjectedRow>,
    ) -> Result<DistinctProjectionPage, QueryError> {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
            collect_distinct_projected_rows(strategy, window, rows, || {}, || {}, Ok)
                .map_err(QueryError::execute)
        })
    }

    fn text_rows(rows: Vec<RowView>) -> Vec<Vec<Value>> {
        rows.into_iter().map(RowView::into_owned).collect()
    }

    #[test]
    fn direct_projection_requires_a_complete_leading_order_equivalence() {
        assert!(direct_projection_is_leading_order_equivalence(
            &[1],
            &[1, 0]
        ));
        assert!(direct_projection_is_leading_order_equivalence(
            &[1, 2],
            &[2, 1, 0]
        ));
        assert!(!direct_projection_is_leading_order_equivalence(
            &[1],
            &[0, 1]
        ));
        assert!(!direct_projection_is_leading_order_equivalence(
            &[1, 2],
            &[1, 0, 2]
        ));
    }

    #[test]
    fn adjacent_distinct_closes_the_emitted_group_before_its_cursor_boundary() {
        let first = collect(
            ProjectionDistinctStrategy::OrderedAdjacent,
            ProjectionDistinctWindow::new(0, Some(2)),
            vec![
                text_candidate("a", 1),
                text_candidate("a", 2),
                text_candidate("b", 3),
                text_candidate("b", 4),
                text_candidate("c", 5),
                text_candidate("c", 6),
            ],
        )
        .expect("adjacent DISTINCT page should execute");
        let (rows, last_emitted, has_more, stats) = first.into_parts();

        assert_eq!(
            text_rows(rows),
            vec![
                vec![Value::Text("a".to_string())],
                vec![Value::Text("b".to_string())],
            ]
        );
        assert_eq!(last_emitted, Some(boundary(4)));
        assert!(has_more);
        assert_eq!(stats.candidate_rows, 5);
        assert_eq!(stats.unique_rows, 3);
        assert_eq!(stats.peak_retained_entries, 1);

        let resumed = collect(
            ProjectionDistinctStrategy::OrderedAdjacent,
            ProjectionDistinctWindow::new(0, Some(2)),
            vec![
                text_candidate("c", 5),
                text_candidate("c", 6),
                text_candidate("d", 7),
            ],
        )
        .expect("adjacent DISTINCT resume should execute");
        let (rows, last_emitted, has_more, _) = resumed.into_parts();

        assert_eq!(
            text_rows(rows),
            vec![
                vec![Value::Text("c".to_string())],
                vec![Value::Text("d".to_string())],
            ]
        );
        assert_eq!(last_emitted, Some(boundary(7)));
        assert!(!has_more);
    }

    #[test]
    fn global_distinct_replay_handles_nonadjacent_duplicates_across_pages() {
        let candidates = || {
            vec![
                text_candidate("a", 1),
                text_candidate("b", 2),
                text_candidate("a", 3),
                text_candidate("c", 4),
                text_candidate("b", 5),
            ]
        };
        let first = collect(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(0, Some(2)),
            candidates(),
        )
        .expect("first global DISTINCT page should execute");
        let (rows, last_emitted, has_more, stats) = first.into_parts();

        assert_eq!(
            text_rows(rows),
            vec![
                vec![Value::Text("a".to_string())],
                vec![Value::Text("b".to_string())],
            ]
        );
        assert_eq!(last_emitted, Some(boundary(2)));
        assert!(has_more);
        assert_eq!(stats.candidate_rows, 5);
        assert_eq!(stats.unique_rows, 3);
        assert_eq!(stats.peak_retained_entries, 3);

        let second = collect(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(2, Some(2)),
            candidates(),
        )
        .expect("global DISTINCT replay should execute");
        let (rows, last_emitted, has_more, _) = second.into_parts();

        assert_eq!(text_rows(rows), vec![vec![Value::Text("c".to_string())]]);
        assert_eq!(last_emitted, Some(boundary(4)));
        assert!(!has_more);
    }

    #[test]
    fn global_distinct_owns_canonical_null_and_nested_keys() {
        let nested = Value::List(vec![Value::Map(vec![(
            Value::Text("key".to_string()),
            Value::Nat64(7),
        )])]);
        let candidates = vec![
            DistinctProjectedRow::new(
                RowView::owned(vec![Value::Null, nested.clone()]),
                Some(boundary(1)),
            ),
            DistinctProjectedRow::new(
                RowView::owned(vec![Value::Null, nested.clone()]),
                Some(boundary(2)),
            ),
            DistinctProjectedRow::new(
                RowView::owned(vec![Value::Text("x".to_string()), nested]),
                Some(boundary(3)),
            ),
        ];
        let page = collect(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(0, None),
            candidates,
        )
        .expect("nested DISTINCT keys should execute");
        let (rows, last_emitted, has_more, stats) = page.into_parts();

        assert_eq!(rows.len(), 2);
        assert_eq!(last_emitted, Some(boundary(3)));
        assert!(!has_more);
        assert_eq!(stats.unique_rows, 2);
        assert!(stats.peak_retained_backing_bytes > 0);
    }

    #[test]
    fn distinct_state_exhaustion_returns_a_typed_hard_budget_error() {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
                0,
            );
        let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
            collect_distinct_projected_rows(
                ProjectionDistinctStrategy::GlobalReplay,
                ProjectionDistinctWindow::new(0, None),
                vec![text_candidate("too-large", 1)],
                || {},
                || {},
                Ok,
            )
            .map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("DISTINCT state must obey the hard budget")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }
}
