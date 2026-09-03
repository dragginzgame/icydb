//! Module: db::executor::projection::materialize::distinct
//! Responsibility: projected-row DISTINCT strategy, bounded state, and output windowing.
//! Does not own: row decoding, scalar expression evaluation, or source ordering.
//! Boundary: consumes compact projected rows plus optional canonical source boundaries.

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            budget::{charge_current_execution_budget, runtime_value_work},
            group::{
                GroupKey, KeyCanonicalError, StableHash, retained_hash_entry_backing_bytes,
                retained_vec_element_backing_bytes, stable_hash_from_digest,
                try_reserve_hash_entry, try_reserve_vec_elements,
            },
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

/// Projected DISTINCT output plus continuation proof owned by the strategy.
pub(super) struct DistinctProjectionPage {
    rows: Vec<RowView>,
    last_emitted_logical: Option<CursorBoundary>,
    has_more: bool,
}

impl DistinctProjectionPage {
    pub(super) fn into_parts(self) -> (Vec<RowView>, Option<CursorBoundary>, bool) {
        (self.rows, self.last_emitted_logical, self.has_more)
    }
}

/// Canonical projected-row key set used only by the complete global build.
struct DistinctProjectionRowSet {
    buckets: HashMap<StableHash, Vec<Value>>,
    entries: usize,
}

impl DistinctProjectionRowSet {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
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
        charge_distinct_key(&canonical)?;
        self.retain_unique_canonical(hash, canonical)?;

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

        charge_distinct_key(&canonical)?;
        self.retain_unique_canonical(hash, canonical)?;

        Ok(true)
    }

    // Retain one caller-proven unique canonical key only after charging and
    // fallibly reserving the complete hash-entry and collision-vector backing.
    fn retain_unique_canonical(
        &mut self,
        hash: StableHash,
        canonical: Value,
    ) -> Result<(), InternalError> {
        let new_hash_bucket = !self.buckets.contains_key(&hash);
        let structural_bytes =
            retained_vec_element_backing_bytes::<Value>().saturating_add(if new_hash_bucket {
                retained_hash_entry_backing_bytes::<StableHash, Vec<Value>>()
            } else {
                0
            });
        charge_distinct_structural_backing(structural_bytes)?;

        if new_hash_bucket {
            try_reserve_hash_entry(&mut self.buckets)?;
        }
        let bucket = self.buckets.entry(hash).or_default();
        try_reserve_vec_elements(bucket, 1)?;
        bucket.push(canonical);
        self.entries = self.entries.saturating_add(1);

        Ok(())
    }
}

struct GlobalDistinctAccumulator {
    distinct_rows: DistinctProjectionRowSet,
    output_rows: Vec<RowView>,
    last_emitted_logical: Option<CursorBoundary>,
    window: ProjectionDistinctWindow,
    output_envelope_full: bool,
}

impl GlobalDistinctAccumulator {
    fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            distinct_rows: DistinctProjectionRowSet::new(),
            output_rows: Vec::new(),
            last_emitted_logical: None,
            window,
            output_envelope_full: false,
        }
    }

    fn consider_row(
        &mut self,
        candidate: DistinctProjectedRow,
        admit_output: &mut impl FnMut(&RowView) -> Result<bool, InternalError>,
    ) -> Result<(), InternalError> {
        if !self.distinct_rows.insert_row(&candidate.row)? {
            return Ok(());
        }

        let distinct_index = self.distinct_rows.entries.saturating_sub(1);
        let within_window = distinct_index >= self.window.offset
            && self
                .window
                .limit
                .is_none_or(|limit| distinct_index < self.window.offset.saturating_add(limit));
        if within_window && !self.output_envelope_full {
            if !admit_output(&candidate.row)? {
                self.output_envelope_full = true;
                return Ok(());
            }
            charge_distinct_output_row(&candidate.row)?;
            let structural_bytes = retained_vec_element_backing_bytes::<RowView>();
            charge_distinct_structural_backing(structural_bytes)?;
            try_reserve_vec_elements(&mut self.output_rows, 1)?;
            self.last_emitted_logical = candidate.boundary;
            self.output_rows.push(candidate.row);
        }

        Ok(())
    }

    fn finish(self) -> DistinctProjectionPage {
        let distinct_count = self.distinct_rows.entries;
        let end = self.window.output_end(distinct_count);
        let has_more = end < distinct_count || self.output_envelope_full;

        DistinctProjectionPage {
            rows: self.output_rows,
            last_emitted_logical: self.last_emitted_logical,
            has_more,
        }
    }
}

struct AdjacentDistinctGroup {
    canonical_key: Value,
    output: Option<DistinctProjectedRow>,
}

struct AdjacentDistinctAccumulator {
    current: Option<AdjacentDistinctGroup>,
    output_rows: Vec<RowView>,
    last_emitted_logical: Option<CursorBoundary>,
    window: ProjectionDistinctWindow,
    unique_rows: usize,
    has_more: bool,
}

impl AdjacentDistinctAccumulator {
    const fn new(window: ProjectionDistinctWindow) -> Self {
        Self {
            current: None,
            output_rows: Vec::new(),
            last_emitted_logical: None,
            window,
            unique_rows: 0,
            has_more: false,
        }
    }

    fn consider_row(
        &mut self,
        candidate: DistinctProjectedRow,
        admit_output: &mut impl FnMut(&RowView) -> Result<bool, InternalError>,
    ) -> Result<bool, InternalError> {
        if let Some(current) = self.current.as_mut()
            && projected_row_matches_canonical(&candidate.row, &current.canonical_key)?
        {
            if let Some(output) = current.output.as_mut() {
                output.boundary = candidate.boundary;
            }
            return Ok(true);
        }

        if !self.close_current_group(admit_output)? {
            self.has_more = true;
            return Ok(false);
        }
        if self
            .window
            .limit
            .is_some_and(|limit| self.output_rows.len() >= limit)
        {
            self.unique_rows = self.unique_rows.saturating_add(1);
            self.has_more = true;
            return Ok(false);
        }

        let canonical_key = canonical_projected_row(&candidate.row)?;
        charge_distinct_key(&canonical_key)?;
        let distinct_index = self.unique_rows;
        self.unique_rows = self.unique_rows.saturating_add(1);
        let output = if distinct_index >= self.window.offset {
            charge_distinct_output_row(&candidate.row)?;
            Some(candidate)
        } else {
            None
        };
        self.current = Some(AdjacentDistinctGroup {
            canonical_key,
            output,
        });

        Ok(true)
    }

    fn close_current_group(
        &mut self,
        admit_output: &mut impl FnMut(&RowView) -> Result<bool, InternalError>,
    ) -> Result<bool, InternalError> {
        let Some(group) = self.current.take() else {
            return Ok(true);
        };
        if let Some(output) = group.output {
            if !admit_output(&output.row)? {
                self.has_more = true;
                return Ok(false);
            }
            let structural_bytes = retained_vec_element_backing_bytes::<RowView>();
            charge_distinct_structural_backing(structural_bytes)?;
            try_reserve_vec_elements(&mut self.output_rows, 1)?;
            self.last_emitted_logical = output.boundary;
            self.output_rows.push(output.row);
        }

        Ok(true)
    }

    fn finish(
        mut self,
        admit_output: &mut impl FnMut(&RowView) -> Result<bool, InternalError>,
    ) -> Result<DistinctProjectionPage, InternalError> {
        let _ = self.close_current_group(admit_output)?;
        Ok(DistinctProjectionPage {
            rows: self.output_rows,
            last_emitted_logical: self.last_emitted_logical,
            has_more: self.has_more,
        })
    }
}

pub(super) fn collect_distinct_projected_rows<I>(
    strategy: ProjectionDistinctStrategy,
    window: ProjectionDistinctWindow,
    rows: impl IntoIterator<Item = I>,
    mut admit_output: impl FnMut(&RowView) -> Result<bool, InternalError>,
    mut project_row: impl FnMut(I) -> Result<DistinctProjectedRow, InternalError>,
) -> Result<DistinctProjectionPage, InternalError> {
    if matches!(window.limit, Some(0)) {
        return Ok(DistinctProjectionPage {
            rows: Vec::new(),
            last_emitted_logical: None,
            has_more: false,
        });
    }

    match strategy {
        ProjectionDistinctStrategy::OrderedAdjacent => {
            let mut accumulator = AdjacentDistinctAccumulator::new(window);
            for row in rows {
                let projected = project_row(row)?;
                if !accumulator.consider_row(projected, &mut admit_output)? {
                    break;
                }
            }
            accumulator.finish(&mut admit_output)
        }
        ProjectionDistinctStrategy::GlobalReplay => {
            let mut accumulator = GlobalDistinctAccumulator::new(window);
            for row in rows {
                let projected = project_row(row)?;
                accumulator.consider_row(projected, &mut admit_output)?;
            }
            Ok(accumulator.finish())
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

fn charge_distinct_key(key: &Value) -> Result<(), InternalError> {
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

    Ok(())
}

fn charge_distinct_output_row(row: &RowView) -> Result<(), InternalError> {
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

    Ok(())
}

fn charge_distinct_structural_backing(bytes: u64) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
        bytes,
    )
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
            collect_distinct_projected_rows(strategy, window, rows, |_| Ok(true), Ok)
                .map_err(QueryError::execute)
        })
    }

    fn text_rows(rows: Vec<RowView>) -> Vec<Vec<Value>> {
        rows.into_iter().map(RowView::into_owned).collect()
    }

    fn collect_with_output_limit(
        strategy: ProjectionDistinctStrategy,
        rows: Vec<DistinctProjectedRow>,
        output_limit: usize,
    ) -> Result<DistinctProjectionPage, QueryError> {
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
        with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
            let mut admitted = 0_usize;
            collect_distinct_projected_rows(
                strategy,
                ProjectionDistinctWindow::new(0, None),
                rows,
                |_| {
                    if admitted >= output_limit {
                        return Ok(false);
                    }
                    admitted = admitted.saturating_add(1);
                    Ok(true)
                },
                Ok,
            )
            .map_err(QueryError::execute)
        })
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
        let (rows, last_emitted, has_more) = first.into_parts();

        assert_eq!(
            text_rows(rows),
            vec![
                vec![Value::Text("a".to_string())],
                vec![Value::Text("b".to_string())],
            ]
        );
        assert_eq!(last_emitted, Some(boundary(4)));
        assert!(has_more);
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
        let (rows, last_emitted, has_more) = resumed.into_parts();

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
        let (rows, last_emitted, has_more) = first.into_parts();

        assert_eq!(
            text_rows(rows),
            vec![
                vec![Value::Text("a".to_string())],
                vec![Value::Text("b".to_string())],
            ]
        );
        assert_eq!(last_emitted, Some(boundary(2)));
        assert!(has_more);
        let second = collect(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(2, Some(2)),
            candidates(),
        )
        .expect("global DISTINCT replay should execute");
        let (rows, last_emitted, has_more) = second.into_parts();

        assert_eq!(text_rows(rows), vec![vec![Value::Text("c".to_string())]]);
        assert_eq!(last_emitted, Some(boundary(4)));
        assert!(!has_more);
    }

    #[test]
    fn distinct_output_admission_preserves_the_last_returned_group_boundary() {
        for strategy in [
            ProjectionDistinctStrategy::OrderedAdjacent,
            ProjectionDistinctStrategy::GlobalReplay,
        ] {
            let page = collect_with_output_limit(
                strategy,
                vec![
                    text_candidate("a", 1),
                    text_candidate("a", 2),
                    text_candidate("b", 3),
                    text_candidate("c", 4),
                ],
                1,
            )
            .expect("output admission should return resumable DISTINCT progress");
            let (rows, last_emitted, has_more) = page.into_parts();

            assert_eq!(text_rows(rows), vec![vec![Value::Text("a".to_string())]]);
            let expected_boundary = match strategy {
                ProjectionDistinctStrategy::OrderedAdjacent => boundary(2),
                ProjectionDistinctStrategy::GlobalReplay => boundary(1),
            };
            assert_eq!(last_emitted, Some(expected_boundary));
            assert!(has_more);
        }
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
        let (rows, last_emitted, has_more) = page.into_parts();

        assert_eq!(rows.len(), 2);
        assert_eq!(last_emitted, Some(boundary(3)));
        assert!(!has_more);
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
                |_| Ok(true),
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

    #[test]
    fn global_distinct_hash_capacity_is_part_of_the_typed_state_budget() {
        let candidate = text_candidate("one", 1);
        let canonical = canonical_projected_row(&candidate.row).expect("canonical key");
        let canonical_bytes = runtime_value_work(&canonical).0;
        let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
            .with_limit_for_tests(
                DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
                canonical_bytes,
            );
        let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
            collect_distinct_projected_rows(
                ProjectionDistinctStrategy::GlobalReplay,
                ProjectionDistinctWindow::new(0, None),
                vec![candidate],
                |_| Ok(true),
                Ok,
            )
            .map_err(QueryError::execute)
        });
        let Err(error) = result else {
            panic!("canonical bytes alone must not admit hidden hash capacity")
        };

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
    }
}
