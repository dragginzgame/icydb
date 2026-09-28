//! Module: executor::order
//! Responsibility: shared structural ordering helpers for executor row paths.
//! Does not own: planner order semantics or cursor wire validation.
//! Boundary: consumes planner-resolved order contracts and applies canonical ordering over slot-readable rows.

use crate::{
    db::{
        cursor::{CursorBoundary, CursorBoundarySlot, apply_order_direction},
        data::{CanonicalSlotReader, DataRow},
        executor::{
            budget::{charge_current_execution_budget, charge_sort_work, runtime_value_work},
            projection::eval_compiled_expr_with_value_reader,
            terminal::RowLayout,
        },
        numeric::canonical_value_compare,
        query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderValueSource},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{array, borrow::Cow, cmp::Ordering};

const INLINE_ORDER_VALUE_CAPACITY: usize = 2;
const BOUNDED_ORDER_INITIAL_CAPACITY: usize = 64;

///
/// OrderReadableRow
///
/// Structural executor row contract used by shared ordering logic.
/// Implementors expose slot-indexed values without re-entering typed entity
/// comparators in sort and cursor-boundary hot loops.
///

pub(in crate::db::executor) trait OrderReadableRow {
    /// Borrow one slot value directly when the row owns stable decoded slots.
    ///
    /// This keeps direct-slot ordering from constructing `Cow` wrappers in
    /// comparator hot loops.
    fn read_order_slot_ref(&self, slot: usize) -> Option<&Value>;

    /// Read one slot value for structural ordering and predicate evaluation.
    /// Structural row paths may return borrowed values so shared order/cursor
    /// helpers do not clone already-decoded slots in comparator hot loops.
    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>>;

    /// Return whether direct field-slot reads are stable borrowed row views.
    ///
    /// Row types that synthesize values on demand must keep the default so
    /// ordering caches their owned values once instead of rebuilding them in
    /// every comparator call.
    fn order_slots_are_borrowed(&self) -> bool {
        false
    }

    /// Estimate the complete owned backing kept alive when this row crosses
    /// the blocking-order boundary. Implementors with indirect allocations
    /// must override the inline-size default.
    fn retained_order_backing_bytes(&self) -> u64 {
        u64::try_from(std::mem::size_of_val(self)).unwrap_or(u64::MAX)
    }

    /// Read one slot value as an owned payload when a caller still needs to
    /// leave the borrowed structural-ordering boundary.
    fn read_order_slot(&self, slot: usize) -> Option<Value> {
        self.read_order_slot_cow(slot).map(Cow::into_owned)
    }
}

// Cache a small ORDER BY tuple inline so common single-field and two-field
// sorts do not heap-allocate one key vector per retained row.
enum CachedOrderValues {
    Inline {
        len: usize,
        values: [Option<Value>; INLINE_ORDER_VALUE_CAPACITY],
    },
    Heap(Vec<Option<Value>>),
}

impl CachedOrderValues {
    fn with_capacity(field_count: usize) -> Self {
        if field_count <= INLINE_ORDER_VALUE_CAPACITY {
            Self::Inline {
                len: 0,
                values: array::from_fn(|_| None),
            }
        } else {
            Self::Heap(Vec::with_capacity(field_count))
        }
    }

    fn push(&mut self, value: Option<Value>) {
        // SQL NULL produced by an expression is represented as `Value::Null`,
        // while a nullable stored slot is absent. Ordering and cursor
        // boundaries must use one canonical missing-slot representation.
        let value = match value {
            Some(Value::Null) | None => None,
            value => value,
        };
        match self {
            Self::Inline { len, values } => {
                debug_assert!(
                    *len < INLINE_ORDER_VALUE_CAPACITY,
                    "inline order-value buffer overflowed declared capacity",
                );
                values[*len] = value;
                *len += 1;
            }
            Self::Heap(values) => values.push(value),
        }
    }

    fn into_values(self) -> impl Iterator<Item = Option<Value>> {
        let values = match self {
            Self::Inline { len, values } => values.into_iter().take(len).collect(),
            Self::Heap(values) => values,
        };

        values.into_iter()
    }

    fn estimated_backing_bytes(&self) -> u64 {
        let values: &[Option<Value>] = match self {
            Self::Inline { len, values } => &values[..*len],
            Self::Heap(values) => values.as_slice(),
        };

        values.iter().flatten().fold(0_u64, |total, value| {
            total.saturating_add(runtime_value_work(value).0)
        })
    }
}

/// Apply canonical in-memory ordering with an optional bounded top-k window.
pub(in crate::db::executor) fn apply_structural_order_window<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    if let Some(keep_count) = keep_count
        && keep_count == 0
    {
        rows.clear();
        return Ok(());
    }

    if rows.len() <= 1 {
        return Ok(());
    }
    charge_sort_work::<R>(rows.len())?;
    apply_structural_order_window_inner(rows, resolved_order, keep_count)
}

fn apply_structural_order_window_inner<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    // Phase 1: pure direct-slot orders over retained executor rows can compare
    // borrowed values directly. This avoids materializing owned order keys for
    // the common `ORDER BY field[, id]` path while preserving the existing
    // cached fallback for expression orders and rows that synthesize values.
    if can_use_borrowed_direct_order_path(rows.as_slice(), resolved_order) {
        apply_borrowed_direct_order_window(rows, resolved_order, keep_count);
        return Ok(());
    }

    // Phase 2: cache resolved order values once per row so bounded selection
    // and final sort do not re-read sparse slots or re-run expression-order
    // derivation inside comparator hot loops.
    let source_rows = std::mem::take(rows);
    let cached_values = source_rows
        .iter()
        .map(|row| cache_order_values_from_row(row, resolved_order))
        .collect::<Vec<_>>();
    let mut ordered_indices = (0..source_rows.len()).collect::<Vec<_>>();

    // Phase 3: retain only the bounded canonical window when pagination
    // exposes one, using the cached order keys instead of live row reads.
    if let Some(keep_count) = keep_count
        && ordered_indices.len() > keep_count
    {
        ordered_indices.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_cached_order_indices(&cached_values, *left, *right, resolved_order)
        });
        ordered_indices.truncate(keep_count);
    }

    // Phase 4: sort compact source positions, then move each retained row at
    // most once into final order. Sorting complete kernel payloads repeats
    // large row moves without contributing to comparison semantics.
    ordered_indices.sort_by(|left, right| {
        compare_cached_order_indices(&cached_values, *left, *right, resolved_order)
    });
    *rows = reorder_rows_by_original_indices(source_rows, ordered_indices.as_slice())?;

    Ok(())
}

fn compare_cached_order_indices(
    cached_values: &[CachedOrderValues],
    left: usize,
    right: usize,
    resolved_order: &ResolvedOrder,
) -> Ordering {
    match (cached_values.get(left), cached_values.get(right)) {
        (Some(left), Some(right)) => compare_cached_orderable_rows(left, right, resolved_order),
        _ => left.cmp(&right),
    }
}

fn reorder_rows_by_original_indices<R>(
    rows: Vec<R>,
    ordered_original_indices: &[usize],
) -> Result<Vec<R>, InternalError> {
    let mut source_rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    let mut ordered_rows = Vec::with_capacity(ordered_original_indices.len());
    for original in ordered_original_indices {
        let row = source_rows
            .get_mut(*original)
            .and_then(Option::take)
            .ok_or_else(InternalError::query_executor_invariant)?;
        ordered_rows.push(row);
    }

    Ok(ordered_rows)
}

///
/// PendingOrderRows
///
/// Rows retained by a structural scan before canonical post-access ordering.
/// Expression-order rows remain inseparably paired with their evaluated
/// values and originating order contract until that phase consumes them.
///

pub(in crate::db::executor) struct PendingOrderRows<R> {
    storage: PendingOrderRowStorage<R>,
}

impl<R> PendingOrderRows<R> {
    /// Wrap rows that carry no scan-evaluated expression-order values.
    #[must_use]
    pub(in crate::db::executor) const fn plain(rows: Vec<R>) -> Self {
        Self {
            storage: PendingOrderRowStorage::Plain(rows),
        }
    }

    /// Apply canonical ordering, consuming any scan-evaluated order values.
    ///
    /// # Errors
    ///
    /// Returns a query-executor invariant error when cached values were
    /// produced under a different resolved order or bounded keep count.
    pub(in crate::db::executor) fn apply_order(
        self,
        resolved_order: &ResolvedOrder,
        keep_count: Option<usize>,
    ) -> Result<Vec<R>, InternalError>
    where
        R: OrderReadableRow,
    {
        match self.storage {
            PendingOrderRowStorage::Plain(mut rows) => {
                apply_structural_order_window(&mut rows, resolved_order, keep_count)?;
                Ok(rows)
            }
            PendingOrderRowStorage::Cached {
                resolved_order: cached_order,
                mut rows,
                keep_count: cached_keep_count,
            } => {
                if &cached_order != resolved_order
                    || keep_count != Some(cached_keep_count)
                    || rows.len() > cached_keep_count
                {
                    return Err(InternalError::query_executor_invariant());
                }

                let rows_sorted = rows.len();
                if rows_sorted > 1 {
                    charge_sort_work::<R>(rows_sorted)?;
                    rows.sort_by(|left, right| {
                        compare_cached_orderable_rows(&left.1, &right.1, resolved_order)
                    });
                }

                Ok(rows.into_iter().map(|(row, _)| row).collect())
            }
        }
    }

    /// Borrow plain rows when no scan-evaluated order values are attached.
    #[must_use]
    pub(in crate::db::executor) const fn plain_rows(&self) -> Option<&[R]> {
        match &self.storage {
            PendingOrderRowStorage::Plain(rows) => Some(rows.as_slice()),
            PendingOrderRowStorage::Cached { .. } => None,
        }
    }

    /// Return the number of retained rows independent of storage strategy.
    #[must_use]
    pub(in crate::db::executor) const fn retained_count(&self) -> usize {
        match &self.storage {
            PendingOrderRowStorage::Plain(rows) => rows.len(),
            PendingOrderRowStorage::Cached { rows, .. } => rows.len(),
        }
    }
    /// Consume rows that must not carry pending expression-order values.
    ///
    /// # Errors
    ///
    /// Returns a query-executor invariant error when canonical ordering has
    /// not yet consumed cached expression-order values.
    pub(in crate::db::executor) fn into_plain_rows(self) -> Result<Vec<R>, InternalError> {
        match self.storage {
            PendingOrderRowStorage::Plain(rows) => Ok(rows),
            PendingOrderRowStorage::Cached { .. } => Err(InternalError::query_executor_invariant()),
        }
    }
}

/// Internal storage for rows awaiting canonical structural ordering.
enum PendingOrderRowStorage<R> {
    /// Rows without scan-evaluated expression-order values.
    Plain(Vec<R>),
    /// Bounded rows paired with values evaluated under one exact contract.
    Cached {
        resolved_order: ResolvedOrder,
        rows: Vec<(R, CachedOrderValues)>,
        keep_count: usize,
    },
}

///
/// BoundedOrderWindow
///
/// BoundedOrderWindow retains the best `keep_count` rows while a scan is still
/// running. Direct-field orders keep borrowed comparisons; expression-backed
/// orders cache each candidate's complete resolved ordering tuple once.
/// It captures the resolved order used to choose the strategy so later pushes
/// cannot supply a different comparison contract.
/// It deliberately does not final-sort rows; the canonical post-access
/// order/window phase remains the final ordering authority.
///

pub(in crate::db::executor) struct BoundedOrderWindow<'a, R> {
    resolved_order: &'a ResolvedOrder,
    candidates: BoundedOrderCandidates<R>,
}

///
/// DataRowOrderWindow
///
/// DataRowOrderWindow performs incompatible-order selection while raw rows
/// are scanned. Bounded queries retain only the winning output rows plus their
/// compact canonical order tuples; unbounded queries retain the complete set
/// required by full-sort semantics.
///

pub(in crate::db::executor) struct DataRowOrderWindow<'a> {
    row_layout: RowLayout,
    resolved_order: &'a ResolvedOrder,
    candidates: DataRowOrderCandidates,
}

impl<'a> DataRowOrderWindow<'a> {
    /// Build one raw-row ordering window from the semantic page bound.
    #[must_use]
    pub(in crate::db::executor) fn new(
        row_layout: RowLayout,
        resolved_order: &'a ResolvedOrder,
        keep_count: Option<usize>,
    ) -> Self {
        let candidates = keep_count.map_or_else(
            || DataRowOrderCandidates::Complete {
                rows: Vec::new(),
                retained_backing_bytes: 0,
            },
            |keep_count| DataRowOrderCandidates::Bounded(BoundedOrderRows::new(keep_count)),
        );

        Self {
            row_layout,
            resolved_order,
            candidates,
        }
    }

    /// Evaluate and retain one candidate under the captured order contract.
    pub(in crate::db::executor) fn push(
        &mut self,
        candidate: DataRow,
    ) -> Result<(), InternalError> {
        let cached_values =
            cache_order_values_from_data_row(&candidate, &self.row_layout, self.resolved_order)?;
        let retained_count = self.retained_count();
        let comparisons = match &self.candidates {
            DataRowOrderCandidates::Bounded(window) if retained_count != 0 => {
                if retained_count < window.keep_count {
                    1
                } else {
                    retained_count.saturating_add(1)
                }
            }
            DataRowOrderCandidates::Bounded(_) | DataRowOrderCandidates::Complete { .. } => 0,
        };
        let retained_backing_bytes = data_row_retained_backing_bytes(&candidate)
            .saturating_add(cached_values.estimated_backing_bytes());
        charge_order_candidate_work(comparisons, retained_backing_bytes)?;

        match &mut self.candidates {
            DataRowOrderCandidates::Bounded(window) => {
                window.push((candidate, cached_values), |left, right| {
                    compare_cached_orderable_rows(&left.1, &right.1, self.resolved_order)
                });
            }
            DataRowOrderCandidates::Complete {
                rows,
                retained_backing_bytes: total,
            } => {
                rows.push((candidate, cached_values));
                *total = total.saturating_add(retained_backing_bytes);
            }
        }

        Ok(())
    }

    /// Return the current blocking-state row count.
    #[must_use]
    pub(in crate::db::executor) const fn retained_count(&self) -> usize {
        match &self.candidates {
            DataRowOrderCandidates::Bounded(window) => window.rows.len(),
            DataRowOrderCandidates::Complete { rows, .. } => rows.len(),
        }
    }

    /// Consume the selected candidates in final canonical order.
    pub(in crate::db::executor) fn into_sorted_rows(self) -> Result<Vec<DataRow>, InternalError> {
        let mut rows = match self.candidates {
            DataRowOrderCandidates::Bounded(window) => window.into_rows(),
            DataRowOrderCandidates::Complete { rows, .. } => rows,
        };
        let rows_sorted = rows.len();
        if rows_sorted > 1 {
            charge_sort_work::<DataRow>(rows_sorted)?;
            rows.sort_by(|left, right| {
                compare_cached_orderable_rows(&left.1, &right.1, self.resolved_order)
            });
        }

        Ok(rows.into_iter().map(|(row, _)| row).collect())
    }
}

enum DataRowOrderCandidates {
    Bounded(BoundedOrderRows<(DataRow, CachedOrderValues)>),
    Complete {
        rows: Vec<(DataRow, CachedOrderValues)>,
        retained_backing_bytes: u64,
    },
}

impl<'a, R> BoundedOrderWindow<'a, R>
where
    R: OrderReadableRow,
{
    /// Build one bounded accumulator for the planner-resolved order contract.
    #[must_use]
    pub(in crate::db::executor) fn new(
        keep_count: usize,
        resolved_order: &'a ResolvedOrder,
    ) -> Self {
        let candidates = if resolved_order_uses_only_direct_fields(resolved_order) {
            BoundedOrderCandidates::Direct(BoundedOrderRows::new(keep_count))
        } else {
            BoundedOrderCandidates::Cached(BoundedOrderRows::new(keep_count))
        };

        Self {
            resolved_order,
            candidates,
        }
    }

    /// Retain one candidate if it belongs in the bounded resolved-order window.
    pub(in crate::db::executor) fn push(&mut self, candidate: R) -> Result<(), InternalError> {
        let retained_count = match &self.candidates {
            BoundedOrderCandidates::Direct(window) => window.rows.len(),
            BoundedOrderCandidates::Cached(window) => window.rows.len(),
        };
        let comparisons = if retained_count == 0 {
            0
        } else if retained_count < self.candidates.keep_count() {
            1
        } else {
            retained_count.saturating_add(1)
        };
        match &mut self.candidates {
            BoundedOrderCandidates::Direct(window) => {
                charge_order_candidate_work(comparisons, candidate.retained_order_backing_bytes())?;
                window.push(candidate, |left, right| {
                    compare_borrowed_direct_orderable_rows(left, right, self.resolved_order)
                });
            }
            BoundedOrderCandidates::Cached(window) => {
                let cached_values = cache_order_values_from_row(&candidate, self.resolved_order);
                let retained_backing_bytes = candidate
                    .retained_order_backing_bytes()
                    .saturating_add(cached_values.estimated_backing_bytes());
                charge_order_candidate_work(comparisons, retained_backing_bytes)?;
                window.push((candidate, cached_values), |left, right| {
                    compare_cached_orderable_rows(&left.1, &right.1, self.resolved_order)
                });
            }
        }

        Ok(())
    }

    /// Consume retained rows while preserving expression-order values for
    /// canonical post-access ordering.
    #[must_use]
    pub(in crate::db::executor) fn into_pending_rows(self) -> PendingOrderRows<R> {
        match self.candidates {
            BoundedOrderCandidates::Direct(window) => PendingOrderRows::plain(window.into_rows()),
            BoundedOrderCandidates::Cached(window) => PendingOrderRows {
                storage: PendingOrderRowStorage::Cached {
                    resolved_order: self.resolved_order.clone(),
                    keep_count: window.keep_count,
                    rows: window.into_rows(),
                },
            },
        }
    }
}

///
/// BoundedOrderCandidates
///
/// Strategy-owned candidates selected once from the captured resolved order.
///

enum BoundedOrderCandidates<R> {
    Direct(BoundedOrderRows<R>),
    Cached(BoundedOrderRows<(R, CachedOrderValues)>),
}

impl<R> BoundedOrderCandidates<R> {
    const fn keep_count(&self) -> usize {
        match self {
            Self::Direct(window) => window.keep_count,
            Self::Cached(window) => window.keep_count,
        }
    }
}

/// Retain the best bounded set while keeping each caller's comparison strategy.
/// Budget charging happens before insertion; this buffer owns only selection,
/// and canonical post-access ordering still owns the final sort.
struct BoundedOrderRows<R> {
    rows: Vec<R>,
    worst_index: Option<usize>,
    keep_count: usize,
}

impl<R> BoundedOrderRows<R> {
    fn new(keep_count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(keep_count.min(BOUNDED_ORDER_INITIAL_CAPACITY)),
            worst_index: None,
            keep_count,
        }
    }

    fn push(&mut self, candidate: R, compare: impl Fn(&R, &R) -> Ordering) {
        if self.keep_count == 0 {
            return;
        }
        if self.rows.len() < self.keep_count {
            let appended_index = self.rows.len();
            self.rows.push(candidate);
            if self.worst_index.is_none_or(|worst_index| {
                compare(&self.rows[appended_index], &self.rows[worst_index]).is_gt()
            }) {
                self.worst_index = Some(appended_index);
            }
            return;
        }

        let worst_index = self
            .worst_index
            .unwrap_or_else(|| self.worst_row_index(&compare));
        // Equal candidates do not displace retained rows; worst-row scans also
        // retain the first maximum when several retained rows compare equally.
        if compare(&candidate, &self.rows[worst_index]).is_lt() {
            self.rows[worst_index] = candidate;
            self.worst_index = Some(self.worst_row_index(&compare));
        }
    }

    fn into_rows(self) -> Vec<R> {
        self.rows
    }

    fn worst_row_index(&self, compare: &impl Fn(&R, &R) -> Ordering) -> usize {
        debug_assert!(
            !self.rows.is_empty(),
            "bounded order window must have retained rows before resolving worst row",
        );
        let mut worst_index = 0;
        for index in 1..self.rows.len() {
            if compare(&self.rows[index], &self.rows[worst_index]).is_gt() {
                worst_index = index;
            }
        }
        worst_index
    }
}

fn charge_order_candidate_work(
    comparisons: usize,
    retained_backing_bytes: u64,
) -> Result<(), InternalError> {
    charge_current_execution_budget(DiagnosticExecutionBudgetResource::SortEntries, 1)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        u64::try_from(comparisons).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortTemporaryBytes,
        retained_backing_bytes,
    )
}

fn data_row_retained_backing_bytes(row: &DataRow) -> u64 {
    u64::try_from(std::mem::size_of::<DataRow>())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(row.1.len()).unwrap_or(u64::MAX))
}

/// Compare one structural row against one cursor boundary under the canonical order contract.
pub(in crate::db::executor) fn compare_orderable_row_with_boundary<R>(
    row: &R,
    resolved_order: &ResolvedOrder,
    boundary: &CursorBoundary,
) -> Result<Ordering, InternalError>
where
    R: OrderReadableRow,
{
    compare_structural_order_slots_fallible(resolved_order, |slot_index, source, direction| {
        let row_slot = order_value_from_row(row, source);
        let boundary_slot = boundary
            .slots
            .get(slot_index)
            .ok_or_else(InternalError::query_executor_invariant)?;

        Ok(apply_order_direction(
            compare_order_value_with_boundary(row_slot, boundary_slot),
            direction,
        ))
    })
}

fn compare_structural_order_slots_fallible(
    resolved_order: &ResolvedOrder,
    mut compare_slot: impl FnMut(
        usize,
        &ResolvedOrderValueSource,
        OrderDirection,
    ) -> Result<Ordering, InternalError>,
) -> Result<Ordering, InternalError> {
    for (slot_index, field) in resolved_order.fields().iter().enumerate() {
        let ordering = compare_slot(slot_index, field.source(), field.direction())?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }

    Ok(Ordering::Equal)
}

// Compare two cached structural ordering tuples according to the resolved
// canonical order without re-reading row slots inside the comparator.
fn compare_cached_orderable_rows(
    left: &CachedOrderValues,
    right: &CachedOrderValues,
    resolved_order: &ResolvedOrder,
) -> Ordering {
    match (left, right) {
        (
            CachedOrderValues::Inline {
                len: left_len,
                values: left_values,
            },
            CachedOrderValues::Inline {
                len: right_len,
                values: right_values,
            },
        ) => compare_cached_order_value_lists(
            &left_values[..*left_len],
            &right_values[..*right_len],
            resolved_order,
        ),
        (CachedOrderValues::Heap(left_values), CachedOrderValues::Heap(right_values)) => {
            compare_cached_order_value_lists(left_values, right_values, resolved_order)
        }
        (
            CachedOrderValues::Inline {
                len: left_len,
                values: left_values,
            },
            CachedOrderValues::Heap(right_values),
        ) => compare_cached_order_value_lists(
            &left_values[..*left_len],
            right_values,
            resolved_order,
        ),
        (
            CachedOrderValues::Heap(left_values),
            CachedOrderValues::Inline {
                len: right_len,
                values: right_values,
            },
        ) => compare_cached_order_value_lists(
            left_values,
            &right_values[..*right_len],
            resolved_order,
        ),
    }
}

// Return whether one row set can use the borrowed direct-slot comparator path.
fn can_use_borrowed_direct_order_path<R>(rows: &[R], resolved_order: &ResolvedOrder) -> bool
where
    R: OrderReadableRow,
{
    resolved_order_uses_only_direct_fields(resolved_order)
        && rows
            .first()
            .is_some_and(OrderReadableRow::order_slots_are_borrowed)
}

fn resolved_order_uses_only_direct_fields(resolved_order: &ResolvedOrder) -> bool {
    resolved_order
        .fields()
        .iter()
        .all(|field| matches!(field.source(), ResolvedOrderValueSource::DirectField(_)))
}

// Apply direct-slot ordering by borrowing row values during comparisons instead
// of building owned cached order tuples.
fn apply_borrowed_direct_order_window<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) where
    R: OrderReadableRow,
{
    if let Some(keep_count) = keep_count
        && rows.len() > keep_count
    {
        rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_borrowed_direct_orderable_rows(left, right, resolved_order)
        });
        rows.truncate(keep_count);
    }

    rows.sort_by(|left, right| compare_borrowed_direct_orderable_rows(left, right, resolved_order));
}

// Compare direct field-slot order rows through borrowed slot values only.
fn compare_borrowed_direct_orderable_rows<R>(
    left: &R,
    right: &R,
    resolved_order: &ResolvedOrder,
) -> Ordering
where
    R: OrderReadableRow,
{
    for field in resolved_order.fields() {
        let ResolvedOrderValueSource::DirectField(slot) = field.source() else {
            return Ordering::Equal;
        };

        let ordering = apply_order_direction(
            compare_cached_order_values(
                left.read_order_slot_ref(*slot),
                right.read_order_slot_ref(*slot),
            ),
            field.direction(),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Cache one row's order values once so sort/select hot loops can compare
// cheap owned key tuples instead of re-deriving them repeatedly.
fn cache_order_values_from_row<R>(row: &R, resolved_order: &ResolvedOrder) -> CachedOrderValues
where
    R: OrderReadableRow,
{
    let fields = resolved_order.fields();
    let mut cached_values = CachedOrderValues::with_capacity(fields.len());

    for field in fields {
        cached_values.push(order_value_from_row(row, field.source()).map(Cow::into_owned));
    }

    cached_values
}

// Cache one raw row's order values once so materialized raw-row sort/select
// can avoid building retained-slot kernel rows only to feed the order cache.
fn cache_order_values_from_data_row(
    row: &DataRow,
    row_layout: &RowLayout,
    resolved_order: &ResolvedOrder,
) -> Result<CachedOrderValues, InternalError> {
    // Phase 1: pure direct-field ORDER BY terms can stay on the sparse
    // contract path and decode only the ordered slots in field order.
    if let Some(required_slots) = resolved_order.direct_field_slots() {
        let values = row_layout.decode_indexed_values_from_data_key(
            &row.1,
            &row.0,
            required_slots.as_slice(),
        )?;
        let mut cached_values = CachedOrderValues::with_capacity(values.len());

        for value in values {
            cached_values.push(value);
        }

        return Ok(cached_values);
    }

    // Phase 2: expression-backed ordering still needs the general structural
    // slot reader so expression evaluation can borrow slots repeatedly.
    let slots = row_layout.open_raw_row_with_contract(&row.1)?;
    let mut cached_values = CachedOrderValues::with_capacity(resolved_order.fields().len());

    for field in resolved_order.fields() {
        let value = match field.source() {
            ResolvedOrderValueSource::DirectField(slot) => {
                Some(slots.required_value_by_contract(*slot)?)
            }
            ResolvedOrderValueSource::Expression(expr) => {
                eval_compiled_expr_with_value_reader(expr, &mut |slot| {
                    slots.required_value_by_contract(slot).ok()
                })
                .ok()
            }
        };

        cached_values.push(value);
    }

    Ok(cached_values)
}

/// Build one canonical continuation boundary from a decoded structural row.
pub(in crate::db::executor) fn cursor_boundary_from_orderable_row<R>(
    row: &R,
    resolved_order: &ResolvedOrder,
) -> CursorBoundary
where
    R: OrderReadableRow,
{
    let slots = resolved_order
        .fields()
        .iter()
        .map(|field| match order_value_from_row(row, field.source()) {
            Some(value) => CursorBoundarySlot::Present(value.into_owned()),
            None => CursorBoundarySlot::Missing,
        })
        .collect();

    CursorBoundary { slots }
}

/// Build one canonical continuation boundary from a persisted data row.
pub(in crate::db::executor) fn cursor_boundary_from_data_row(
    row: &DataRow,
    row_layout: &RowLayout,
    resolved_order: &ResolvedOrder,
) -> Result<CursorBoundary, InternalError> {
    let values = cache_order_values_from_data_row(row, row_layout, resolved_order)?;
    let slots = values
        .into_values()
        .map(|value| match value {
            Some(value) => CursorBoundarySlot::Present(value),
            None => CursorBoundarySlot::Missing,
        })
        .collect();

    Ok(CursorBoundary { slots })
}

// Compare two already-materialized ordering tuples by walking their cached
// value lists directly instead of re-entering indexed slot lookups.
fn compare_cached_order_value_lists(
    left: &[Option<Value>],
    right: &[Option<Value>],
    resolved_order: &ResolvedOrder,
) -> Ordering {
    debug_assert_eq!(
        left.len(),
        resolved_order.fields().len(),
        "cached left order values must align with resolved order fields",
    );
    debug_assert_eq!(
        right.len(),
        resolved_order.fields().len(),
        "cached right order values must align with resolved order fields",
    );

    for ((left_slot, right_slot), field) in left
        .iter()
        .zip(right.iter())
        .zip(resolved_order.fields().iter())
    {
        let ordering = apply_order_direction(
            compare_cached_order_values(left_slot.as_ref(), right_slot.as_ref()),
            field.direction(),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Borrow one slot-reader value through the shared ordering seam.
fn order_value_from_row<'a, R>(
    row: &'a R,
    source: &'a ResolvedOrderValueSource,
) -> Option<Cow<'a, Value>>
where
    R: OrderReadableRow + ?Sized,
{
    let value = match source {
        ResolvedOrderValueSource::DirectField(slot) => row.read_order_slot_cow(*slot),
        ResolvedOrderValueSource::Expression(expr) => {
            eval_compiled_expr_with_value_reader(expr, &mut |slot| row.read_order_slot(slot))
                .ok()
                .map(Cow::Owned)
        }
    };

    value.filter(|value| !matches!(value.as_ref(), Value::Null))
}

// Compare two cached owned ordering values after key precomputation.
fn compare_cached_order_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => canonical_value_compare(left, right),
    }
}

// Compare one row-provided ordering value against one persisted cursor
// boundary slot without rebuilding the row side into an owned boundary slot.
fn compare_order_value_with_boundary(
    value: Option<Cow<'_, Value>>,
    boundary: &CursorBoundarySlot,
) -> Ordering {
    match (value, boundary) {
        (None, CursorBoundarySlot::Missing) => Ordering::Equal,
        (None, CursorBoundarySlot::Present(_)) => Ordering::Less,
        (Some(_), CursorBoundarySlot::Missing) => Ordering::Greater,
        (Some(value), CursorBoundarySlot::Present(boundary_value)) => {
            canonical_value_compare(value.as_ref(), boundary_value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedOrderRows, reorder_rows_by_original_indices};

    #[test]
    fn bounded_selection_matches_sorted_prefixes_in_both_directions() {
        let input = [4, 1, 7, 7, 2, 9, 0, 5];
        for descending in [false, true] {
            let compare = |left: &i32, right: &i32| {
                if descending {
                    right.cmp(left)
                } else {
                    left.cmp(right)
                }
            };
            for keep_count in 0..=input.len() + 1 {
                let mut window = BoundedOrderRows::new(keep_count);
                for (index, value) in input.into_iter().enumerate() {
                    window.push(value, compare);
                    let mut expected = input[..=index].to_vec();
                    expected.sort_by(compare);
                    expected.truncate(keep_count);
                    let mut actual = window.rows.clone();
                    actual.sort_by(compare);
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    #[test]
    fn equal_candidates_preserve_retained_row_identity() {
        let mut window = BoundedOrderRows::new(2);
        for row in [(1, 'a'), (1, 'b'), (1, 'c')] {
            window.push(row, |left, right| left.0.cmp(&right.0));
        }
        assert_eq!(window.into_rows(), vec![(1, 'a'), (1, 'b')]);
    }

    #[cfg(feature = "sql")]
    #[test]
    fn bounded_strategies_preserve_budget_charges_and_cache_each_candidate_once() {
        use super::*;
        use crate::db::{
            executor::budget::{
                HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
                current_execution_budget_usage, with_execution_budget_for_tests,
            },
            query::plan::{ResolvedOrderField, expr::CompiledExpr},
        };
        use icydb_diagnostic_code::{DiagnosticExecutionBudgetScope, DiagnosticExecutionLane};
        use std::{cell::Cell, rc::Rc};

        struct Row(Value, Rc<Cell<usize>>);
        impl OrderReadableRow for Row {
            fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
                (slot == 0).then_some(&self.0)
            }
            fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
                self.1.set(self.1.get() + 1);
                self.read_order_slot_ref(slot).map(Cow::Borrowed)
            }
            fn order_slots_are_borrowed(&self) -> bool {
                true
            }
        }

        for cached in [false, true] {
            let source = if cached {
                ResolvedOrderValueSource::expression(CompiledExpr::Slot {
                    slot: 0,
                    field: "value".to_string(),
                })
            } else {
                ResolvedOrderValueSource::direct_field(0)
            };
            let order =
                ResolvedOrder::new(vec![ResolvedOrderField::new(source, OrderDirection::Asc)]);
            let reads = Rc::new(Cell::new(0));
            let budget = HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            );
            let context = HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::TrustedRead,
                0,
            );
            with_execution_budget_for_tests(
                budget,
                context,
                || {
                    let mut window = BoundedOrderWindow::new(2, &order);
                    let mut expected_bytes = 0;
                    for value in [4, 1, 5, 0] {
                        let row = Row(Value::Int64(value), Rc::clone(&reads));
                        expected_bytes += row.retained_order_backing_bytes();
                        if cached {
                            expected_bytes += runtime_value_work(&row.0).0;
                        }
                        window.push(row)?;
                    }
                    let usage = current_execution_budget_usage()?;
                    assert_eq!(
                        usage.observed(DiagnosticExecutionBudgetResource::SortEntries),
                        4
                    );
                    assert_eq!(
                        usage.observed(DiagnosticExecutionBudgetResource::SortComparisons),
                        7
                    );
                    assert_eq!(
                        usage.observed(DiagnosticExecutionBudgetResource::SortTemporaryBytes),
                        expected_bytes
                    );
                    let rows = window.into_pending_rows().apply_order(&order, Some(2))?;
                    assert_eq!(
                        rows.into_iter().map(|row| row.0).collect::<Vec<_>>(),
                        vec![Value::Int64(0), Value::Int64(1)]
                    );
                    assert_eq!(reads.get(), if cached { 4 } else { 0 });
                    Ok::<_, InternalError>(())
                },
                std::convert::identity,
            )
            .unwrap();
        }
    }

    #[test]
    fn compact_order_indices_reorder_complete_and_bounded_rows() {
        assert_eq!(
            reorder_rows_by_original_indices(vec!['a', 'b', 'c', 'd'], &[2, 0, 3, 1])
                .expect("complete permutation should reorder"),
            vec!['c', 'a', 'd', 'b'],
        );
        assert_eq!(
            reorder_rows_by_original_indices(vec!['a', 'b', 'c', 'd'], &[3, 1])
                .expect("bounded permutation should retain selected rows"),
            vec!['d', 'b'],
        );
        assert!(reorder_rows_by_original_indices(vec!['a'], &[1]).is_err());
    }
}
