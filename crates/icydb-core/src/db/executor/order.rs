//! Module: executor::order
//! Responsibility: shared structural ordering helpers for executor row paths.
//! Does not own: planner order semantics or cursor wire validation.
//! Boundary: consumes planner-resolved order contracts and applies canonical ordering over slot-readable rows.

use crate::{
    db::{
        cursor::{CursorBoundary, CursorBoundarySlot, apply_order_direction},
        data::{CanonicalSlotReader, DataRow},
        executor::{
            measure_execution_stats_phase, projection::eval_compiled_expr_with_value_reader,
            record_ordering, terminal::RowLayout,
        },
        numeric::canonical_value_compare,
        query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderValueSource},
    },
    error::InternalError,
    value::Value,
};
use std::{array, borrow::Cow, cmp::Ordering, mem};

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

    fn into_boundary_slots(self) -> Vec<CursorBoundarySlot> {
        match self {
            Self::Inline { len, values } => {
                let mut slots = Vec::with_capacity(len);
                for value in values.into_iter().take(len) {
                    slots.push(match value {
                        Some(value) => CursorBoundarySlot::Present(value),
                        None => CursorBoundarySlot::Missing,
                    });
                }
                slots
            }
            Self::Heap(values) => {
                let mut slots = Vec::with_capacity(values.len());
                for value in values {
                    slots.push(match value {
                        Some(value) => CursorBoundarySlot::Present(value),
                        None => CursorBoundarySlot::Missing,
                    });
                }
                slots
            }
        }
    }
}

/// Apply canonical in-memory ordering with an optional bounded top-k window.
pub(in crate::db::executor) fn apply_structural_order_window<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) where
    R: OrderReadableRow,
{
    if let Some(keep_count) = keep_count
        && keep_count == 0
    {
        rows.clear();
        return;
    }

    if rows.len() <= 1 {
        return;
    }
    let rows_sorted = rows.len();
    let ((), ordering_micros) = measure_execution_stats_phase(|| {
        apply_structural_order_window_inner(rows, resolved_order, keep_count);
    });
    record_ordering(rows_sorted, ordering_micros);
}

fn apply_structural_order_window_inner<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) where
    R: OrderReadableRow,
{
    // Phase 1: pure direct-slot orders over retained executor rows can compare
    // borrowed values directly. This avoids materializing owned order keys for
    // the common `ORDER BY field[, id]` path while preserving the existing
    // cached fallback for expression orders and rows that synthesize values.
    if can_use_borrowed_direct_order_path(rows.as_slice(), resolved_order) {
        apply_borrowed_direct_order_window(rows, resolved_order, keep_count);
        return;
    }

    // Phase 2: cache resolved order values once per row so bounded selection
    // and final sort do not re-read sparse slots or re-run expression-order
    // derivation inside comparator hot loops.
    let source_rows = std::mem::take(rows);
    let mut cached_rows = Vec::with_capacity(source_rows.len());
    for row in source_rows {
        let cached_values = cache_order_values_from_row(&row, resolved_order);

        cached_rows.push((row, cached_values));
    }

    // Phase 3: retain only the bounded canonical window when pagination
    // exposes one, using the cached order keys instead of live row reads.
    if let Some(keep_count) = keep_count
        && cached_rows.len() > keep_count
    {
        cached_rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_cached_orderable_rows(&left.1, &right.1, resolved_order)
        });
        cached_rows.truncate(keep_count);
    }

    // Phase 4: sort the retained rows into final canonical order using the
    // precomputed key values.
    cached_rows
        .sort_by(|left, right| compare_cached_orderable_rows(&left.1, &right.1, resolved_order));
    rows.extend(cached_rows.into_iter().map(|(row, _)| row));
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
                apply_structural_order_window(&mut rows, resolved_order, keep_count);
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
                    let ((), ordering_micros) = measure_execution_stats_phase(|| {
                        rows.sort_by(|left, right| {
                            compare_cached_orderable_rows(&left.1, &right.1, resolved_order)
                        });
                    });
                    record_ordering(rows_sorted, ordering_micros);
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
            BoundedOrderCandidates::Direct(BoundedDirectOrderWindow::new(keep_count))
        } else {
            BoundedOrderCandidates::Cached(BoundedCachedOrderWindow::new(keep_count))
        };

        Self {
            resolved_order,
            candidates,
        }
    }

    /// Retain one candidate if it belongs in the bounded resolved-order window.
    pub(in crate::db::executor) fn push(&mut self, candidate: R) {
        match &mut self.candidates {
            BoundedOrderCandidates::Direct(window) => window.push(candidate, self.resolved_order),
            BoundedOrderCandidates::Cached(window) => window.push(candidate, self.resolved_order),
        }
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
                    rows: window.into_rows_with_cached_values(),
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
    Direct(BoundedDirectOrderWindow<R>),
    Cached(BoundedCachedOrderWindow<R>),
}

///
/// BoundedDirectOrderWindow
///
/// BoundedDirectOrderWindow retains the best `keep_count` rows under one
/// direct-slot order while a scan is still running.
/// It deliberately does not final-sort rows; the canonical post-access
/// order/window phase remains the final ordering authority.
///

struct BoundedDirectOrderWindow<R> {
    rows: Vec<R>,
    worst_index: Option<usize>,
    keep_count: usize,
}

impl<R> BoundedDirectOrderWindow<R>
where
    R: OrderReadableRow,
{
    /// Build one bounded direct-order accumulator.
    #[must_use]
    fn new(keep_count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(keep_count.min(BOUNDED_ORDER_INITIAL_CAPACITY)),
            worst_index: None,
            keep_count,
        }
    }

    /// Retain one candidate if it belongs in the bounded order window.
    fn push(&mut self, candidate: R, resolved_order: &ResolvedOrder) {
        if self.keep_count == 0 {
            return;
        }
        if self.rows.len() < self.keep_count {
            self.rows.push(candidate);
            self.update_worst_after_append(resolved_order);
            return;
        }

        let worst_index = self
            .worst_index
            .unwrap_or_else(|| worst_direct_order_row_index(self.rows.as_slice(), resolved_order));
        if compare_borrowed_direct_orderable_rows(
            &candidate,
            &self.rows[worst_index],
            resolved_order,
        )
        .is_lt()
        {
            self.rows[worst_index] = candidate;
            self.worst_index = Some(worst_direct_order_row_index(
                self.rows.as_slice(),
                resolved_order,
            ));
        }
    }

    /// Consume the retained, not-yet-final-sorted rows.
    #[must_use]
    fn into_rows(self) -> Vec<R> {
        self.rows
    }

    fn update_worst_after_append(&mut self, resolved_order: &ResolvedOrder) {
        let appended_index = self.rows.len().saturating_sub(1);
        let Some(worst_index) = self.worst_index else {
            self.worst_index = Some(appended_index);
            return;
        };
        if compare_borrowed_direct_orderable_rows(
            &self.rows[appended_index],
            &self.rows[worst_index],
            resolved_order,
        )
        .is_gt()
        {
            self.worst_index = Some(appended_index);
        }
    }
}

///
/// BoundedCachedOrderWindow
///
/// Expression-backed candidates paired with their complete resolved order
/// tuples so comparisons never re-evaluate an expression for an already-seen
/// row.
///

struct BoundedCachedOrderWindow<R> {
    rows: Vec<(R, CachedOrderValues)>,
    worst_index: Option<usize>,
    keep_count: usize,
}

impl<R> BoundedCachedOrderWindow<R>
where
    R: OrderReadableRow,
{
    fn new(keep_count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(keep_count.min(BOUNDED_ORDER_INITIAL_CAPACITY)),
            worst_index: None,
            keep_count,
        }
    }

    fn push(&mut self, candidate: R, resolved_order: &ResolvedOrder) {
        if self.keep_count == 0 {
            return;
        }

        let cached_values = cache_order_values_from_row(&candidate, resolved_order);
        if self.rows.len() < self.keep_count {
            self.rows.push((candidate, cached_values));
            self.update_worst_after_append(resolved_order);
            return;
        }

        let worst_index = self
            .worst_index
            .unwrap_or_else(|| worst_cached_order_row_index(self.rows.as_slice(), resolved_order));
        if compare_cached_orderable_rows(&cached_values, &self.rows[worst_index].1, resolved_order)
            .is_lt()
        {
            self.rows[worst_index] = (candidate, cached_values);
            self.worst_index = Some(worst_cached_order_row_index(
                self.rows.as_slice(),
                resolved_order,
            ));
        }
    }

    fn into_rows_with_cached_values(self) -> Vec<(R, CachedOrderValues)> {
        self.rows
    }

    fn update_worst_after_append(&mut self, resolved_order: &ResolvedOrder) {
        let appended_index = self.rows.len().saturating_sub(1);
        let Some(worst_index) = self.worst_index else {
            self.worst_index = Some(appended_index);
            return;
        };
        if compare_cached_orderable_rows(
            &self.rows[appended_index].1,
            &self.rows[worst_index].1,
            resolved_order,
        )
        .is_gt()
        {
            self.worst_index = Some(appended_index);
        }
    }
}

/// Apply canonical in-memory ordering with an optional bounded top-k window
/// directly over canonical `DataRow` payloads.
pub(in crate::db::executor) fn apply_structural_order_window_to_data_rows(
    rows: &mut Vec<DataRow>,
    row_layout: RowLayout,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) -> Result<(), InternalError> {
    if let Some(keep_count) = keep_count
        && keep_count == 0
    {
        rows.clear();
        return Ok(());
    }

    if rows.len() <= 1 {
        return Ok(());
    }
    let rows_sorted = rows.len();
    let (result, ordering_micros) = measure_execution_stats_phase(|| {
        apply_structural_order_window_to_data_rows_inner(
            rows,
            row_layout,
            resolved_order,
            keep_count,
        )
    });
    result?;
    record_ordering(rows_sorted, ordering_micros);

    Ok(())
}

fn apply_structural_order_window_to_data_rows_inner(
    rows: &mut Vec<DataRow>,
    row_layout: RowLayout,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) -> Result<(), InternalError> {
    // Phase 1: cache resolved order values once per raw row so the direct
    // `DataRow` lane can reuse the same bounded selection and final sort
    // logic without forcing retained-slot kernel rows first.
    let source_rows = mem::take(rows);
    let mut cached_rows = Vec::with_capacity(source_rows.len());
    for row in source_rows {
        let cached_values = cache_order_values_from_data_row(&row, &row_layout, resolved_order)?;

        cached_rows.push((row, cached_values));
    }

    // Phase 2: retain only the bounded canonical window when pagination
    // exposes one, using the cached order keys instead of live row reads.
    if let Some(keep_count) = keep_count
        && cached_rows.len() > keep_count
    {
        cached_rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_cached_orderable_rows(&left.1, &right.1, resolved_order)
        });
        cached_rows.truncate(keep_count);
    }

    // Phase 3: sort the retained rows into final canonical order using the
    // precomputed key values.
    cached_rows
        .sort_by(|left, right| compare_cached_orderable_rows(&left.1, &right.1, resolved_order));
    rows.extend(cached_rows.into_iter().map(|(row, _)| row));

    Ok(())
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

/// Materialize one cursor boundary directly from one already-decoded row under
/// the planner-frozen resolved order contract.
#[must_use]
pub(in crate::db::executor) fn cursor_boundary_from_orderable_row<R>(
    row: &R,
    resolved_order: &ResolvedOrder,
) -> CursorBoundary
where
    R: OrderReadableRow,
{
    let cached_values = cache_order_values_from_row(row, resolved_order);
    CursorBoundary {
        slots: cached_values.into_boundary_slots(),
    }
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

// Find the currently worst retained row under canonical direct-slot ordering.
fn worst_direct_order_row_index<R>(rows: &[R], resolved_order: &ResolvedOrder) -> usize
where
    R: OrderReadableRow,
{
    debug_assert!(
        !rows.is_empty(),
        "bounded order window must have retained rows before resolving worst row",
    );
    let mut worst_index = 0usize;
    for index in 1..rows.len() {
        if compare_borrowed_direct_orderable_rows(&rows[index], &rows[worst_index], resolved_order)
            .is_gt()
        {
            worst_index = index;
        }
    }

    worst_index
}

// Find the currently worst retained cached tuple under the complete resolved
// order, including direction and the planner-appended primary-key tie-breaker.
fn worst_cached_order_row_index<R>(
    rows: &[(R, CachedOrderValues)],
    resolved_order: &ResolvedOrder,
) -> usize {
    debug_assert!(
        !rows.is_empty(),
        "bounded cached order window must have retained rows before resolving worst row",
    );
    let mut worst_index = 0usize;
    for index in 1..rows.len() {
        if compare_cached_orderable_rows(&rows[index].1, &rows[worst_index].1, resolved_order)
            .is_gt()
        {
            worst_index = index;
        }
    }

    worst_index
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
