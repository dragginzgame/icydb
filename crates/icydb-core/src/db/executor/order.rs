//! Module: executor::order
//! Responsibility: shared structural ordering helpers for executor row paths.
//! Does not own: planner order semantics or cursor wire validation.
//! Boundary: consumes planner-resolved order contracts and applies canonical ordering over slot-readable rows.

use crate::{
    db::{
        cursor::{CursorBoundary, CursorBoundarySlot, apply_order_direction},
        data::{CanonicalSlotReader, DataRow},
        executor::{
            measure_execution_stats_phase,
            projection::eval_scalar_projection_expr_with_value_reader, record_ordering,
            terminal::RowLayout,
        },
        numeric::canonical_value_compare,
        query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderValueSource},
    },
    error::InternalError,
    value::Value,
};
use std::{array, borrow::Cow, cmp::Ordering, mem};

const INLINE_ORDER_VALUE_CAPACITY: usize = 2;

///
/// OrderReadableRow
///
/// Structural executor row contract used by shared ordering logic.
/// Implementors expose slot-indexed values without re-entering typed entity
/// comparators in sort and cursor-boundary hot loops.
///

pub(in crate::db::executor) trait OrderReadableRow {
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
        let cached_values = cache_order_values_from_data_row(&row, row_layout, resolved_order)?;

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
) -> Ordering
where
    R: OrderReadableRow,
{
    compare_structural_order_slots(resolved_order, |slot_index, field_index, direction| {
        let row_slot = order_value_from_row(row, field_index);
        let boundary_slot = boundary
            .slots
            .get(slot_index)
            .expect("cursor boundary must align with resolved order");

        apply_order_direction(
            compare_order_value_with_boundary(row_slot, boundary_slot),
            direction,
        )
    })
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
    resolved_order.direct_field_slots().is_some()
        && rows
            .first()
            .is_some_and(OrderReadableRow::order_slots_are_borrowed)
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
    compare_structural_order_slots(resolved_order, |_slot_index, field_index, direction| {
        let left_slot = order_value_from_row(left, field_index);
        let right_slot = order_value_from_row(right, field_index);

        apply_order_direction(
            compare_order_values(left_slot.as_ref(), right_slot.as_ref()),
            direction,
        )
    })
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
    row_layout: RowLayout,
    resolved_order: &ResolvedOrder,
) -> Result<CachedOrderValues, InternalError> {
    // Phase 1: pure direct-field ORDER BY terms can stay on the sparse
    // contract path and decode only the ordered slots in field order.
    if let Some(required_slots) = resolved_order.direct_field_slots() {
        let values = row_layout.decode_indexed_values(
            &row.1,
            row.0.storage_key(),
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
    let slots = row_layout.open_raw_row(&row.1)?;
    let mut cached_values = CachedOrderValues::with_capacity(resolved_order.fields().len());

    for field in resolved_order.fields() {
        let value = match field.source() {
            ResolvedOrderValueSource::DirectField(slot) => {
                Some(slots.required_value_by_contract(*slot)?)
            }
            ResolvedOrderValueSource::Expression(expr) => {
                eval_scalar_projection_expr_with_value_reader(expr, &mut |slot| {
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

// Compare one structural ordering tuple by resolving slot pairs lazily in canonical field order.
fn compare_structural_order_slots<F>(
    resolved_order: &ResolvedOrder,
    mut compare_slot: F,
) -> Ordering
where
    F: FnMut(usize, &ResolvedOrderValueSource, OrderDirection) -> Ordering,
{
    for (slot_index, field) in resolved_order.fields().iter().enumerate() {
        let ordering = compare_slot(slot_index, field.source(), field.direction());
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
    match source {
        ResolvedOrderValueSource::DirectField(slot) => row.read_order_slot_cow(*slot),
        ResolvedOrderValueSource::Expression(expr) => {
            eval_scalar_projection_expr_with_value_reader(expr, &mut |slot| {
                row.read_order_slot(slot)
            })
            .ok()
            .map(Cow::Owned)
        }
    }
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

// Compare borrowed ordering values with the same missing/present semantics used
// by cached owned order keys.
fn compare_order_values(left: Option<&Cow<'_, Value>>, right: Option<&Cow<'_, Value>>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => canonical_value_compare(left.as_ref(), right.as_ref()),
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
mod tests {
    use super::*;
    use crate::{
        db::data::{CanonicalRow, with_structural_read_metrics},
        db::query::plan::ResolvedOrderField,
        model::field::FieldKind,
        traits::EntitySchema,
        types::{Blob, Text, Ulid},
        value::Value,
    };
    use icydb_derive::{FieldProjection, PersistedRow};
    use serde::Deserialize;
    use std::{borrow::Cow, cell::Cell, rc::Rc};

    struct TestRow {
        slots: Vec<Option<Value>>,
    }

    impl TestRow {
        fn new(slots: Vec<Option<Value>>) -> Self {
            Self { slots }
        }
    }

    impl OrderReadableRow for TestRow {
        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.slots
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
        }
    }

    struct CountingRow {
        reads: Rc<Cell<usize>>,
        borrowed: bool,
        slots: Vec<Option<Value>>,
    }

    impl CountingRow {
        fn new(reads: Rc<Cell<usize>>, slots: Vec<Option<Value>>) -> Self {
            Self {
                reads,
                borrowed: false,
                slots,
            }
        }

        fn borrowed(reads: Rc<Cell<usize>>, slots: Vec<Option<Value>>) -> Self {
            Self {
                reads,
                borrowed: true,
                slots,
            }
        }
    }

    impl OrderReadableRow for CountingRow {
        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.reads.set(self.reads.get().saturating_add(1));
            self.slots
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
        }

        fn order_slots_are_borrowed(&self) -> bool {
            self.borrowed
        }
    }

    fn resolved_order(fields: &[(usize, OrderDirection)]) -> ResolvedOrder {
        ResolvedOrder::new(
            fields
                .iter()
                .map(|(field_index, direction)| {
                    ResolvedOrderField::new(
                        ResolvedOrderValueSource::direct_field(*field_index),
                        *direction,
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn apply_structural_order_sorts_rows_by_resolved_slots() {
        let mut rows = vec![
            TestRow::new(vec![Some(Value::Uint(3))]),
            TestRow::new(vec![Some(Value::Uint(1))]),
            TestRow::new(vec![Some(Value::Uint(2))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(0, OrderDirection::Asc)]),
            None,
        );

        let ordered = rows
            .into_iter()
            .map(|row| row.read_order_slot(0))
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                Some(Value::Uint(1)),
                Some(Value::Uint(2)),
                Some(Value::Uint(3))
            ]
        );
    }

    #[test]
    fn apply_structural_order_bounded_keeps_smallest_rows_in_canonical_order() {
        let mut rows = vec![
            TestRow::new(vec![Some(Value::Uint(4))]),
            TestRow::new(vec![Some(Value::Uint(2))]),
            TestRow::new(vec![Some(Value::Uint(3))]),
            TestRow::new(vec![Some(Value::Uint(1))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(0, OrderDirection::Asc)]),
            Some(2),
        );

        let ordered = rows
            .into_iter()
            .map(|row| row.read_order_slot(0))
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![Some(Value::Uint(1)), Some(Value::Uint(2))]);
    }

    #[test]
    fn compare_orderable_row_with_boundary_respects_desc_direction() {
        let row = TestRow::new(vec![Some(Value::Uint(7))]);
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(5))],
        };

        let ordering = compare_orderable_row_with_boundary(
            &row,
            &resolved_order(&[(0, OrderDirection::Desc)]),
            &boundary,
        );

        assert_eq!(ordering, Ordering::Less);
    }

    #[test]
    fn apply_structural_order_window_caches_slot_reads_once_per_row() {
        let left_reads = Rc::new(Cell::new(0));
        let middle_reads = Rc::new(Cell::new(0));
        let right_reads = Rc::new(Cell::new(0));
        let mut rows = vec![
            CountingRow::new(left_reads.clone(), vec![Some(Value::Uint(3))]),
            CountingRow::new(middle_reads.clone(), vec![Some(Value::Uint(1))]),
            CountingRow::new(right_reads.clone(), vec![Some(Value::Uint(2))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(0, OrderDirection::Asc)]),
            Some(2),
        );

        assert_eq!(left_reads.get(), 1);
        assert_eq!(middle_reads.get(), 1);
        assert_eq!(right_reads.get(), 1);
    }

    #[test]
    fn apply_structural_order_window_uses_borrowed_direct_slot_fast_path() {
        let left_reads = Rc::new(Cell::new(0));
        let middle_reads = Rc::new(Cell::new(0));
        let right_reads = Rc::new(Cell::new(0));
        let mut rows = vec![
            CountingRow::borrowed(left_reads.clone(), vec![Some(Value::Uint(3))]),
            CountingRow::borrowed(middle_reads.clone(), vec![Some(Value::Uint(1))]),
            CountingRow::borrowed(right_reads.clone(), vec![Some(Value::Uint(2))]),
        ];

        apply_structural_order_window(
            &mut rows,
            &resolved_order(&[(0, OrderDirection::Asc)]),
            Some(2),
        );

        let ordered = rows
            .iter()
            .map(|row| row.read_order_slot(0))
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![Some(Value::Uint(1)), Some(Value::Uint(2))]);
        assert!(
            left_reads.get() + middle_reads.get() + right_reads.get() > 3,
            "borrowed direct-slot fast path should compare row slots directly instead of using the one-read cache",
        );
    }

    crate::test_canister! {
        ident = OrderWindowCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = OrderWindowStore,
        canister = OrderWindowCanister,
    }

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
    struct OrderWindowEntity {
        id: Ulid,
        title: Text,
        tags: Vec<Text>,
        portrait: Blob,
    }

    crate::test_entity_schema! {
        ident = OrderWindowEntity,
        id = Ulid,
        id_field = id,
        entity_name = "OrderWindowEntity",
        entity_tag = crate::testing::PROBE_ENTITY_TAG,
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("title", FieldKind::Text { max_len: None }),
            ("tags", FieldKind::List(&FieldKind::Text { max_len: None })),
            ("portrait", FieldKind::Blob),
        ],
        indexes = [],
        store = OrderWindowStore,
        canister = OrderWindowCanister,
    }

    fn direct_data_row(entity: &OrderWindowEntity) -> DataRow {
        let key = crate::db::data::DataKey::try_new::<OrderWindowEntity>(entity.id)
            .expect("test key construction should succeed");
        let row = CanonicalRow::from_entity(entity)
            .expect("test row serialization should succeed")
            .into_raw_row();

        (key, row)
    }

    #[test]
    fn cursor_boundary_from_orderable_row_handles_heap_cached_values() {
        let row = TestRow::new(vec![
            Some(Value::Uint(1)),
            Some(Value::Uint(2)),
            Some(Value::Uint(3)),
            Some(Value::Uint(4)),
            Some(Value::Uint(5)),
        ]);
        let boundary = cursor_boundary_from_orderable_row(
            &row,
            &resolved_order(&[
                (0, OrderDirection::Asc),
                (1, OrderDirection::Asc),
                (2, OrderDirection::Asc),
                (3, OrderDirection::Asc),
                (4, OrderDirection::Asc),
            ]),
        );

        assert_eq!(
            boundary.slots,
            vec![
                CursorBoundarySlot::Present(Value::Uint(1)),
                CursorBoundarySlot::Present(Value::Uint(2)),
                CursorBoundarySlot::Present(Value::Uint(3)),
                CursorBoundarySlot::Present(Value::Uint(4)),
                CursorBoundarySlot::Present(Value::Uint(5)),
            ]
        );
    }

    #[test]
    fn direct_data_row_order_window_uses_sparse_direct_field_decode() {
        let alpha = OrderWindowEntity {
            id: Ulid::from_u128(1),
            title: "alpha".to_string(),
            tags: vec!["one".to_string(), "two".to_string()],
            portrait: Blob::from(vec![0x10, 0x20, 0x30]),
        };
        let beta = OrderWindowEntity {
            id: Ulid::from_u128(2),
            title: "beta".to_string(),
            tags: vec!["three".to_string()],
            portrait: Blob::from(vec![0x40, 0x50, 0x60]),
        };
        let mut rows = vec![direct_data_row(&beta), direct_data_row(&alpha)];

        let (_result, metrics) = with_structural_read_metrics(|| {
            apply_structural_order_window_to_data_rows(
                &mut rows,
                RowLayout::from_model(OrderWindowEntity::MODEL),
                &resolved_order(&[(1, OrderDirection::Asc)]),
                None,
            )
        });

        assert_eq!(rows[0].1.try_decode::<OrderWindowEntity>().unwrap(), alpha);
        assert_eq!(rows[1].1.try_decode::<OrderWindowEntity>().unwrap(), beta);
        assert_eq!(metrics.rows_opened, 2);
        assert_eq!(
            metrics.declared_slots_validated, 2,
            "pure direct-field ordering should validate only the ordered slot per row",
        );
        assert_eq!(
            metrics.validated_non_scalar_slots, 0,
            "direct-field ordering should not validate untouched non-scalar slots",
        );
        assert_eq!(
            metrics.materialized_non_scalar_slots, 0,
            "direct-field ordering should leave untouched non-scalar slots unmaterialized",
        );
        assert_eq!(metrics.rows_without_lazy_non_scalar_materializations, 2);
    }
}
