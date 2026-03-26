//! Module: executor::order
//! Responsibility: shared structural ordering helpers for executor row paths.
//! Does not own: planner order semantics or cursor wire validation.
//! Boundary: resolves order slots once and applies canonical ordering over slot-readable rows.

use crate::{
    db::{
        cursor::{
            CursorBoundary, CursorBoundarySlot, apply_order_direction, compare_boundary_slots,
        },
        query::plan::{OrderDirection, OrderSpec},
    },
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};
use std::cmp::Ordering;

///
/// OrderReadableRow
///
/// Structural executor row contract used by shared ordering logic.
/// Implementors expose slot-indexed values without re-entering typed entity
/// comparators in sort and cursor-boundary hot loops.
///

pub(in crate::db::executor) trait OrderReadableRow {
    /// Read one slot value for structural ordering and predicate evaluation.
    fn read_order_slot(&self, slot: usize) -> Option<Value>;
}

///
/// ResolvedOrderField
///
/// One order slot resolved from field name to model slot index.
/// Shared structural ordering keeps this resolved shape outside comparator
/// loops so sorting does not repeat field-name lookups.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedOrderField {
    field_index: Option<usize>,
    direction: OrderDirection,
}

///
/// ResolvedOrder
///
/// Slot-resolved canonical ordering shape shared by executor row paths.
/// This keeps sorting and cursor-boundary comparisons structural once the
/// planner has already fixed the visible order contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ResolvedOrder {
    fields: Vec<ResolvedOrderField>,
}

impl ResolvedOrder {
    fn iter(&self) -> impl Iterator<Item = ResolvedOrderField> + '_ {
        self.fields.iter().copied()
    }
}

/// Resolve one order spec into slot indexes for structural executor use.
#[must_use]
pub(in crate::db::executor) fn resolve_structural_order(
    model: &EntityModel,
    order: &OrderSpec,
) -> ResolvedOrder {
    let fields = order
        .fields
        .iter()
        .map(|(field, direction)| ResolvedOrderField {
            field_index: resolve_field_slot(model, field),
            direction: *direction,
        })
        .collect();

    ResolvedOrder { fields }
}

/// Apply canonical in-memory ordering with an optional bounded top-k window.
pub(in crate::db::executor) fn apply_structural_order_window<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) where
    R: OrderReadableRow,
{
    if let Some(keep_count) = keep_count {
        if keep_count == 0 {
            rows.clear();
            return;
        }

        if rows.len() > keep_count {
            rows.select_nth_unstable_by(keep_count - 1, |left, right| {
                compare_orderable_rows(left, right, resolved_order)
            });
            rows.truncate(keep_count);
        }
    }

    sort_structural_rows(rows.as_mut_slice(), resolved_order);
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
        let row_slot = boundary_slot_from_row(row, field_index);
        let boundary_slot = boundary
            .slots
            .get(slot_index)
            .expect("cursor boundary must align with resolved order");

        apply_order_direction(compare_boundary_slots(&row_slot, boundary_slot), direction)
    })
}

// Compare two structural rows according to the resolved canonical order.
fn compare_orderable_rows(
    left: &dyn OrderReadableRow,
    right: &dyn OrderReadableRow,
    resolved_order: &ResolvedOrder,
) -> Ordering {
    compare_structural_order_slots(resolved_order, |_slot_index, field_index, direction| {
        let left_slot = boundary_slot_from_row(left, field_index);
        let right_slot = boundary_slot_from_row(right, field_index);

        apply_order_direction(compare_boundary_slots(&left_slot, &right_slot), direction)
    })
}

// Apply the canonical shared in-memory sort contract over one structural row slice.
fn sort_structural_rows<R>(rows: &mut [R], resolved_order: &ResolvedOrder)
where
    R: OrderReadableRow,
{
    rows.sort_by(|left, right| compare_orderable_rows(left, right, resolved_order));
}

// Compare one structural ordering tuple by resolving slot pairs lazily in canonical field order.
fn compare_structural_order_slots<F>(
    resolved_order: &ResolvedOrder,
    mut compare_slot: F,
) -> Ordering
where
    F: FnMut(usize, Option<usize>, OrderDirection) -> Ordering,
{
    for (slot_index, slot) in resolved_order.iter().enumerate() {
        let ordering = compare_slot(slot_index, slot.field_index, slot.direction);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Convert one slot-reader value into the explicit cursor ordering slot contract.
fn boundary_slot_from_row(
    row: &dyn OrderReadableRow,
    field_index: Option<usize>,
) -> CursorBoundarySlot {
    let value = field_index.and_then(|slot| row.read_order_slot(slot));

    match value {
        Some(value) => CursorBoundarySlot::Present(value),
        None => CursorBoundarySlot::Missing,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRow {
        slots: Vec<Option<Value>>,
    }

    impl TestRow {
        fn new(slots: Vec<Option<Value>>) -> Self {
            Self { slots }
        }
    }

    impl OrderReadableRow for TestRow {
        fn read_order_slot(&self, slot: usize) -> Option<Value> {
            self.slots.get(slot).cloned().flatten()
        }
    }

    fn resolved_order(fields: &[(Option<usize>, OrderDirection)]) -> ResolvedOrder {
        ResolvedOrder {
            fields: fields
                .iter()
                .map(|(field_index, direction)| ResolvedOrderField {
                    field_index: *field_index,
                    direction: *direction,
                })
                .collect(),
        }
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
            &resolved_order(&[(Some(0), OrderDirection::Asc)]),
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
            &resolved_order(&[(Some(0), OrderDirection::Asc)]),
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
            &resolved_order(&[(Some(0), OrderDirection::Desc)]),
            &boundary,
        );

        assert_eq!(ordering, Ordering::Less);
    }
}
