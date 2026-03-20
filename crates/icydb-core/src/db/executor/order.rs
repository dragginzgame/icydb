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
/// ResolvedStructuralOrderField
///
/// One order slot resolved from field name to model slot index.
/// Shared structural ordering keeps this resolved shape outside comparator
/// loops so sorting does not repeat field-name lookups.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedStructuralOrderField {
    field_index: Option<usize>,
    direction: OrderDirection,
}

///
/// ResolvedStructuralOrder
///
/// Slot-resolved canonical ordering shape shared by executor row paths.
/// This keeps sorting and cursor-boundary comparisons structural once the
/// planner has already fixed the visible order contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ResolvedStructuralOrder {
    fields: Vec<ResolvedStructuralOrderField>,
}

impl ResolvedStructuralOrder {
    fn iter(&self) -> impl Iterator<Item = ResolvedStructuralOrderField> + '_ {
        self.fields.iter().copied()
    }
}

/// Resolve one order spec into slot indexes for structural executor use.
#[must_use]
pub(in crate::db::executor) fn resolve_structural_order(
    model: &EntityModel,
    order: &OrderSpec,
) -> ResolvedStructuralOrder {
    let fields = order
        .fields
        .iter()
        .map(|(field, direction)| ResolvedStructuralOrderField {
            field_index: resolve_field_slot(model, field),
            direction: *direction,
        })
        .collect();

    ResolvedStructuralOrder { fields }
}

/// Apply canonical in-memory ordering over structural rows.
pub(in crate::db::executor) fn apply_structural_order<R>(
    rows: &mut [R],
    resolved_order: &ResolvedStructuralOrder,
) where
    R: OrderReadableRow,
{
    rows.sort_by(|left, right| compare_orderable_rows(left, right, resolved_order));
}

/// Apply bounded canonical ordering for first-page and top-k paths.
pub(in crate::db::executor) fn apply_structural_order_bounded<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedStructuralOrder,
    keep_count: usize,
) where
    R: OrderReadableRow,
{
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

    rows.sort_by(|left, right| compare_orderable_rows(left, right, resolved_order));
}

/// Compare one structural row against one cursor boundary under the canonical order contract.
pub(in crate::db::executor) fn compare_orderable_row_with_boundary<R>(
    row: &R,
    resolved_order: &ResolvedStructuralOrder,
    boundary: &CursorBoundary,
) -> Ordering
where
    R: OrderReadableRow,
{
    for (slot, boundary_slot) in resolved_order.iter().zip(boundary.slots.iter()) {
        let row_slot = boundary_slot_from_row(row, slot.field_index);
        let ordering = apply_order_direction(
            compare_boundary_slots(&row_slot, boundary_slot),
            slot.direction,
        );

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Compare two structural rows according to the resolved canonical order.
fn compare_orderable_rows(
    left: &dyn OrderReadableRow,
    right: &dyn OrderReadableRow,
    resolved_order: &ResolvedStructuralOrder,
) -> Ordering {
    for slot in resolved_order.iter() {
        let left_slot = boundary_slot_from_row(left, slot.field_index);
        let right_slot = boundary_slot_from_row(right, slot.field_index);
        let ordering = apply_order_direction(
            compare_boundary_slots(&left_slot, &right_slot),
            slot.direction,
        );

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
