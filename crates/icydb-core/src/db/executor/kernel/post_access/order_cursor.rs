use crate::{
    db::{
        cursor::{
            CursorBoundary, CursorBoundarySlot, apply_order_direction, compare_boundary_slots,
        },
        executor::kernel::PlanRow,
        index::continuation_advances_from_ordering,
        plan::{OrderDirection, OrderSpec},
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
};
use std::cmp::Ordering;

///
/// ResolvedOrderField
///
/// One order slot resolved from field name to schema index.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedOrderField {
    field_index: Option<usize>,
    direction: OrderDirection,
}

///
/// ResolvedOrderSpec
///
/// Slot-resolved ordering shape for one execution pass.
/// This avoids repeated field-name slot scans in comparator hot loops.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedOrderSpec {
    fields: Vec<ResolvedOrderField>,
}

fn resolve_order_spec<E: EntityKind>(order: &OrderSpec) -> ResolvedOrderSpec {
    let fields = order
        .fields
        .iter()
        .map(|(field, direction)| ResolvedOrderField {
            field_index: resolve_field_slot(E::MODEL, field),
            direction: *direction,
        })
        .collect();

    ResolvedOrderSpec { fields }
}

// Convert one resolved field slot into the explicit ordering slot used for deterministic comparisons.
fn field_slot_by_index<E: EntityValue>(
    entity: &E,
    field_index: Option<usize>,
) -> CursorBoundarySlot {
    let value = field_index.and_then(|slot| entity.get_value_by_index(slot));

    match value {
        Some(value) => CursorBoundarySlot::Present(value),
        None => CursorBoundarySlot::Missing,
    }
}

pub(super) fn apply_order_spec<E, R>(rows: &mut [R], order: &OrderSpec)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    let resolved = resolve_order_spec::<E>(order);

    // Canonical order already includes the PK tie-break; comparator equality should only occur
    // for semantically equal rows. Avoid positional tie-breakers so cursor-boundary comparison can
    // share this exact ordering contract.
    rows.sort_by(|left, right| compare_entities::<E>(left.entity(), right.entity(), &resolved));
}

// Bounded ordering for first-page loads.
// We select the smallest `keep_count` rows under canonical order and then sort
// only that prefix. This preserves output and continuation behavior.
pub(super) fn apply_order_spec_bounded<E, R>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    keep_count: usize,
) where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    if keep_count == 0 {
        rows.clear();
        return;
    }

    let resolved = resolve_order_spec::<E>(order);

    if rows.len() > keep_count {
        // Partition around the last element we want to keep.
        // After this call, `0..keep_count` contains the canonical top-k set
        // (unsorted), which we then sort deterministically.
        rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_entities::<E>(left.entity(), right.entity(), &resolved)
        });
        rows.truncate(keep_count);
    }

    // Canonical order already includes the PK tie-break; comparator equality should only occur
    // for semantically equal rows. Avoid positional tie-breakers so cursor-boundary comparison can
    // share this exact ordering contract.
    rows.sort_by(|left, right| compare_entities::<E>(left.entity(), right.entity(), &resolved));
}

// Apply a strict continuation boundary using the canonical order comparator.
pub(in crate::db::executor::kernel) fn apply_cursor_boundary<E, R>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    let resolved = resolve_order_spec::<E>(order);

    debug_assert_eq!(
        boundary.slots.len(),
        resolved.fields.len(),
        "continuation boundary arity is validated by the cursor spine",
    );

    // Strict continuation: keep only rows greater than the boundary under canonical order.
    rows.retain(|row| {
        continuation_advances_from_ordering(compare_entity_with_boundary::<E>(
            row.entity(),
            &resolved,
            boundary,
        ))
    });
}

// Compare two entities according to the order spec, returning the first non-equal field ordering.
fn compare_entities<E: EntityValue>(left: &E, right: &E, order: &ResolvedOrderSpec) -> Ordering {
    for slot in &order.fields {
        let ordering = compare_entity_field_pair(left, right, *slot);

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Compare one configured order field across two entities.
fn compare_entity_field_pair<E: EntityValue>(
    left: &E,
    right: &E,
    slot: ResolvedOrderField,
) -> Ordering {
    let left_slot = field_slot_by_index(left, slot.field_index);
    let right_slot = field_slot_by_index(right, slot.field_index);
    let ordering = compare_boundary_slots(&left_slot, &right_slot);

    apply_order_direction(ordering, slot.direction)
}

// Compare one configured order field between an entity and a boundary slot.
fn compare_entity_field_to_boundary<E: EntityValue>(
    entity: &E,
    boundary_slot: &CursorBoundarySlot,
    slot: ResolvedOrderField,
) -> Ordering {
    let entity_slot = field_slot_by_index(entity, slot.field_index);
    let ordering = compare_boundary_slots(&entity_slot, boundary_slot);

    apply_order_direction(ordering, slot.direction)
}

// Compare an entity with a continuation boundary using the exact canonical ordering semantics.
fn compare_entity_with_boundary<E: EntityValue>(
    entity: &E,
    order: &ResolvedOrderSpec,
    boundary: &CursorBoundary,
) -> Ordering {
    for (slot, boundary_slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let ordering = compare_entity_field_to_boundary(entity, boundary_slot, *slot);

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}
