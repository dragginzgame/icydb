use crate::{
    db::query::{
        plan::{CursorBoundary, CursorBoundarySlot, OrderDirection, OrderSpec, logical::PlanRow},
        predicate::coercion::canonical_cmp,
    },
    traits::{EntityKind, EntityValue},
};
use std::cmp::Ordering;

pub(in crate::db::query::plan::logical) fn apply_order_spec<E, R>(rows: &mut [R], order: &OrderSpec)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    // Canonical order already includes the PK tie-break; comparator equality should only occur
    // for semantically equal rows. Avoid positional tie-breakers so cursor-boundary comparison can
    // share this exact ordering contract.
    rows.sort_by(|left, right| compare_entities::<E>(left.entity(), right.entity(), order));
}

// Bounded ordering for first-page loads.
// We select the smallest `keep_count` rows under canonical order and then sort
// only that prefix. This preserves output and continuation behavior.
pub(in crate::db::query::plan::logical) fn apply_order_spec_bounded<E, R>(
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

    if rows.len() > keep_count {
        // Partition around the last element we want to keep.
        // After this call, `0..keep_count` contains the canonical top-k set
        // (unsorted), which we then sort deterministically.
        rows.select_nth_unstable_by(keep_count - 1, |left, right| {
            compare_entities::<E>(left.entity(), right.entity(), order)
        });
        rows.truncate(keep_count);
    }

    apply_order_spec::<E, R>(rows, order);
}

// Convert one field value into the explicit ordering slot used for deterministic comparisons.
pub(in crate::db::query::plan::logical) fn field_slot<E: EntityKind + EntityValue>(
    entity: &E,
    field: &str,
) -> CursorBoundarySlot {
    let value = E::MODEL
        .field_index(field)
        .and_then(|field_index| entity.get_value_by_index(field_index));

    match value {
        Some(value) => CursorBoundarySlot::Present(value),
        None => CursorBoundarySlot::Missing,
    }
}

// Apply a strict continuation boundary using the canonical order comparator.
pub(in crate::db::query::plan::logical) fn apply_cursor_boundary<E, R>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    debug_assert_eq!(
        boundary.slots.len(),
        order.fields.len(),
        "continuation boundary arity is validated by the cursor spine",
    );

    // Strict continuation: keep only rows greater than the boundary under canonical order.
    rows.retain(|row| compare_entity_with_boundary::<E>(row.entity(), order, boundary).is_gt());
}

// Compare two entities according to the order spec, returning the first non-equal field ordering.
fn compare_entities<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    order: &OrderSpec,
) -> Ordering {
    for (field, direction) in &order.fields {
        let ordering = compare_entity_field_pair(left, right, field, *direction);

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

// Compare ordering slots using the same semantics used by query ordering:
// - Missing values sort lower than present values in ascending order
// - Present values use canonical value ordering
fn compare_order_slots(left: &CursorBoundarySlot, right: &CursorBoundarySlot) -> Ordering {
    match (left, right) {
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Missing) => Ordering::Equal,
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Present(_)) => Ordering::Less,
        (CursorBoundarySlot::Present(_), CursorBoundarySlot::Missing) => Ordering::Greater,
        (CursorBoundarySlot::Present(left_value), CursorBoundarySlot::Present(right_value)) => {
            canonical_cmp(left_value, right_value)
        }
    }
}

// Apply configured order direction to one base slot ordering.
const fn apply_order_direction(ordering: Ordering, direction: OrderDirection) -> Ordering {
    match direction {
        OrderDirection::Asc => ordering,
        OrderDirection::Desc => ordering.reverse(),
    }
}

// Compare one configured order field across two entities.
fn compare_entity_field_pair<E: EntityKind + EntityValue>(
    left: &E,
    right: &E,
    field: &str,
    direction: OrderDirection,
) -> Ordering {
    let left_slot = field_slot(left, field);
    let right_slot = field_slot(right, field);
    let ordering = compare_order_slots(&left_slot, &right_slot);

    apply_order_direction(ordering, direction)
}

// Compare one configured order field between an entity and a boundary slot.
fn compare_entity_field_to_boundary<E: EntityKind + EntityValue>(
    entity: &E,
    field: &str,
    boundary_slot: &CursorBoundarySlot,
    direction: OrderDirection,
) -> Ordering {
    let entity_slot = field_slot(entity, field);
    let ordering = compare_order_slots(&entity_slot, boundary_slot);

    apply_order_direction(ordering, direction)
}

// Compare an entity with a continuation boundary using the exact canonical ordering semantics.
fn compare_entity_with_boundary<E: EntityKind + EntityValue>(
    entity: &E,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Ordering {
    for ((field, direction), boundary_slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let ordering = compare_entity_field_to_boundary(entity, field, boundary_slot, *direction);

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}
