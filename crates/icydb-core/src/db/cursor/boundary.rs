//! Module: cursor::boundary
//! Responsibility: cursor boundary slot modeling and deterministic cursor boundary handling.
//! Does not own: planner query validation policy or access-path execution routing.
//! Boundary: defines cursor-boundary domain types shared by cursor planning/runtime paths.

use crate::{
    db::{
        contracts::canonical_value_compare,
        cursor::CursorPlanError,
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec},
        schema::{SchemaInfo, literal_matches_type},
    },
    model::entity::{EntityModel, resolve_field_slot},
    traits::{EntityKind, FieldValue},
    value::Value,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CursorBoundary {
    pub(crate) slots: Vec<CursorBoundarySlot>,
}

impl CursorBoundary {
    /// Build one cursor boundary from one structural slot reader using canonical
    /// order fields.
    #[must_use]
    pub(in crate::db) fn from_slot_reader<F>(
        model: &EntityModel,
        order: &OrderSpec,
        read_slot: &mut F,
    ) -> Self
    where
        F: FnMut(usize) -> Option<Value>,
    {
        Self {
            slots: boundary_slots_from_slot_reader(model, order, read_slot),
        }
    }
}

/// Build boundary slots from one structural slot reader using canonical order fields.
#[must_use]
pub(in crate::db) fn boundary_slots_from_slot_reader<F>(
    model: &EntityModel,
    order: &OrderSpec,
    read_slot: &mut F,
) -> Vec<CursorBoundarySlot>
where
    F: FnMut(usize) -> Option<Value>,
{
    order
        .fields
        .iter()
        .map(|(field, _)| {
            let value = resolve_field_slot(model, field).and_then(&mut *read_slot);

            match value {
                Some(value) => CursorBoundarySlot::Present(value),
                None => CursorBoundarySlot::Missing,
            }
        })
        .collect()
}

/// Compare two cursor boundary slots under canonical cursor ordering semantics.
#[must_use]
pub(in crate::db) fn compare_boundary_slots(
    left: &CursorBoundarySlot,
    right: &CursorBoundarySlot,
) -> Ordering {
    match (left, right) {
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Missing) => Ordering::Equal,
        (CursorBoundarySlot::Missing, CursorBoundarySlot::Present(_)) => Ordering::Less,
        (CursorBoundarySlot::Present(_), CursorBoundarySlot::Missing) => Ordering::Greater,
        (CursorBoundarySlot::Present(left_value), CursorBoundarySlot::Present(right_value)) => {
            canonical_value_compare(left_value, right_value)
        }
    }
}

/// Apply one order direction to one base slot ordering.
#[must_use]
pub(in crate::db) const fn apply_order_direction(
    ordering: Ordering,
    direction: OrderDirection,
) -> Ordering {
    match direction {
        OrderDirection::Asc => ordering,
        OrderDirection::Desc => ordering.reverse(),
    }
}

/// Validate continuation direction compatibility.
pub(in crate::db) fn validate_cursor_direction(
    expected_direction: Direction,
    actual_direction: Direction,
) -> Result<(), CursorPlanError> {
    if actual_direction != expected_direction {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "continuation cursor direction does not match executable plan direction",
        ));
    }

    Ok(())
}

/// Validate continuation initial-offset compatibility.
pub(in crate::db) const fn validate_cursor_window_offset(
    expected_initial_offset: u32,
    actual_initial_offset: u32,
) -> Result<(), CursorPlanError> {
    if actual_initial_offset != expected_initial_offset {
        return Err(CursorPlanError::continuation_cursor_window_mismatch(
            expected_initial_offset,
            actual_initial_offset,
        ));
    }

    Ok(())
}

/// Validate one cursor boundary arity against canonical order width.
pub(in crate::db) const fn validate_cursor_boundary_arity(
    boundary: &CursorBoundary,
    expected_arity: usize,
) -> Result<(), CursorPlanError> {
    if boundary.slots.len() != expected_arity {
        return Err(
            CursorPlanError::continuation_cursor_boundary_arity_mismatch(
                expected_arity,
                boundary.slots.len(),
            ),
        );
    }

    Ok(())
}

/// Validate one cursor boundary against canonical order fields and return typed PK key.
pub(in crate::db) fn validate_cursor_boundary_for_order<K: FieldValue>(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<K, CursorPlanError> {
    validate_cursor_boundary_arity(boundary, order.fields.len())?;
    validate_cursor_boundary_types(model, order, boundary)?;

    decode_typed_primary_key_cursor_slot::<K>(model, order, boundary)
}

/// Validate cursor boundary slot types against canonical order fields.
pub(in crate::db) fn validate_cursor_boundary_types(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), CursorPlanError> {
    let schema = SchemaInfo::from_entity_model(model)
        .map_err(|err| CursorPlanError::invalid_continuation_cursor_payload(err.to_string()))?;

    for ((field, _), slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field_type = schema.field(field).ok_or_else(|| {
            CursorPlanError::invalid_continuation_cursor_payload(format!(
                "unknown order field '{field}'"
            ))
        })?;

        match slot {
            CursorBoundarySlot::Missing => {
                if field == model.primary_key.name {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                            field.clone(),
                            field_type.to_string(),
                            None,
                        ),
                    );
                }
            }
            CursorBoundarySlot::Present(value) => {
                if !literal_matches_type(value, field_type) {
                    if field == model.primary_key.name {
                        return Err(
                            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                                field.clone(),
                                field_type.to_string(),
                                Some(value.clone()),
                            ),
                        );
                    }

                    return Err(CursorPlanError::continuation_cursor_boundary_type_mismatch(
                        field.clone(),
                        field_type.to_string(),
                        value.clone(),
                    ));
                }

                if field == model.primary_key.name && Value::as_storage_key(value).is_none() {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                            field.clone(),
                            field_type.to_string(),
                            Some(value.clone()),
                        ),
                    );
                }
            }
        }
    }

    Ok(())
}

/// Decode the typed primary-key cursor slot from one validated cursor boundary.
pub(in crate::db) fn decode_typed_primary_key_cursor_slot<K: FieldValue>(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<K, CursorPlanError> {
    let pk_field = model.primary_key.name;
    let pk_index = order
        .fields
        .iter()
        .position(|(field, _)| field == pk_field)
        .ok_or_else(|| {
            CursorPlanError::invalid_continuation_cursor_payload(format!(
                "order specification must end with primary key '{pk_field}' as deterministic tie-break"
            ))
        })?;

    let schema = SchemaInfo::from_entity_model(model)
        .map_err(|err| CursorPlanError::invalid_continuation_cursor_payload(err.to_string()))?;
    let expected = schema
        .field(pk_field)
        .ok_or_else(|| {
            CursorPlanError::invalid_continuation_cursor_payload(format!(
                "unknown order field '{pk_field}'"
            ))
        })?
        .to_string();
    let pk_slot = &boundary.slots[pk_index];

    match pk_slot {
        CursorBoundarySlot::Missing => Err(
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                pk_field.to_string(),
                expected,
                None,
            ),
        ),
        CursorBoundarySlot::Present(value) => K::from_value(value).ok_or_else(|| {
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                pk_field.to_string(),
                expected,
                Some(value.clone()),
            )
        }),
    }
}

/// Decode one typed primary-key boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary<E>(
    boundary: Option<&CursorBoundary>,
) -> Result<Option<E::Key>, CursorPlanError>
where
    E: EntityKind,
    E::Key: FieldValue,
{
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        1,
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );

    let order = OrderSpec {
        fields: vec![(E::MODEL.primary_key.name.to_string(), OrderDirection::Asc)],
    };

    decode_typed_primary_key_cursor_slot::<E::Key>(E::MODEL, &order, boundary).map(Some)
}
