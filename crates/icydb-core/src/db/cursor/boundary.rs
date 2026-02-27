use crate::{
    db::{
        contracts::{SchemaInfo, literal_matches_type},
        cursor::CursorPlanError,
        direction::Direction,
        query::plan::{OrderDirection, OrderSpec},
    },
    model::entity::{EntityModel, resolve_field_slot},
    traits::{EntityKind, EntityValue, FieldValue},
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
    /// Build one cursor boundary from one entity using canonical order fields.
    #[must_use]
    pub(in crate::db) fn from_ordered_entity<E: EntityKind + EntityValue>(
        entity: &E,
        order: &OrderSpec,
    ) -> Self {
        Self {
            slots: boundary_slots_from_entity(entity, order),
        }
    }
}

/// Build boundary slots from one entity using canonical order fields.
#[must_use]
pub(in crate::db) fn boundary_slots_from_entity<E: EntityKind + EntityValue>(
    entity: &E,
    order: &OrderSpec,
) -> Vec<CursorBoundarySlot> {
    order
        .fields
        .iter()
        .map(|(field, _)| {
            let value = resolve_field_slot(E::MODEL, field)
                .and_then(|slot| entity.get_value_by_index(slot));

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
            Value::canonical_cmp(left_value, right_value)
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
        return Err(CursorPlanError::InvalidContinuationCursorPayload {
            reason: "continuation cursor direction does not match executable plan direction"
                .to_string(),
        });
    }

    Ok(())
}

/// Validate continuation initial-offset compatibility.
pub(in crate::db) const fn validate_cursor_window_offset(
    expected_initial_offset: u32,
    actual_initial_offset: u32,
) -> Result<(), CursorPlanError> {
    if actual_initial_offset != expected_initial_offset {
        return Err(CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset: expected_initial_offset,
            actual_offset: actual_initial_offset,
        });
    }

    Ok(())
}

/// Validate one cursor boundary arity against canonical order width.
pub(in crate::db) const fn validate_cursor_boundary_arity(
    boundary: &CursorBoundary,
    expected_arity: usize,
) -> Result<(), CursorPlanError> {
    if boundary.slots.len() != expected_arity {
        return Err(CursorPlanError::ContinuationCursorBoundaryArityMismatch {
            expected: expected_arity,
            found: boundary.slots.len(),
        });
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
    let schema = SchemaInfo::from_entity_model(model).map_err(|err| {
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: err.to_string(),
        }
    })?;

    for ((field, _), slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field_type = schema.field(field).ok_or_else(|| {
            CursorPlanError::InvalidContinuationCursorPayload {
                reason: format!("unknown order field '{field}'"),
            }
        })?;

        match slot {
            CursorBoundarySlot::Missing => {
                if field == model.primary_key.name {
                    return Err(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: None,
                    });
                }
            }
            CursorBoundarySlot::Present(value) => {
                if !literal_matches_type(value, field_type) {
                    if field == model.primary_key.name {
                        return Err(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                            field: field.clone(),
                            expected: field_type.to_string(),
                            value: Some(value.clone()),
                        });
                    }

                    return Err(CursorPlanError::ContinuationCursorBoundaryTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: value.clone(),
                    });
                }

                if field == model.primary_key.name && Value::as_storage_key(value).is_none() {
                    return Err(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: Some(value.clone()),
                    });
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
        .ok_or_else(|| CursorPlanError::InvalidContinuationCursorPayload {
            reason: format!(
                "order specification must end with primary key '{pk_field}' as deterministic tie-break"
            ),
        })?;

    let schema = SchemaInfo::from_entity_model(model).map_err(|err| {
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: err.to_string(),
        }
    })?;
    let expected = schema
        .field(pk_field)
        .ok_or_else(|| CursorPlanError::InvalidContinuationCursorPayload {
            reason: format!("unknown order field '{pk_field}'"),
        })?
        .to_string();
    let pk_slot = &boundary.slots[pk_index];

    match pk_slot {
        CursorBoundarySlot::Missing => {
            Err(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                field: pk_field.to_string(),
                expected,
                value: None,
            })
        }
        CursorBoundarySlot::Present(value) => K::from_value(value).ok_or_else(|| {
            CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                field: pk_field.to_string(),
                expected,
                value: Some(value.clone()),
            }
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
