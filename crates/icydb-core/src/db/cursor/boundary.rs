//! Module: cursor::boundary
//! Responsibility: cursor boundary slot modeling and deterministic cursor boundary handling.
//! Does not own: planner query validation policy or access-path execution routing.
//! Boundary: defines cursor-boundary domain types shared by cursor planning/runtime paths.

use crate::{
    db::{
        cursor::CursorPlanError,
        direction::Direction,
        query::plan::{ExpressionOrderTerm, OrderDirection, OrderSpec},
        scalar_expr::derive_expression_order_value,
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    model::entity::{EntityModel, resolve_field_slot},
    traits::FieldValue,
    value::{StorageKey, Value},
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
            let value = boundary_slot_value_from_reader(model, field, read_slot);

            match value {
                Some(value) => CursorBoundarySlot::Present(value),
                None => CursorBoundarySlot::Missing,
            }
        })
        .collect()
}

// Resolve one canonical boundary slot value from the underlying structural row.
fn boundary_slot_value_from_reader<F>(
    model: &EntityModel,
    field: &str,
    read_slot: &mut F,
) -> Option<Value>
where
    F: FnMut(usize) -> Option<Value>,
{
    if let Some(expression) = ExpressionOrderTerm::parse(field) {
        let slot = resolve_field_slot(model, expression.field())?;
        let value = read_slot(slot)?;

        return derive_expression_order_value(expression, &value);
    }

    resolve_field_slot(model, field).and_then(read_slot)
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
        return Err(CursorPlanError::continuation_cursor_direction_mismatch());
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

// Load canonical schema information for cursor boundary validation.
fn boundary_schema(model: &EntityModel) -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(model)
}

// Resolve one order field type from canonical schema info.
fn boundary_order_field_type<'a>(
    schema: &'a SchemaInfo,
    field: &str,
) -> Result<&'a FieldType, CursorPlanError> {
    if let Some(expression) = ExpressionOrderTerm::parse(field) {
        return schema
            .field(expression.field())
            .ok_or_else(|| CursorPlanError::continuation_cursor_unknown_order_field(field));
    }

    schema
        .field(field)
        .ok_or_else(|| CursorPlanError::continuation_cursor_unknown_order_field(field))
}

// Resolve the deterministic primary-key tie-break position from one order spec.
fn primary_key_boundary_index(order: &OrderSpec, pk_field: &str) -> Result<usize, CursorPlanError> {
    order
        .fields
        .iter()
        .position(|(field, _)| field == pk_field)
        .ok_or_else(|| {
            CursorPlanError::continuation_cursor_primary_key_tie_break_required(pk_field)
        })
}

/// Validate cursor boundary slot types against canonical order fields.
pub(in crate::db) fn validate_cursor_boundary_types(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), CursorPlanError> {
    let schema = boundary_schema(model);

    for ((field, _), slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field_type = boundary_order_field_type(schema, field)?;

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
    let pk_index = primary_key_boundary_index(order, pk_field)?;
    let schema = boundary_schema(model);
    let expected = boundary_order_field_type(schema, pk_field)?.to_string();
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

/// Decode the structural primary-key cursor slot from one validated cursor boundary.
pub(in crate::db) fn decode_structural_primary_key_cursor_slot(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<StorageKey, CursorPlanError> {
    let pk_field = model.primary_key.name;
    let pk_index = primary_key_boundary_index(order, pk_field)?;
    let schema = boundary_schema(model);
    let expected = boundary_order_field_type(schema, pk_field)?.to_string();
    let pk_slot = &boundary.slots[pk_index];

    match pk_slot {
        CursorBoundarySlot::Missing => Err(
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                pk_field.to_string(),
                expected,
                None,
            ),
        ),
        CursorBoundarySlot::Present(value) => value.as_storage_key().ok_or_else(|| {
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                pk_field.to_string(),
                expected,
                Some(value.clone()),
            )
        }),
    }
}

/// Decode one structural primary-key boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary_storage_key(
    boundary: Option<&CursorBoundary>,
    model: &EntityModel,
) -> Result<Option<StorageKey>, CursorPlanError> {
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        1,
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );

    let order = OrderSpec {
        fields: vec![(model.primary_key.name.to_string(), OrderDirection::Asc)],
    };

    decode_structural_primary_key_cursor_slot(model, &order, boundary).map(Some)
}
