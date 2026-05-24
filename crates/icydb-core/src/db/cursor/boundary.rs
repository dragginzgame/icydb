//! Module: cursor::boundary
//! Responsibility: cursor boundary slot modeling and deterministic cursor boundary handling.
//! Does not own: planner query validation policy or access-path execution routing.
//! Boundary: defines cursor-boundary domain types shared by cursor planning/runtime paths.

use crate::db::{data::primary_key_value_from_structural_value, key_taxonomy::PrimaryKeyValue};
use crate::{
    db::{
        cursor::CursorPlanError,
        direction::Direction,
        query::plan::{
            OrderDirection, OrderSpec,
            expr::{Expr, ExprType, infer_expr_type},
        },
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    model::entity::EntityModel,
    value::Value,
};
use serde::Deserialize;
use std::cmp::Ordering;

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct CursorBoundary {
    pub(crate) slots: Vec<CursorBoundarySlot>,
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
pub(in crate::db) fn validate_cursor_boundary_for_order(
    model: &EntityModel,
    schema: &SchemaInfo,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    validate_cursor_boundary_arity(boundary, order.fields.len())?;
    validate_cursor_boundary_types(model, schema, order, boundary)?;

    decode_structural_primary_key_cursor_slots(model, order, boundary)
}

// Resolve one plain order field type from canonical schema info.
fn boundary_order_field_type<'a>(
    schema: &'a SchemaInfo,
    field: &str,
) -> Result<&'a FieldType, CursorPlanError> {
    schema
        .field(field)
        .ok_or_else(|| CursorPlanError::continuation_cursor_unknown_order_field(field))
}

// Resolve the deterministic primary-key tie-break position from one order spec.
fn primary_key_boundary_index(order: &OrderSpec, pk_field: &str) -> Result<usize, CursorPlanError> {
    order
        .fields
        .iter()
        .position(|term| term.direct_field() == Some(pk_field))
        .ok_or_else(|| {
            CursorPlanError::continuation_cursor_primary_key_tie_break_required(pk_field)
        })
}

/// Validate cursor boundary slot types against canonical order fields.
pub(in crate::db) fn validate_cursor_boundary_types(
    model: &EntityModel,
    schema: &SchemaInfo,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), CursorPlanError> {
    for (term, slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field = term.direct_field();
        let expression = field.is_none().then(|| term.expr().clone());
        let field_type = match field {
            Some(field) => Some(boundary_order_field_type(schema, field)?),
            None => None,
        };
        let rendered = expression.as_ref().map_or_else(
            || {
                field
                    .expect("field-backed order term should have field")
                    .to_owned()
            },
            |_| term.rendered_label(),
        );

        match slot {
            CursorBoundarySlot::Missing => {
                if field.is_some_and(|field| {
                    model
                        .primary_key_model()
                        .fields()
                        .iter()
                        .any(|primary_key_field| primary_key_field.name() == field)
                }) {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                            rendered,
                            boundary_order_expected_type_name(
                                schema,
                                field_type,
                                expression.as_ref(),
                            ),
                            None,
                        ),
                    );
                }
            }
            CursorBoundarySlot::Present(value) => {
                let type_matches = match (field_type, expression.as_ref()) {
                    (Some(field_type), None) => literal_matches_type(value, field_type),
                    (None, Some(expression)) => {
                        boundary_order_expression_value_matches(schema, expression, value)?
                    }
                    _ => false,
                };

                if !type_matches {
                    let expected =
                        boundary_order_expected_type_name(schema, field_type, expression.as_ref());

                    if field.is_some_and(|field| {
                        model
                            .primary_key_model()
                            .fields()
                            .iter()
                            .any(|primary_key_field| primary_key_field.name() == field)
                    }) {
                        return Err(
                            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                                rendered,
                                expected,
                                Some(value.clone()),
                            ),
                        );
                    }

                    return Err(CursorPlanError::continuation_cursor_boundary_type_mismatch(
                        rendered,
                        expected,
                        value.clone(),
                    ));
                }

                if field.is_some_and(|field| {
                    model
                        .primary_key_model()
                        .fields()
                        .iter()
                        .any(|primary_key_field| primary_key_field.name() == field)
                }) && Value::as_primary_key_value(value).is_none()
                {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                            rendered,
                            boundary_order_expected_type_name(
                                schema,
                                field_type,
                                expression.as_ref(),
                            ),
                            Some(value.clone()),
                        ),
                    );
                }
            }
        }
    }

    Ok(())
}

fn boundary_order_expected_type_name(
    schema: &SchemaInfo,
    field_type: Option<&FieldType>,
    expression: Option<&Expr>,
) -> String {
    if let Some(field_type) = field_type {
        return field_type.to_string();
    }

    let Some(expression) = expression else {
        return "unknown".to_string();
    };

    match infer_expr_type(expression, schema) {
        Ok(ExprType::Bool) => "bool".to_string(),
        Ok(ExprType::Blob) => "blob".to_string(),
        Ok(ExprType::Text) => "text".to_string(),
        Ok(ExprType::Numeric(_)) => "numeric".to_string(),
        Ok(ExprType::Collection) => "collection".to_string(),
        Ok(ExprType::Structured) => "structured".to_string(),
        Ok(ExprType::Opaque) => "opaque".to_string(),
        Ok(ExprType::Unknown) | Err(_) => "unknown".to_string(),
        #[cfg(test)]
        Ok(ExprType::Null) => "null".to_string(),
    }
}

fn boundary_order_expression_value_matches(
    schema: &SchemaInfo,
    expression: &Expr,
    value: &Value,
) -> Result<bool, CursorPlanError> {
    let inferred = infer_expr_type(expression, schema)
        .map_err(|_| CursorPlanError::continuation_cursor_unknown_order_field("expression"))?;

    Ok(match inferred {
        ExprType::Bool => matches!(value, Value::Bool(_)),
        ExprType::Blob => matches!(value, Value::Blob(_)),
        ExprType::Text => matches!(value, Value::Text(_) | Value::Enum(_)),
        ExprType::Numeric(_) => matches!(
            value,
            Value::Int(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Nat(_)
                | Value::Nat128(_)
                | Value::NatBig(_)
                | Value::Decimal(_)
                | Value::Float32(_)
                | Value::Float64(_)
                | Value::Date(_)
                | Value::Duration(_)
                | Value::Timestamp(_)
        ),
        ExprType::Collection | ExprType::Structured | ExprType::Opaque | ExprType::Unknown => false,
        #[cfg(test)]
        ExprType::Null => matches!(value, Value::Null),
    })
}

/// Decode the structural primary-key cursor slot from one validated cursor boundary.
pub(in crate::db) fn decode_structural_primary_key_cursor_slots(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    let primary_key_fields: Vec<&str> = model
        .primary_key_model()
        .fields()
        .iter()
        .map(crate::model::field::FieldModel::name)
        .collect();

    if let [primary_key_field] = primary_key_fields.as_slice() {
        return decode_structural_primary_key_cursor_slot_from_name(
            primary_key_field,
            order,
            boundary,
        );
    }

    let mut values = Vec::with_capacity(primary_key_fields.len());
    for primary_key_field in primary_key_fields {
        let index = primary_key_boundary_index(order, primary_key_field)?;
        let slot = &boundary.slots[index];
        match slot {
            CursorBoundarySlot::Missing => {
                return Err(
                    CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                        primary_key_field.to_string(),
                        "primary key value".to_string(),
                        None,
                    ),
                );
            }
            CursorBoundarySlot::Present(value) => values.push(value.clone()),
        }
    }

    primary_key_value_from_structural_value(&Value::List(values)).map_err(|_| {
        CursorPlanError::continuation_cursor_primary_key_type_mismatch(
            model.primary_key.name.to_string(),
            "primary key value".to_string(),
            None,
        )
    })
}

/// Decode one scalar structural primary-key cursor slot from one validated cursor boundary.
pub(in crate::db) fn decode_structural_primary_key_cursor_slot_from_name(
    pk_field: &str,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    let pk_index = primary_key_boundary_index(order, pk_field)?;
    let expected = "primary key value".to_string();
    let pk_slot = &boundary.slots[pk_index];

    match pk_slot {
        CursorBoundarySlot::Missing => Err(
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                pk_field.to_string(),
                expected,
                None,
            ),
        ),
        CursorBoundarySlot::Present(value) => primary_key_value_from_structural_value(value)
            .map_err(|_| {
                CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                    pk_field.to_string(),
                    expected,
                    Some(value.clone()),
                )
            }),
    }
}

/// Decode one structural primary-key boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary_primary_key_value_for_name(
    boundary: Option<&CursorBoundary>,
    primary_key_name: &str,
) -> Result<Option<PrimaryKeyValue>, CursorPlanError> {
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        1,
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );

    let order = OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            primary_key_name,
            OrderDirection::Asc,
        )],
    };

    decode_structural_primary_key_cursor_slot_from_name(primary_key_name, &order, boundary)
        .map(Some)
}
