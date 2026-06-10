//! Module: cursor::boundary
//! Responsibility: cursor boundary slot modeling and deterministic cursor boundary handling.
//! Does not own: planner query validation policy or access-path execution routing.
//! Boundary: defines cursor-boundary domain types shared by cursor planning/runtime paths.

use crate::db::{
    data::primary_key_value_from_structural_value,
    key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
};
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
    value::Value,
};
use serde::Deserialize;
use std::cmp::Ordering;

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct CursorBoundary {
    pub(in crate::db) slots: Vec<CursorBoundarySlot>,
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
    schema: &SchemaInfo,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    validate_cursor_boundary_arity(boundary, order.fields.len())?;
    validate_cursor_boundary_types(schema, order, boundary)?;

    decode_structural_primary_key_cursor_slots(schema, order, boundary)
}

// Resolve one plain order field type from canonical schema info.
fn boundary_order_field_type<'a>(
    schema: &'a SchemaInfo,
    field: &str,
    index: usize,
) -> Result<&'a FieldType, CursorPlanError> {
    schema
        .field(field)
        .ok_or_else(|| CursorPlanError::continuation_cursor_unknown_order_field_at(field, index))
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
    schema: &SchemaInfo,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), CursorPlanError> {
    for (index, (term, slot)) in order.fields.iter().zip(boundary.slots.iter()).enumerate() {
        let field = term.direct_field();
        let expression = field.is_none().then(|| term.expr().clone());
        let field_type = match field {
            Some(field) => Some(boundary_order_field_type(schema, field, index)?),
            None => None,
        };

        match slot {
            CursorBoundarySlot::Missing => {
                if field.is_some_and(|field| is_primary_key_field(schema, field)) {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(index),
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
                    if field.is_some_and(|field| is_primary_key_field(schema, field)) {
                        return Err(
                            CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(
                                index,
                            ),
                        );
                    }

                    return Err(
                        CursorPlanError::continuation_cursor_boundary_type_mismatch_at(index),
                    );
                }

                if field.is_some_and(|field| is_primary_key_field(schema, field))
                    && PrimaryKeyComponent::from_runtime_value(value).is_none()
                {
                    return Err(
                        CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(index),
                    );
                }
            }
        }
    }

    Ok(())
}

fn is_primary_key_field(schema: &SchemaInfo, field: &str) -> bool {
    schema
        .primary_key_names()
        .iter()
        .any(|primary_key_field| primary_key_field == field)
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
            Value::Int64(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Nat64(_)
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
    schema: &SchemaInfo,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    let primary_key_fields: Vec<&str> = schema
        .primary_key_names()
        .iter()
        .map(String::as_str)
        .collect();

    if let [primary_key_field] = primary_key_fields.as_slice() {
        return decode_structural_primary_key_cursor_slot_from_name(
            primary_key_field,
            order,
            boundary,
        );
    }

    let mut values = Vec::with_capacity(primary_key_fields.len());
    for primary_key_field in &primary_key_fields {
        let index = primary_key_boundary_index(order, primary_key_field)?;
        let slot = &boundary.slots[index];
        match slot {
            CursorBoundarySlot::Missing => {
                return Err(
                    CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(index),
                );
            }
            CursorBoundarySlot::Present(value) => values.push(value.clone()),
        }
    }

    primary_key_value_from_structural_value(&Value::List(values))
        .map_err(|_| CursorPlanError::continuation_cursor_primary_key_type_mismatch())
}

/// Decode one scalar structural primary-key cursor slot from one validated cursor boundary.
pub(in crate::db) fn decode_structural_primary_key_cursor_slot_from_name(
    pk_field: &str,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<PrimaryKeyValue, CursorPlanError> {
    let pk_index = primary_key_boundary_index(order, pk_field)?;
    let pk_slot = &boundary.slots[pk_index];

    match pk_slot {
        CursorBoundarySlot::Missing => {
            Err(CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(pk_index))
        }
        CursorBoundarySlot::Present(value) => primary_key_value_from_structural_value(value)
            .map_err(|_| {
                CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(pk_index)
            }),
    }
}

/// Decode one structural primary-key boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary_primary_key_value_for_names(
    boundary: Option<&CursorBoundary>,
    primary_key_names: &[&str],
) -> Result<Option<PrimaryKeyValue>, CursorPlanError> {
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        primary_key_names.len(),
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );

    let order = OrderSpec {
        fields: primary_key_names
            .iter()
            .map(|field| crate::db::query::plan::OrderTerm::field(*field, OrderDirection::Asc))
            .collect(),
    };

    if let [primary_key_name] = primary_key_names {
        return decode_structural_primary_key_cursor_slot_from_name(
            primary_key_name,
            &order,
            boundary,
        )
        .map(Some);
    }

    let mut values = Vec::with_capacity(primary_key_names.len());
    for primary_key_name in primary_key_names {
        let index = primary_key_boundary_index(&order, primary_key_name)?;
        let slot = &boundary.slots[index];
        match slot {
            CursorBoundarySlot::Missing => {
                return Err(
                    CursorPlanError::continuation_cursor_primary_key_type_mismatch_at(index),
                );
            }
            CursorBoundarySlot::Present(value) => values.push(value.clone()),
        }
    }

    primary_key_value_from_structural_value(&Value::List(values))
        .map(Some)
        .map_err(|_| CursorPlanError::continuation_cursor_primary_key_type_mismatch())
}

#[cfg(test)]
mod tests {
    use super::{
        CursorBoundary, CursorBoundarySlot, decode_pk_cursor_boundary_primary_key_value_for_names,
    };
    use crate::{
        db::key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        value::Value,
    };

    #[test]
    fn pk_ordered_boundary_decode_accepts_ordered_composite_primary_key_fields() {
        let boundary = CursorBoundary {
            slots: vec![
                CursorBoundarySlot::Present(Value::Nat64(7)),
                CursorBoundarySlot::Present(Value::Int64(-3)),
            ],
        };
        let composite = CompositePrimaryKeyValue::try_from_components(&[
            PrimaryKeyComponent::Nat64(7),
            PrimaryKeyComponent::Int64(-3),
        ])
        .expect("fixture components should form composite key");

        let decoded = decode_pk_cursor_boundary_primary_key_value_for_names(
            Some(&boundary),
            &["tenant", "id"],
        )
        .expect("composite pk cursor boundary should decode");

        assert_eq!(decoded, Some(PrimaryKeyValue::Composite(composite)));
    }
}
