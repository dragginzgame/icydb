//! Module: db::query::plan::validate::order
//! Responsibility: validate order-by semantics against model fields, grouped
//! query rules, and cursor/paging invariants.
//! Does not own: broader query validation policy outside ordering semantics.
//! Boundary: keeps order-specific validation rules isolated within query-plan validation.

use crate::{
    db::{
        query::plan::{
            OrderSpec,
            expr::{ExprType, infer_expr_type, parse_supported_order_expr},
            validate::{OrderPlanError, PlanError},
        },
        schema::SchemaInfo,
    },
    model::entity::EntityModel,
};

/// Validate ORDER BY fields against the schema.
pub(crate) fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        validate_order_term(schema, field)?;
    }

    Ok(())
}

// Canonical ORDER BY validation first prefers direct schema fields and only
// falls back to the supported expression subset when no field matches.
fn validate_order_term(schema: &SchemaInfo, field: &str) -> Result<(), PlanError> {
    if let Some(field_type) = schema.field(field) {
        return field_type
            .is_orderable()
            .then_some(())
            .ok_or_else(|| PlanError::from(OrderPlanError::unorderable_field(field)));
    }

    validate_expression_order_term(schema, field)
}

fn validate_expression_order_term(schema: &SchemaInfo, field: &str) -> Result<(), PlanError> {
    let Some(expression) = parse_supported_order_expr(field) else {
        return Err(PlanError::from(OrderPlanError::unknown_field(field)));
    };
    let inferred = infer_expr_type(&expression, schema)?;

    if !matches!(
        inferred,
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_)
    ) {
        return Err(PlanError::from(OrderPlanError::unorderable_field(field)));
    }

    Ok(())
}

/// Reject duplicate non-primary-key fields in ORDER BY.
pub(crate) fn validate_no_duplicate_non_pk_order_fields(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    let mut seen = Vec::with_capacity(order.fields.len());
    let pk_field = model.primary_key.name;

    for (field, _) in &order.fields {
        let non_pk_field = field != pk_field;
        if !non_pk_field {
            continue;
        }
        if seen.contains(&field.as_str()) {
            return Err(PlanError::from(OrderPlanError::duplicate_order_field(
                field,
            )));
        }
        seen.push(field.as_str());
    }

    Ok(())
}

// Ordered plans must include exactly one terminal primary-key field so ordering is total and
// deterministic across explain, fingerprint, and executor comparison paths.
pub(crate) fn validate_primary_key_tie_break(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    order.fields.is_empty().then_some(()).map_or_else(
        || {
            let pk_field = model.primary_key.name;
            order
                .has_exact_primary_key_tie_break(pk_field)
                .then_some(())
                .ok_or_else(|| {
                    PlanError::from(OrderPlanError::missing_primary_key_tie_break(pk_field))
                })
        },
        |()| Ok(()),
    )
}
