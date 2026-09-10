//! Module: db::query::plan::validate::order
//! Responsibility: validate order-by semantics against model fields, grouped
//! query rules, and cursor/paging invariants.
//! Does not own: broader query validation policy outside ordering semantics.
//! Boundary: keeps order-specific validation rules isolated within query-plan validation.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    query::plan::{
        OrderSpec, OrderTerm,
        expr::{ExprType, infer_expr_type},
        validate::{OrderPlanError, PlanError},
    },
    query::{
        builder::scalar_projection::write_scalar_projection_expr_plan_label,
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, QueryFieldRole};
use std::borrow::Cow;

/// Validate ORDER BY fields against the schema.
pub(in crate::db::query::plan::validate) fn validate_order(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    for (term_index, term) in order.fields.iter().enumerate() {
        validate_order_term(schema, term_index, term)?;
    }

    Ok(())
}

// Canonical ORDER BY validation first prefers direct schema fields and only
// falls back to the supported expression subset when no field matches.
fn validate_order_term(
    schema: &SchemaInfo,
    term_index: usize,
    term: &OrderTerm,
) -> Result<(), PlanError> {
    if let Some(field) = term.direct_field() {
        let Some(field_type) = schema.field(field) else {
            return Err(
                PlanError::from(OrderPlanError::unknown_field(term_index, field))
                    .attach_query_field(QueryFieldRole::OrderBy),
            );
        };

        return field_type
            .is_orderable()
            .then_some(())
            .ok_or_else(|| PlanError::from(OrderPlanError::unorderable_field(term_index)));
    }

    if matches!(
        term.expr(),
        crate::db::query::plan::expr::Expr::FieldPath(_)
    ) {
        return validate_field_path_order_term(schema, term_index, term);
    }

    validate_expression_order_term(schema, term_index, term)
}

fn validate_field_path_order_term(
    schema: &SchemaInfo,
    term_index: usize,
    term: &OrderTerm,
) -> Result<(), PlanError> {
    let inferred = infer_expr_type(term.expr(), schema)
        .map_err(|error| error.attach_query_field(QueryFieldRole::OrderBy))?;

    if matches!(
        inferred,
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_) | ExprType::U256 | ExprType::Unknown
    ) {
        return Ok(());
    }

    Err(PlanError::from(OrderPlanError::unorderable_field(
        term_index,
    )))
}

fn validate_expression_order_term(
    schema: &SchemaInfo,
    term_index: usize,
    term: &OrderTerm,
) -> Result<(), PlanError> {
    let inferred = infer_expr_type(term.expr(), schema)
        .map_err(|error| error.attach_query_field(QueryFieldRole::OrderBy))?;

    if !matches!(
        inferred,
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_) | ExprType::U256
    ) {
        return Err(PlanError::from(OrderPlanError::unorderable_field(
            term_index,
        )));
    }

    Ok(())
}

/// Reject duplicate non-primary-key fields in ORDER BY.
pub(in crate::db::query::plan::validate) fn validate_no_duplicate_non_pk_order_fields(
    primary_key_names: &[String],
    order: &OrderSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let mut seen: Vec<(usize, Cow<'_, str>)> = Vec::new();

    for (term_index, term) in order.fields.iter().enumerate() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        // Preserve label-based duplicate semantics, but borrow direct names.
        // Only computed labels need the shared abortable rendering sink.
        let field = match term.direct_field() {
            Some(field) => Cow::Borrowed(field),
            None => Cow::Owned(
                work.render_text(|out| write_scalar_projection_expr_plan_label(term.expr(), out))?,
            ),
        };
        let mut primary_key = false;
        for pk_field in primary_key_names {
            if equal_order_labels(pk_field, &field, work)? {
                primary_key = true;
                break;
            }
        }
        if primary_key {
            continue;
        }
        for (first_term_index, seen_field) in &seen {
            if equal_order_labels(seen_field, &field, work)? {
                return Err(PlanError::from(OrderPlanError::duplicate_order_field(
                    *first_term_index,
                    term_index,
                ))
                .into());
            }
        }
        work.reserve_vec(&mut seen, 1)?;
        seen.push((term_index, field));
    }

    Ok(())
}

// Ordered scalar plans must include every primary-key component somewhere in
// the order tuple so ordering is total and deterministic across explain,
// fingerprint, and executor comparison paths. The canonicalizer appends only
// missing components; explicit user order terms keep their declared positions.
pub(in crate::db::query::plan::validate) fn validate_primary_key_tie_break(
    primary_key_names: &[String],
    order: &OrderSpec,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    if order.fields.is_empty() {
        return Ok(());
    }
    for (primary_key_index, primary_key_name) in primary_key_names.iter().enumerate() {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let mut present = false;
        for term in &order.fields {
            let Some(field) = term.direct_field() else {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                continue;
            };
            if equal_order_labels(field, primary_key_name, work)? {
                present = true;
                break;
            }
        }
        if !present {
            return Err(
                PlanError::from(OrderPlanError::missing_primary_key_tie_break(
                    primary_key_index,
                ))
                .into(),
            );
        }
    }
    Ok(())
}

// Admit each equality check and its maximum byte comparison before comparing.
fn equal_order_labels(
    left: &str,
    right: &str,
    work: &PreparationWork<'_>,
) -> Result<bool, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if left.len() != right.len() {
        return Ok(false);
    }
    work.charge(Resource::PredicateExpressionSteps, left.len() as u64)?;
    Ok(left == right)
}
