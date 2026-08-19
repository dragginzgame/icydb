//! Module: db::query::plan::validate::order
//! Responsibility: validate order-by semantics against model fields, grouped
//! query rules, and cursor/paging invariants.
//! Does not own: broader query validation policy outside ordering semantics.
//! Boundary: keeps order-specific validation rules isolated within query-plan validation.

use crate::db::{
    query::plan::{
        OrderSpec, OrderTerm,
        expr::{ExprType, infer_expr_type},
        validate::{OrderPlanError, PlanError},
    },
    schema::SchemaInfo,
};
use icydb_diagnostic_code::QueryFieldRole;

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
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_) | ExprType::Unknown
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
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_)
    ) {
        return Err(PlanError::from(OrderPlanError::unorderable_field(
            term_index,
        )));
    }

    Ok(())
}

/// Reject duplicate non-primary-key fields in ORDER BY.
pub(in crate::db::query::plan::validate) fn validate_no_duplicate_non_pk_order_fields(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    let mut seen = Vec::with_capacity(order.fields.len());
    let primary_key_names = schema.primary_key_names();

    for (term_index, term) in order.fields.iter().enumerate() {
        let field = term
            .direct_field()
            .map_or_else(|| term.rendered_label(), str::to_owned);
        let non_pk_field = !primary_key_names.iter().any(|pk_field| pk_field == &field);
        if !non_pk_field {
            continue;
        }
        if let Some((first_term_index, _)) =
            seen.iter().find(|(_, seen_field)| seen_field == &field)
        {
            return Err(PlanError::from(OrderPlanError::duplicate_order_field(
                *first_term_index,
                term_index,
            )));
        }
        seen.push((term_index, field));
    }

    Ok(())
}

// Ordered scalar plans must include every primary-key component somewhere in
// the order tuple so ordering is total and deterministic across explain,
// fingerprint, and executor comparison paths. The canonicalizer appends only
// missing components; explicit user order terms keep their declared positions.
pub(in crate::db::query::plan::validate) fn validate_primary_key_tie_break(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    order.fields.is_empty().then_some(()).map_or_else(
        || {
            let primary_key_names = schema.primary_key_names();
            let primary_key_name_refs: Vec<&str> =
                primary_key_names.iter().map(String::as_str).collect();
            if let Some(primary_key_index) =
                primary_key_name_refs.iter().copied().enumerate().find_map(
                    |(primary_key_index, primary_key_name)| {
                        let primary_key_present = order
                            .fields
                            .iter()
                            .any(|term| term.direct_field() == Some(primary_key_name));

                        (!primary_key_present).then_some(primary_key_index)
                    },
                )
            {
                return Err(PlanError::from(
                    OrderPlanError::missing_primary_key_tie_break(primary_key_index),
                ));
            }

            Ok(())
        },
        |()| Ok(()),
    )
}
