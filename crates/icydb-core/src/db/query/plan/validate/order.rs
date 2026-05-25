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

/// Validate ORDER BY fields against the schema.
pub(in crate::db::query::plan::validate) fn validate_order(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    for term in &order.fields {
        validate_order_term(schema, term)?;
    }

    Ok(())
}

// Canonical ORDER BY validation first prefers direct schema fields and only
// falls back to the supported expression subset when no field matches.
fn validate_order_term(schema: &SchemaInfo, term: &OrderTerm) -> Result<(), PlanError> {
    if let Some(field) = term.direct_field() {
        let Some(field_type) = schema.field(field) else {
            return Err(PlanError::from(OrderPlanError::UnknownField {
                field: field.to_owned(),
            }));
        };

        return field_type
            .is_orderable()
            .then_some(())
            .ok_or_else(|| PlanError::from(OrderPlanError::unorderable_field(field)));
    }

    if matches!(
        term.expr(),
        crate::db::query::plan::expr::Expr::FieldPath(_)
    ) {
        return validate_field_path_order_term(schema, term);
    }

    validate_expression_order_term(schema, term)
}

fn validate_field_path_order_term(schema: &SchemaInfo, term: &OrderTerm) -> Result<(), PlanError> {
    let inferred = infer_expr_type(term.expr(), schema)?;

    if matches!(
        inferred,
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_) | ExprType::Unknown
    ) {
        return Ok(());
    }

    Err(PlanError::from(OrderPlanError::unorderable_field(
        term.rendered_label(),
    )))
}

fn validate_expression_order_term(schema: &SchemaInfo, term: &OrderTerm) -> Result<(), PlanError> {
    let inferred = infer_expr_type(term.expr(), schema)?;

    if !matches!(
        inferred,
        ExprType::Bool | ExprType::Text | ExprType::Numeric(_)
    ) {
        return Err(PlanError::from(OrderPlanError::unorderable_field(
            term.rendered_label(),
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

    for term in &order.fields {
        let field = term
            .direct_field()
            .map_or_else(|| term.rendered_label(), str::to_owned);
        let non_pk_field = !primary_key_names.iter().any(|pk_field| pk_field == &field);
        if !non_pk_field {
            continue;
        }
        if seen.iter().any(|seen_field| seen_field == &field) {
            return Err(PlanError::from(OrderPlanError::duplicate_order_field(
                field,
            )));
        }
        seen.push(field);
    }

    Ok(())
}

// Ordered plans must include exactly one terminal primary-key field so ordering is total and
// deterministic across explain, fingerprint, and executor comparison paths.
pub(in crate::db::query::plan::validate) fn validate_primary_key_tie_break(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    order.fields.is_empty().then_some(()).map_or_else(
        || {
            let primary_key_names = schema.primary_key_names();
            let primary_key_name_refs: Vec<&str> =
                primary_key_names.iter().map(String::as_str).collect();
            order
                .has_exact_primary_key_tie_break_fields(primary_key_name_refs.as_slice())
                .then_some(())
                .ok_or_else(|| {
                    PlanError::from(OrderPlanError::missing_primary_key_tie_break(
                        primary_key_name_refs
                            .first()
                            .copied()
                            .unwrap_or("<missing>"),
                    ))
                })
        },
        |()| Ok(()),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        OrderPlanError, PlanError, validate_no_duplicate_non_pk_order_fields,
        validate_primary_key_tie_break,
    };
    use crate::{
        db::{
            query::plan::{OrderDirection, OrderSpec, OrderTerm},
            schema::SchemaInfo,
        },
        model::{
            entity::{EntityModel, PrimaryKeyModel},
            field::{FieldKind, FieldModel},
        },
    };

    static FIELDS: [FieldModel; 3] = [
        FieldModel::generated("tenant_id", FieldKind::Nat64),
        FieldModel::generated("local_id", FieldKind::Nat64),
        FieldModel::generated("rank", FieldKind::Nat64),
    ];
    static PK_FIELDS: [&FieldModel; 2] = [&FIELDS[0], &FIELDS[1]];
    static MODEL: EntityModel = EntityModel::generated_with_primary_key_model(
        "query::plan::validate::order::tests::CompositeOrderEntity",
        "CompositeOrderEntity",
        PrimaryKeyModel::ordered(&PK_FIELDS),
        0,
        &FIELDS,
        &[],
    );

    fn schema() -> &'static SchemaInfo {
        SchemaInfo::cached_for_generated_entity_model(&MODEL)
    }

    fn order(fields: &[&str]) -> OrderSpec {
        OrderSpec {
            fields: fields
                .iter()
                .map(|field| OrderTerm::field(*field, OrderDirection::Asc))
                .collect(),
        }
    }

    #[test]
    fn duplicate_order_validation_uses_composite_primary_key_names_from_schema() {
        validate_no_duplicate_non_pk_order_fields(
            schema(),
            &order(&["tenant_id", "tenant_id", "local_id", "local_id"]),
        )
        .expect("accepted primary-key fields are exempt from duplicate non-pk validation");

        let err = validate_no_duplicate_non_pk_order_fields(
            schema(),
            &order(&["tenant_id", "rank", "rank"]),
        )
        .expect_err("duplicate non-primary-key fields should still reject");

        assert!(matches!(
            err,
            PlanError::User(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::PlanUserError::Order(order)
                        if matches!(
                            order.as_ref(),
                            OrderPlanError::DuplicateOrderField { field } if field == "rank"
                        )
                )
        ));
    }

    #[test]
    fn tie_break_validation_uses_ordered_composite_primary_key_names_from_schema() {
        validate_primary_key_tie_break(schema(), &order(&["rank", "tenant_id", "local_id"]))
            .expect("ordered composite primary-key suffix should satisfy deterministic tie-break");

        validate_primary_key_tie_break(schema(), &order(&["rank", "local_id", "tenant_id"]))
            .expect_err("wrong composite primary-key suffix order should reject");
    }
}
