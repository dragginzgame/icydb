use crate::{
    db::{
        predicate::SchemaInfo,
        query::plan::{
            OrderSpec,
            validate::{OrderPlanError, PlanError},
        },
    },
    model::entity::EntityModel,
};
use std::collections::BTreeSet;

/// Validate ORDER BY fields against the schema.
pub(crate) fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        let field_type = schema
            .field(field)
            .ok_or_else(|| OrderPlanError::UnknownField {
                field: field.clone(),
            })
            .map_err(PlanError::from)?;

        if !field_type.is_orderable() {
            // CONTRACT: ORDER BY rejects non-queryable or unordered fields.
            return Err(PlanError::from(OrderPlanError::UnorderableField {
                field: field.clone(),
            }));
        }
    }

    Ok(())
}

/// Reject duplicate non-primary-key fields in ORDER BY.
pub(crate) fn validate_no_duplicate_non_pk_order_fields(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    let mut seen = BTreeSet::new();
    let pk_field = model.primary_key.name;

    for (field, _) in &order.fields {
        if field == pk_field {
            continue;
        }
        if !seen.insert(field.as_str()) {
            return Err(PlanError::from(OrderPlanError::DuplicateOrderField {
                field: field.clone(),
            }));
        }
    }

    Ok(())
}

// Ordered plans must include exactly one terminal primary-key field so ordering is total and
// deterministic across explain, fingerprint, and executor comparison paths.
pub(crate) fn validate_primary_key_tie_break(
    model: &EntityModel,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    if order.fields.is_empty() {
        return Ok(());
    }

    let pk_field = model.primary_key.name;
    let pk_count = order
        .fields
        .iter()
        .filter(|(field, _)| field == pk_field)
        .count();
    let trailing_pk = order
        .fields
        .last()
        .is_some_and(|(field, _)| field == pk_field);

    if pk_count == 1 && trailing_pk {
        Ok(())
    } else {
        Err(PlanError::from(OrderPlanError::MissingPrimaryKeyTieBreak {
            field: pk_field.to_string(),
        }))
    }
}
