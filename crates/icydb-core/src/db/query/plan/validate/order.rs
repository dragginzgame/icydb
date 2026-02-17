use crate::{
    db::query::{
        plan::{OrderSpec, validate::PlanError},
        predicate::SchemaInfo,
    },
    model::entity::EntityModel,
};

/// Validate ORDER BY fields against the schema.
pub(crate) fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        let field_type = schema
            .field(field)
            .ok_or_else(|| PlanError::UnknownOrderField {
                field: field.clone(),
            })?;

        if !field_type.is_orderable() {
            // CONTRACT: ORDER BY rejects non-queryable or unordered fields.
            return Err(PlanError::UnorderableField {
                field: field.clone(),
            });
        }
    }

    Ok(())
}

/// Validate ORDER BY fields for executor-only plans.
///
/// CONTRACT: executor ordering validation matches planner rules.
pub(super) fn validate_executor_order(
    schema: &SchemaInfo,
    order: &OrderSpec,
) -> Result<(), PlanError> {
    validate_order(schema, order)
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
        Err(PlanError::MissingPrimaryKeyTieBreak {
            field: pk_field.to_string(),
        })
    }
}
