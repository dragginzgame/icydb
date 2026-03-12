//! Module: db::query::plan::validate::order
//! Responsibility: module-local ownership and contracts for db::query::plan::validate::order.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        query::plan::{
            OrderSpec,
            validate::{OrderPlanError, PlanError},
        },
        schema::SchemaInfo,
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

        // CONTRACT: ORDER BY rejects non-queryable or unordered fields.
        field_type.is_orderable().then_some(()).ok_or_else(|| {
            PlanError::from(OrderPlanError::UnorderableField {
                field: field.clone(),
            })
        })?;
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
        let non_pk_field = field != pk_field;
        if !non_pk_field {
            continue;
        }
        seen.insert(field.as_str()).then_some(()).ok_or_else(|| {
            PlanError::from(OrderPlanError::DuplicateOrderField {
                field: field.clone(),
            })
        })?;
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
                    PlanError::from(OrderPlanError::MissingPrimaryKeyTieBreak {
                        field: pk_field.to_string(),
                    })
                })
        },
        |()| Ok(()),
    )
}
