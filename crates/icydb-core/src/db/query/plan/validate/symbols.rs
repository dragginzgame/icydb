//! Module: db::query::plan::validate::symbols
//! Responsibility: validate user-facing field and symbol references against
//! the model and grouped/query projection surfaces.
//! Does not own: ordering, cursor, or grouped policy enforcement outside symbol lookup.
//! Boundary: keeps symbol-resolution failures localized within query-plan validation.

use crate::{
    db::query::{
        intent::QueryError,
        plan::{
            FieldSlot,
            validate::{GroupPlanError, PlanError},
        },
    },
    db::schema::{FieldType, SchemaInfo},
    model::entity::EntityModel,
};

/// Resolve one grouped field into a stable field slot.
pub(in crate::db) fn resolve_group_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve(model, field)
        .ok_or_else(|| PlanError::from(GroupPlanError::unknown_group_field(field)))
}

/// Resolve one grouped field through schema slot authority.
pub(in crate::db) fn resolve_group_field_slot_with_schema(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    let index = schema
        .field_slot_index(field)
        .ok_or_else(|| PlanError::from(GroupPlanError::unknown_group_field(field)))?;
    let model_field = model
        .fields()
        .iter()
        .find(|model_field| model_field.name() == field)
        .ok_or_else(|| PlanError::from(GroupPlanError::unknown_group_field(field)))?;

    Ok(FieldSlot {
        index,
        field: model_field.name().to_string(),
        kind: Some(model_field.kind()),
    })
}

/// Resolve one aggregate target field through schema slot authority.
///
/// Accepted-schema planning paths use this helper so the physical slot comes
/// from the selected `SchemaInfo`; the generated model is retained only for
/// field labels and type metadata used by diagnostics and explain surfaces.
pub(in crate::db) fn resolve_aggregate_target_field_slot_with_schema(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    let index = schema
        .field_slot_index(field)
        .ok_or_else(|| QueryError::unknown_aggregate_target_field(field))?;
    let model_field = model
        .fields()
        .iter()
        .find(|model_field| model_field.name() == field)
        .ok_or_else(|| QueryError::unknown_aggregate_target_field(field))?;

    Ok(FieldSlot {
        index,
        field: model_field.name().to_string(),
        kind: Some(model_field.kind()),
    })
}

/// Resolve one grouped aggregate target field into one schema field type.
pub(in crate::db::query::plan::validate) fn resolve_group_aggregate_target_field_type<'a>(
    schema: &'a SchemaInfo,
    field: &str,
    index: usize,
) -> Result<&'a FieldType, GroupPlanError> {
    schema
        .field(field)
        .ok_or_else(|| GroupPlanError::unknown_aggregate_target_field(index, field))
}
