//! Module: db::query::plan::validate::symbols
//! Responsibility: module-local ownership and contracts for db::query::plan::validate::symbols.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
pub(crate) fn resolve_group_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve(model, field).ok_or_else(|| {
        PlanError::from(GroupPlanError::UnknownGroupField {
            field: field.to_string(),
        })
    })
}

/// Resolve one aggregate target field into a stable field slot.
pub(crate) fn resolve_aggregate_target_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    FieldSlot::resolve(model, field).ok_or_else(|| {
        QueryError::unsupported_query(format!("unknown aggregate target field: {field}"))
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
        .ok_or_else(|| GroupPlanError::UnknownAggregateTargetField {
            index,
            field: field.to_string(),
        })
}
