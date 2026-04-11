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
pub(crate) fn resolve_group_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, PlanError> {
    FieldSlot::resolve(model, field)
        .ok_or_else(|| PlanError::from(GroupPlanError::unknown_group_field(field)))
}

/// Resolve one aggregate target field into a stable field slot.
pub(crate) fn resolve_aggregate_target_field_slot(
    model: &EntityModel,
    field: &str,
) -> Result<FieldSlot, QueryError> {
    FieldSlot::resolve(model, field)
        .ok_or_else(|| QueryError::unknown_aggregate_target_field(field))
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
