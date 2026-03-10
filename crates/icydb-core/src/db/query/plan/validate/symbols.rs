//! Module: db::query::plan::validate::symbols
//! Responsibility: module-local ownership and contracts for db::query::plan::validate::symbols.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::query::plan::{
        FieldSlot,
        validate::{GroupPlanError, PlanError},
    },
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
