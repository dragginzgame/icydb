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
