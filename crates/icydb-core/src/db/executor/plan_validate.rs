//! Module: db::executor::plan_validate
//! Responsibility: defensive structural validation at executor entry boundaries.
//! Does not own: logical/user-shape query semantics.
//! Boundary: catches internal planner/executor contract mismatches early.

use crate::{
    db::{
        access::{AccessPlanError, validate_access_structure_model},
        executor::EntityAuthority,
        query::plan::AccessPlannedQuery,
        schema::SchemaInfo,
    },
    error::InternalError,
};

// Load canonical executor-side schema info once for structural plan validation.
fn executor_plan_schema(authority: EntityAuthority) -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(authority.model())
}

/// Validate plans at executor boundaries using structural entity authority.
pub(in crate::db::executor) fn validate_executor_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    let schema = executor_plan_schema(authority);

    validate_access_structure_model(&schema, authority.model(), &plan.access)
        .map_err(AccessPlanError::into_internal_error)?;

    Ok(())
}
