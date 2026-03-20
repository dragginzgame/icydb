//! Module: db::executor::plan_validate
//! Responsibility: defensive structural validation at executor entry boundaries.
//! Does not own: logical/user-shape query semantics.
//! Boundary: catches internal planner/executor contract mismatches early.

use crate::{
    db::{
        access::validate_access_structure_model, executor::EntityAuthority,
        query::plan::AccessPlannedQuery, schema::SchemaInfo,
    },
    error::InternalError,
    traits::EntityKind,
};

/// Validate plans at executor boundaries and surface invariant violations.
///
/// Ownership:
/// - defensive execution-boundary guardrail, not a semantic owner
/// - must enforce structural integrity only, never user-shape semantics
///
/// Any disagreement with logical validation indicates an internal bug and is not
/// a recoverable user-input condition.
pub(in crate::db::executor) fn validate_executor_plan<E: EntityKind>(
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    validate_executor_plan_for_authority(EntityAuthority::for_type::<E>(), plan)
}

/// Validate plans at executor boundaries using structural entity authority.
pub(in crate::db::executor) fn validate_executor_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    let entity_path = authority.entity_path();
    let schema = SchemaInfo::from_entity_model(authority.model()).map_err(|err| {
        crate::db::error::query_invariant(format!("entity schema invalid for {entity_path}: {err}"))
    })?;

    validate_access_structure_model(&schema, authority.model(), &plan.access)
        .map_err(crate::db::error::from_executor_access_plan_error)?;

    Ok(())
}
