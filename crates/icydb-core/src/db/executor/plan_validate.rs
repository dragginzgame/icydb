//! Module: db::executor::plan_validate
//! Responsibility: defensive structural validation at executor entry boundaries.
//! Does not own: logical/user-shape query semantics.
//! Boundary: catches internal planner/executor contract mismatches early.

use crate::{
    db::{
        access::validate_access_structure_model, query::plan::AccessPlannedQuery,
        schema::SchemaInfo,
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
    let schema = SchemaInfo::from_entity_model(E::MODEL).map_err(|err| {
        crate::db::error::query_invariant(format!("entity schema invalid for {}: {err}", E::PATH))
    })?;

    validate_access_structure_model(&schema, E::MODEL, &plan.access)
        .map_err(crate::db::error::from_executor_access_plan_error)?;

    Ok(())
}
