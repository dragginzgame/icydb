//! Module: db::executor::plan_validate
//! Responsibility: defensive structural validation at executor entry boundaries.
//! Does not own: logical/user-shape query semantics.
//! Boundary: catches internal planner/executor contract mismatches early.

use crate::{
    db::{executor::EntityAuthority, query::plan::AccessPlannedQuery},
    error::InternalError,
};

/// Validate plans at executor boundaries using structural entity authority.
pub(in crate::db::executor) fn validate_executor_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    authority.validate_executor_plan(plan)
}
