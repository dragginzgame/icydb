use crate::db::query::{
    plan::{LogicalPlan, validate::PlanError},
    policy,
};

/// Validate plan-level invariants not covered by schema checks.
pub(super) fn validate_plan_semantics(plan: &LogicalPlan) -> Result<(), PlanError> {
    policy::validate_plan_shape(plan).map_err(PlanError::from)
}
