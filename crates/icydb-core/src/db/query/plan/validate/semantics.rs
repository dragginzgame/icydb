use crate::db::{
    policy,
    query::plan::{LogicalPlan, validate::PlanError},
};

/// Validate plan-level invariants not covered by schema checks.
pub(super) fn validate_plan_semantics(plan: &LogicalPlan) -> Result<(), PlanError> {
    policy::validate_plan_shape(plan).map_err(PlanError::from)
}
