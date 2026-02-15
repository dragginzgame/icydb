use crate::db::query::{
    plan::{LogicalPlan, validate::PlanError},
    policy,
};

/// Validate plan-level invariants not covered by schema checks.
pub fn validate_plan_semantics<K>(plan: &LogicalPlan<K>) -> Result<(), PlanError> {
    policy::validate_plan_shape(plan).map_err(PlanError::from)
}
