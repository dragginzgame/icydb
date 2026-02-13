use crate::db::query::{
    plan::{LogicalPlan, validate::PlanError},
    policy::{self, PlanPolicyError},
};

/// Validate plan-level invariants not covered by schema checks.
pub fn validate_plan_semantics<K>(plan: &LogicalPlan<K>) -> Result<(), PlanError> {
    policy::validate_plan_shape(plan).map_err(|err| match err {
        PlanPolicyError::EmptyOrderSpec => PlanError::EmptyOrderSpec,
        PlanPolicyError::DeletePlanWithPagination => PlanError::DeletePlanWithPagination,
        PlanPolicyError::LoadPlanWithDeleteLimit => PlanError::LoadPlanWithDeleteLimit,
        PlanPolicyError::DeleteLimitRequiresOrder => PlanError::DeleteLimitRequiresOrder,
        PlanPolicyError::UnorderedPagination => PlanError::UnorderedPagination,
    })
}
