use crate::{
    db::error::planner::invalid_logical_plan_message,
    db::query::plan::{PlanError, PlanPolicyError, PlanUserError, PolicyPlanError},
    error::InternalError,
};

/// Map grouped plan failures into query-boundary invariants for tests.
pub(crate) fn from_group_plan_error(err: PlanError) -> InternalError {
    let message = match err {
        PlanError::User(inner) => match *inner {
            PlanUserError::Group(inner) => invalid_logical_plan_message(inner.to_string()),
            other => {
                format!("group-plan error conversion received non-group user variant: {other}")
            }
        },
        PlanError::Policy(inner) => match *inner {
            PlanPolicyError::Group(inner) => invalid_logical_plan_message(inner.to_string()),
            PlanPolicyError::Policy(inner) => {
                format!("group-plan error conversion received non-group policy variant: {inner}")
            }
        },
        PlanError::Cursor(inner) => {
            format!("group-plan error conversion received cursor variant: {inner}")
        }
    };

    super::planner_invariant(message)
}

/// Map plan-shape policy variants into executor-boundary invariants for tests.
pub(crate) fn plan_invariant_violation(err: PolicyPlanError) -> InternalError {
    let reason = match err {
        PolicyPlanError::EmptyOrderSpec => "order specification must include at least one field",
        PolicyPlanError::DeletePlanWithOffset => "delete plans must not include OFFSET",
        PolicyPlanError::DeletePlanWithGrouping => {
            "delete plans must not include GROUP BY or HAVING"
        }
        PolicyPlanError::DeletePlanWithPagination => "delete plans must not include pagination",
        PolicyPlanError::LoadPlanWithDeleteLimit => "load plans must not carry delete limits",
        PolicyPlanError::DeleteLimitRequiresOrder => "delete limit requires explicit ordering",
        PolicyPlanError::UnorderedPagination => "pagination requires explicit ordering",
    };

    super::planner_invariant(crate::db::error::executor_invariant_message(reason))
}
