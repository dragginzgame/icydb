//! Module: db::error::planner
//!
//! Responsibility: planner-boundary constructor and plan-policy mapping helpers.
//! Does not own: cursor decode-classification mappings.
//! Boundary: logical-plan policy/invariant failures map through this module.

#[cfg(test)]
use crate::db::query::plan::{PlanError, PlanPolicyError, PlanUserError, PolicyPlanError};
use crate::error::{ErrorClass, ErrorOrigin, InternalError};

/// Construct a planner-origin invariant violation.
pub(crate) fn planner_invariant(message: impl Into<String>) -> InternalError {
    InternalError::classified(
        ErrorClass::InvariantViolation,
        ErrorOrigin::Planner,
        message.into(),
    )
}

/// Build the canonical invalid-logical-plan message prefix.
#[must_use]
pub(crate) fn invalid_logical_plan_message(reason: impl Into<String>) -> String {
    format!("invalid logical plan: {}", reason.into())
}

/// Construct a planner-origin invariant with the canonical invalid-plan prefix.
pub(crate) fn query_invalid_logical_plan(reason: impl Into<String>) -> InternalError {
    planner_invariant(invalid_logical_plan_message(reason))
}

/// Map grouped plan failures into query-boundary invariants.
#[cfg(test)]
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

    planner_invariant(message)
}

/// Map plan-shape policy variants into executor-boundary invariants without
/// string-based conversion paths.
#[cfg(test)]
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

    planner_invariant(crate::db::error::executor_invariant_message(reason))
}
