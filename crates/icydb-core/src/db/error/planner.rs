//! Module: db::error::planner
//!
//! Responsibility: planner-boundary constructor and plan-policy mapping helpers.
//! Does not own: cursor decode-classification mappings.
//! Boundary: logical-plan policy/invariant failures map through this module.

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
