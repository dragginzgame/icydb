//! Module: db::error::executor
//!
//! Responsibility: executor-boundary constructor and message helpers.
//! Does not own: planner/cursor policy mapping.
//! Boundary: executor-domain failures are normalized here.

use crate::error::{ErrorClass, ErrorOrigin, InternalError};

/// Construct an executor-origin invariant violation.
pub(crate) fn executor_invariant(message: impl Into<String>) -> InternalError {
    InternalError::classified(
        ErrorClass::InvariantViolation,
        ErrorOrigin::Executor,
        message.into(),
    )
}

/// Construct an executor-origin internal error.
pub(crate) fn executor_internal(message: impl Into<String>) -> InternalError {
    InternalError::classified(ErrorClass::Internal, ErrorOrigin::Executor, message.into())
}

/// Construct an executor-origin unsupported error.
pub(crate) fn executor_unsupported(message: impl Into<String>) -> InternalError {
    InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Executor,
        message.into(),
    )
}

/// Build the canonical executor-invariant message prefix.
#[must_use]
pub(crate) fn executor_invariant_message(reason: impl Into<String>) -> String {
    format!("executor invariant violated: {}", reason.into())
}
