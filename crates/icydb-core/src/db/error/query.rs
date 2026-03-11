//! Module: db::error::query
//!
//! Responsibility: query-boundary invariant constructor helpers.
//! Does not own: planner/cursor/executor conversion mappings.
//! Boundary: query-layer runtime invariants map into `InternalError` taxonomy here.

use crate::error::{ErrorClass, ErrorOrigin, InternalError};

/// Construct a query-origin invariant violation.
pub(crate) fn query_invariant(message: impl Into<String>) -> InternalError {
    InternalError::classified(
        ErrorClass::InvariantViolation,
        ErrorOrigin::Query,
        message.into(),
    )
}

/// Construct a query-origin invariant with the canonical executor prefix.
pub(crate) fn query_executor_invariant(reason: impl Into<String>) -> InternalError {
    query_invariant(crate::db::error::executor_invariant_message(reason))
}
