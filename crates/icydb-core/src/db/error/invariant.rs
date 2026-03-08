//! Module: db::error::invariant
//! Responsibility: shared invariant constructors for db runtime modules.
//! Does not own: error class/origin taxonomy policy.
//! Boundary: inline helpers that preserve canonical invariant formatting.

use crate::error::InternalError;

/// Construct the canonical query-executor invariant error.
pub(in crate::db) fn executor_invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

/// Construct the canonical cursor invariant error with executor-prefix formatting.
pub(in crate::db) fn cursor_invariant(message: impl Into<String>) -> InternalError {
    InternalError::cursor_invariant(InternalError::executor_invariant_message(message))
}

/// Construct the canonical planner invariant error with executor-prefix formatting.
pub(in crate::db) fn planner_invariant(message: impl Into<String>) -> InternalError {
    InternalError::planner_invariant(InternalError::executor_invariant_message(message))
}
