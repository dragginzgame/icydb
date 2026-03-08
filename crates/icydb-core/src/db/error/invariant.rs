//! Module: db::error::invariant
//! Responsibility: shared invariant constructors for db runtime modules.
//! Does not own: error class/origin taxonomy policy.
//! Boundary: inline helpers that preserve canonical invariant formatting.

use crate::error::InternalError;

/// Construct the canonical query-executor invariant error.
pub(in crate::db) fn executor_invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
