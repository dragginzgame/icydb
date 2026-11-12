//! Module: db::executor::traversal
//! Owns executor-side traversal validation for lowered index-range specs and
//! missing-row policy handling.

//! Traversal helpers shared across executor load/delete paths.
//!
//! Continuation window arithmetic is cursor-owned (`db::cursor::continuation`).
//! Index-range runtime validation is executor-traversal-owned.
//! This module projects row-read consistency and range-spec invariants for executor I/O.

use crate::{
    db::{
        executor::LoweredIndexRangeSpec, predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

///
/// IndexRangeTraversalContract
///
/// IndexRangeTraversalContract owns executor-traversal invariants for
/// consuming and validating lowered index-range specs against executable
/// access paths.
///

pub(in crate::db::executor) struct IndexRangeTraversalContract;

impl IndexRangeTraversalContract {
    /// Require one index-range spec for index-range physical execution.
    pub(in crate::db::executor) fn require_spec(
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<&LoweredIndexRangeSpec, InternalError> {
        index_range_spec.ok_or_else(InternalError::query_executor_invariant)
    }
}

/// Derive row-read missing-row policy for one executor-consumed logical plan.
#[must_use]
pub(in crate::db::executor) fn row_read_consistency_for_plan(
    plan: &AccessPlannedQuery,
) -> MissingRowPolicy {
    plan.scalar_consistency()
}
