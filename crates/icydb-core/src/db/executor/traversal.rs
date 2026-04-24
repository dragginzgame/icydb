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
        executor::{ExecutionPathPayload, LoweredIndexRangeSpec},
        predicate::MissingRowPolicy,
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
    /// Validate that one consumed index-range spec is aligned with one index-range path node.
    pub(in crate::db::executor) fn validate_spec_alignment<K>(
        path: &ExecutionPathPayload<'_, K>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<(), InternalError> {
        let path_capabilities = path.capabilities();
        if let Some(spec) = index_range_spec
            && let Some(index) = path_capabilities.index_range_model()
            && spec.index() != &index
        {
            return Err(InternalError::query_executor_invariant(
                "index-range spec does not match access path index",
            ));
        }

        Ok(())
    }

    /// Require one index-range spec for index-range physical execution.
    pub(in crate::db::executor) fn require_spec(
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<&LoweredIndexRangeSpec, InternalError> {
        index_range_spec.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "index-range execution requires pre-lowered index-range spec",
            )
        })
    }

    /// Validate that index-range lowered specs were fully consumed during traversal.
    pub(in crate::db::executor) fn validate_specs_consumed(
        consumed: usize,
        available: usize,
    ) -> Result<(), InternalError> {
        if consumed < available {
            return Err(InternalError::query_executor_invariant(
                "unused index-range executable specs after access-plan traversal",
            ));
        }

        Ok(())
    }
}

/// Derive row-read missing-row policy for one executor-consumed logical plan.
#[must_use]
pub(in crate::db::executor) const fn row_read_consistency_for_plan(
    plan: &AccessPlannedQuery,
) -> MissingRowPolicy {
    plan.scalar_plan().consistency
}
