//! Module: db::executor::traversal
//! Responsibility: module-local ownership and contracts for db::executor::traversal.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

//! Traversal helpers shared across executor load/delete paths.
//!
//! Continuation window arithmetic is cursor-owned (`db::cursor::continuation`).
//! Index-range runtime validation is executor-traversal-owned.
//! This module projects row-read consistency and range-spec invariants for executor I/O.

use crate::{
    db::{
        executor::{ExecutableAccessPath, LoweredIndexRangeSpec},
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

/// Derive row-read missing-row policy for one executor-consumed logical plan.
#[must_use]
pub(in crate::db::executor) const fn row_read_consistency_for_plan(
    plan: &AccessPlannedQuery,
) -> MissingRowPolicy {
    plan.scalar_plan().consistency
}

/// Validate that one consumed index-range spec is aligned with one index-range path node.
pub(in crate::db::executor) fn validate_index_range_spec_alignment<K>(
    path: &ExecutableAccessPath<'_, K>,
    index_range_spec: Option<&LoweredIndexRangeSpec>,
) -> Result<(), InternalError> {
    let path_capabilities = path.capabilities();
    if let Some(spec) = index_range_spec
        && let Some(index) = path_capabilities.index_range_model()
        && spec.index() != &index
    {
        return Err(crate::db::error::query_executor_invariant(
            "index-range spec does not match access path index",
        ));
    }

    Ok(())
}

/// Require one index-range spec for index-range physical execution.
pub(in crate::db::executor) fn require_index_range_spec(
    index_range_spec: Option<&LoweredIndexRangeSpec>,
) -> Result<&LoweredIndexRangeSpec, InternalError> {
    index_range_spec.ok_or_else(|| {
        crate::db::error::query_executor_invariant(
            "index-range execution requires pre-lowered index-range spec",
        )
    })
}

/// Validate that index-range lowered specs were fully consumed during traversal.
pub(in crate::db::executor) fn validate_index_range_specs_consumed(
    consumed: usize,
    available: usize,
) -> Result<(), InternalError> {
    if consumed < available {
        return Err(crate::db::error::query_executor_invariant(
            "unused index-range executable specs after access-plan traversal",
        ));
    }

    Ok(())
}
