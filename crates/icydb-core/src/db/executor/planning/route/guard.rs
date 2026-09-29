//! Module: executor::planning::route::guard
//! Responsibility: invariant guards for route fast-path lowered-spec arity contracts.
//! Does not own: route decision policy.
//! Boundary: fail-closed internal validation at route/runtime handoff.

use crate::error::InternalError;

/// Enforce at most one lowered range spec when its load fast path is enabled.
pub(in crate::db::executor) fn ensure_index_range_fast_path_spec_arity(
    index_range_pushdown_eligible: bool,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    (!(index_range_pushdown_eligible && index_range_spec_count > 1))
        .then_some(())
        .ok_or_else(InternalError::query_executor_invariant)
}
