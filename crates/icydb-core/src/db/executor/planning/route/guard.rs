//! Module: executor::planning::route::guard
//! Responsibility: invariant guards for route fast-path lowered-spec arity contracts.
//! Does not own: route decision policy.
//! Boundary: fail-closed internal validation at route/runtime handoff.

use crate::error::InternalError;

///
/// RouteFastPathSpecContract
///
/// Route-owned invariant contract for lowered spec arity at fast-path seams.
/// This keeps route/runtime handoff failures under one owner instead of
/// rebuilding internal invariant construction in each guard helper.
///

struct RouteFastPathSpecContract;

impl RouteFastPathSpecContract {
    // Build one route invariant for invalid lowered-spec arity.
    fn invariant() -> InternalError {
        InternalError::query_executor_invariant()
    }

    // Enforce that a fast path consumes at most one lowered spec when enabled.
    fn ensure_spec_at_most_one_if_enabled(
        fast_path_enabled: bool,
        spec_count: usize,
    ) -> Result<(), InternalError> {
        (!(fast_path_enabled && spec_count > 1))
            .then_some(())
            .ok_or_else(Self::invariant)
    }

    // Enforce that a fast path consumes exactly one lowered spec when enabled.
}

/// Guard load fast-path assumptions.
///
/// This keeps contract/runtime spec boundaries explicit and drift-resistant as
/// new fast paths are introduced.
pub(in crate::db::executor) fn ensure_load_fast_path_spec_arity(
    _secondary_pushdown_eligible: bool,
    _index_prefix_spec_count: usize,
    index_range_pushdown_eligible: bool,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    RouteFastPathSpecContract::ensure_spec_at_most_one_if_enabled(
        index_range_pushdown_eligible,
        index_range_spec_count,
    )?;

    Ok(())
}
