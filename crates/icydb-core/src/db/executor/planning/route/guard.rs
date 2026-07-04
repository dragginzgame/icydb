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
    fn ensure_spec_exactly_one_if_enabled(
        fast_path_enabled: bool,
        spec_count: usize,
    ) -> Result<(), InternalError> {
        (!(fast_path_enabled && spec_count != 1))
            .then_some(())
            .ok_or_else(Self::invariant)
    }
}

/// Guard secondary aggregate fast-path assumptions.
///
/// This keeps index-prefix spec consumption from silently drifting if planner
/// shapes evolve.
pub(in crate::db::executor) fn ensure_secondary_aggregate_fast_path_arity(
    secondary_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
) -> Result<(), InternalError> {
    RouteFastPathSpecContract::ensure_spec_at_most_one_if_enabled(
        secondary_pushdown_eligible,
        index_prefix_spec_count,
    )
}

/// Guard index-range aggregate fast-path assumptions.
///
/// This keeps contract/runtime spec boundaries explicit and drift-resistant.
pub(in crate::db::executor) fn ensure_index_range_aggregate_fast_path_specs(
    index_range_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    index_range_pushdown_eligible.then_some(()).map_or_else(
        || Ok(()),
        |()| {
            (index_prefix_spec_count == 0)
                .then_some(())
                .ok_or_else(RouteFastPathSpecContract::invariant)?;
            RouteFastPathSpecContract::ensure_spec_exactly_one_if_enabled(
                true,
                index_range_spec_count,
            )?;

            Ok(())
        },
    )
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::error::ErrorClass;

    #[test]
    fn secondary_aggregate_fast_path_arity_accepts_single_prefix_spec() {
        let result = super::ensure_secondary_aggregate_fast_path_arity(true, 1);

        assert!(
            result.is_ok(),
            "single secondary prefix spec should be accepted"
        );
    }

    #[test]
    fn secondary_aggregate_fast_path_arity_rejects_multiple_prefix_specs() {
        let err = super::ensure_secondary_aggregate_fast_path_arity(true, 2)
            .expect_err("secondary aggregate fast-path must reject multiple prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "arity violation must classify as invariant violation"
        );
        assert_eq!(
            err.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
            "arity violation must return the invariant diagnostic code"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_accept_exact_arity() {
        let result = super::ensure_index_range_aggregate_fast_path_specs(true, 0, 1);

        assert!(
            result.is_ok(),
            "index-range aggregate fast-path should accept one range spec and no prefix specs"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_reject_prefix_spec_presence() {
        let err = super::ensure_index_range_aggregate_fast_path_specs(true, 1, 1)
            .expect_err("index-range aggregate fast-path must reject prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "prefix-spec violation must classify as invariant violation"
        );
        assert_eq!(
            err.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
            "prefix-spec violation must return the invariant diagnostic code"
        );
    }

    #[test]
    fn index_range_aggregate_fast_path_specs_reject_non_exact_range_arity() {
        let err = super::ensure_index_range_aggregate_fast_path_specs(true, 0, 2)
            .expect_err("index-range aggregate fast-path must reject non-exact range arity");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "range-arity violation must classify as invariant violation"
        );
        assert_eq!(
            err.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
            "range-arity violation must return the invariant diagnostic code"
        );
    }
}
