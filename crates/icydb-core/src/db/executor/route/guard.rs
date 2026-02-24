use crate::error::InternalError;

const SECONDARY_AGGREGATE_PREFIX_ARITY_MESSAGE: &str =
    "secondary aggregate fast-path expects at most one index-prefix spec";
const INDEX_RANGE_AGGREGATE_NO_PREFIX_MESSAGE: &str =
    "index-range aggregate fast-path must not consume index-prefix specs";
const INDEX_RANGE_AGGREGATE_EXACT_RANGE_MESSAGE: &str =
    "index-range aggregate fast-path expects exactly one index-range spec";
const SECONDARY_LOAD_PREFIX_ARITY_MESSAGE: &str =
    "secondary fast-path resolution expects at most one index-prefix spec";
const INDEX_RANGE_LOAD_RANGE_ARITY_MESSAGE: &str =
    "index-range fast-path resolution expects at most one index-range spec";

// Shared arity guard: enforce at most one lowered spec when a fast path is enabled.
fn ensure_spec_at_most_one_if_enabled(
    fast_path_enabled: bool,
    spec_count: usize,
    message: &'static str,
) -> Result<(), InternalError> {
    if fast_path_enabled && spec_count > 1 {
        return Err(InternalError::query_executor_invariant(message));
    }

    Ok(())
}

// Shared arity guard: enforce exactly one lowered spec when a fast path is enabled.
fn ensure_spec_exactly_one_if_enabled(
    fast_path_enabled: bool,
    spec_count: usize,
    message: &'static str,
) -> Result<(), InternalError> {
    if fast_path_enabled && spec_count != 1 {
        return Err(InternalError::query_executor_invariant(message));
    }

    Ok(())
}

// Shared helper for prefix-spec arity checks used by load and aggregate routes.
pub(in crate::db::executor) fn ensure_prefix_spec_at_most_one_if_enabled(
    fast_path_enabled: bool,
    index_prefix_spec_count: usize,
    message: &'static str,
) -> Result<(), InternalError> {
    ensure_spec_at_most_one_if_enabled(fast_path_enabled, index_prefix_spec_count, message)
}

// Shared helper for range-spec arity checks used by load and aggregate routes.
pub(in crate::db::executor) fn ensure_range_spec_at_most_one_if_enabled(
    fast_path_enabled: bool,
    index_range_spec_count: usize,
    message: &'static str,
) -> Result<(), InternalError> {
    ensure_spec_at_most_one_if_enabled(fast_path_enabled, index_range_spec_count, message)
}

// Guard secondary aggregate fast-path assumptions so index-prefix
// spec consumption cannot silently drift if planner shapes evolve.
pub(in crate::db::executor) fn ensure_secondary_aggregate_fast_path_arity(
    secondary_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
) -> Result<(), InternalError> {
    ensure_prefix_spec_at_most_one_if_enabled(
        secondary_pushdown_eligible,
        index_prefix_spec_count,
        SECONDARY_AGGREGATE_PREFIX_ARITY_MESSAGE,
    )
}

// Guard index-range aggregate fast-path assumptions so planner/executor
// spec boundaries remain explicit and drift-resistant.
pub(in crate::db::executor) fn ensure_index_range_aggregate_fast_path_specs(
    index_range_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    if !index_range_pushdown_eligible {
        return Ok(());
    }

    if index_prefix_spec_count != 0 {
        return Err(InternalError::query_executor_invariant(
            INDEX_RANGE_AGGREGATE_NO_PREFIX_MESSAGE,
        ));
    }
    ensure_spec_exactly_one_if_enabled(
        true,
        index_range_spec_count,
        INDEX_RANGE_AGGREGATE_EXACT_RANGE_MESSAGE,
    )?;

    Ok(())
}

// Guard load fast-path assumptions so planner/executor spec boundaries remain
// explicit and drift-resistant as new fast paths are introduced.
pub(in crate::db::executor) fn ensure_load_fast_path_spec_arity(
    secondary_pushdown_eligible: bool,
    index_prefix_spec_count: usize,
    index_range_pushdown_eligible: bool,
    index_range_spec_count: usize,
) -> Result<(), InternalError> {
    ensure_prefix_spec_at_most_one_if_enabled(
        secondary_pushdown_eligible,
        index_prefix_spec_count,
        SECONDARY_LOAD_PREFIX_ARITY_MESSAGE,
    )?;
    ensure_range_spec_at_most_one_if_enabled(
        index_range_pushdown_eligible,
        index_range_spec_count,
        INDEX_RANGE_LOAD_RANGE_ARITY_MESSAGE,
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
        assert!(
            err.message
                .contains("secondary aggregate fast-path expects at most one index-prefix spec"),
            "arity violation must return a clear invariant message"
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
        assert!(
            err.message
                .contains("index-range aggregate fast-path must not consume index-prefix specs"),
            "prefix-spec violation must return a clear invariant message"
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
        assert!(
            err.message
                .contains("index-range aggregate fast-path expects exactly one index-range spec"),
            "range-arity violation must return a clear invariant message"
        );
    }
}
