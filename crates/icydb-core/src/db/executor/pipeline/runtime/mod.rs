//! Module: executor::pipeline::runtime
//! Responsibility: key-stream resolution and fast-path/fallback execution dispatch.
//! Does not own: cursor decoding policy or logical-plan construction.
//! Boundary: execution-attempt internals used by pipeline/load orchestration.

mod adapter;
mod attempt;
mod fast_path;
mod retained_slots;

#[cfg(test)]
use crate::db::executor::planning::route::ensure_load_fast_path_spec_arity;
use crate::{
    db::executor::{
        ExecutionTrace, aggregate::runtime::finalize_path_outcome_for_path,
        pipeline::contracts::ExecutionOutcomeMetrics, pipeline::contracts::StructuralCursorPage,
    },
    metrics::sink::{ExecKind, PathSpan},
};

pub(in crate::db::executor) use adapter::{
    ExecutionMaterializationContract, ExecutionRuntimeAdapter,
};
pub(in crate::db::executor) use attempt::ExecutionAttemptKernel;
pub(in crate::db::executor) use retained_slots::compile_retained_slot_layout_for_mode;

/// Finalize one structural scalar page before typed or structural surface projection.
pub(in crate::db::executor) fn finalize_structural_page_for_path(
    entity_path: &'static str,
    page: StructuralCursorPage,
    metrics: ExecutionOutcomeMetrics,
    execution_trace: &mut Option<ExecutionTrace>,
    execution_time_micros: u64,
) -> StructuralCursorPage {
    let rows_emitted = page.row_count();

    finalize_path_outcome_for_path(
        entity_path,
        execution_trace,
        metrics,
        rows_emitted,
        false,
        execution_time_micros,
    );
    let mut span = PathSpan::new(ExecKind::Load, entity_path);
    span.set_rows(u64::try_from(page.row_count()).unwrap_or(u64::MAX));

    page
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::error::ErrorClass;

    #[test]
    fn fast_path_spec_arity_accepts_single_spec_shapes() {
        let result = super::ensure_load_fast_path_spec_arity(true, 1, true, 1);

        assert!(result.is_ok(), "single fast-path specs should be accepted");
    }

    #[test]
    fn fast_path_spec_arity_rejects_multiple_prefix_specs_for_secondary() {
        let err = super::ensure_load_fast_path_spec_arity(true, 2, false, 0)
            .expect_err("secondary fast-path must reject multiple index-prefix specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "prefix-spec arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("secondary fast-path resolution expects at most one index-prefix spec"),
            "prefix-spec arity violation must return a clear invariant message"
        );
    }

    #[test]
    fn fast_path_spec_arity_rejects_multiple_range_specs_for_index_range() {
        let err = super::ensure_load_fast_path_spec_arity(false, 0, true, 2)
            .expect_err("index-range fast-path must reject multiple index-range specs");

        assert_eq!(
            err.class,
            ErrorClass::InvariantViolation,
            "range-spec arity violation must classify as invariant violation"
        );
        assert!(
            err.message
                .contains("index-range fast-path resolution expects at most one index-range spec"),
            "range-spec arity violation must return a clear invariant message"
        );
    }
}
