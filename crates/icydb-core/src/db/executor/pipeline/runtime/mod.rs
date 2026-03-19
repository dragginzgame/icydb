//! Module: executor::pipeline::runtime
//! Responsibility: key-stream resolution and fast-path/fallback execution dispatch.
//! Does not own: cursor decoding policy or logical-plan construction.
//! Boundary: execution-attempt internals used by pipeline/load orchestration.

mod fast_path;

#[cfg(test)]
use crate::db::executor::route::ensure_load_fast_path_spec_arity;
use crate::{
    db::executor::{
        ExecutionTrace,
        pipeline::contracts::ExecutionOutcomeMetrics,
        pipeline::contracts::{LoadExecutor, StructuralCursorPage},
        plan_metrics::set_rows_from_len,
    },
    db::response::EntityResponse,
    metrics::sink::Span,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Finalize one rows-only execution attempt without reconstructing page state.
    pub(in crate::db::executor) fn finalize_entity_response(
        response: EntityResponse<E>,
        metrics: ExecutionOutcomeMetrics,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
        execution_time_micros: u64,
    ) -> EntityResponse<E> {
        Self::finalize_path_outcome(execution_trace, metrics, false, execution_time_micros);
        set_rows_from_len(span, response.len());

        response
    }

    /// Finalize one structural scalar page before typed surface projection.
    pub(in crate::db::executor) fn finalize_structural_page(
        page: StructuralCursorPage,
        metrics: ExecutionOutcomeMetrics,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
        execution_time_micros: u64,
    ) -> StructuralCursorPage {
        Self::finalize_path_outcome(execution_trace, metrics, false, execution_time_micros);
        set_rows_from_len(span, page.row_count());

        page
    }
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
