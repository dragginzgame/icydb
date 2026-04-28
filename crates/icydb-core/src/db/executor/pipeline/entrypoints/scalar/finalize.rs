//! Module: executor::pipeline::entrypoints::scalar::finalize
//! Responsibility: scalar structural page finalization.
//! Does not own: route planning, kernel execution, or public entrypoint setup.
//! Boundary: converts materialized scalar payloads into structural cursor pages.

use crate::db::executor::{
    ExecutionTrace,
    pipeline::{
        contracts::{MaterializedExecutionPayload, StructuralCursorPage},
        entrypoints::scalar::materialized::ScalarPathExecution,
        runtime::finalize_structural_page_for_path,
    },
};

// Finalize one scalar runtime tuple when the payload must be a structural page.
pub(super) fn finalize_scalar_structural_path_execution(
    entity_path: &'static str,
    execution: ScalarPathExecution,
) -> (StructuralCursorPage, Option<ExecutionTrace>) {
    let (payload, metrics, mut trace, execution_time_micros) = execution;
    let page: MaterializedExecutionPayload = payload;
    let page = finalize_structural_page_for_path(
        entity_path,
        page,
        metrics,
        &mut trace,
        execution_time_micros,
    );

    (page, trace)
}
