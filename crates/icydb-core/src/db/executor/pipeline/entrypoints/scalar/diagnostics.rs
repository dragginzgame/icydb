//! Module: executor::pipeline::entrypoints::scalar::diagnostics
//! Responsibility: scalar execution phase attribution.
//! Does not own: normal scalar execution, runtime construction, or finalization policy.
//! Boundary: wraps the scalar runtime and finalize phases with diagnostics counters.

use crate::{
    db::{
        diagnostics::measure_local_instruction_delta as measure_scalar_execute_phase,
        executor::{
            ExecutionTrace,
            pipeline::{
                contracts::StructuralCursorPage,
                entrypoints::scalar::{
                    finalize::finalize_scalar_structural_path_execution,
                    materialized::execute_prepared_scalar_path_execution,
                    runtime::PreparedScalarRouteRuntime,
                },
            },
            terminal::with_direct_data_row_phase_attribution,
        },
    },
    error::InternalError,
};

///
/// ScalarExecutePhaseAttribution
///
/// ScalarExecutePhaseAttribution records the internal scalar-load execute split
/// after a prepared plan has already crossed the session compile boundary.
/// It isolates the monomorphic runtime materialization spine from the final
/// structural page assembly step so perf tooling can see whether the remaining
/// floor lives in runtime traversal or page finalization.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct ScalarExecutePhaseAttribution {
    pub(in crate::db) runtime_local_instructions: u64,
    pub(in crate::db) finalize_local_instructions: u64,
    pub(in crate::db) direct_data_row_scan_local_instructions: u64,
    pub(in crate::db) direct_data_row_key_stream_local_instructions: u64,
    pub(in crate::db) direct_data_row_row_read_local_instructions: u64,
    pub(in crate::db) direct_data_row_key_encode_local_instructions: u64,
    pub(in crate::db) direct_data_row_store_get_local_instructions: u64,
    pub(in crate::db) direct_data_row_order_window_local_instructions: u64,
    pub(in crate::db) direct_data_row_page_window_local_instructions: u64,
}

/// Execute one prepared scalar runtime bundle while reporting the internal
/// runtime/finalize split for perf-only attribution surfaces.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime_with_phase_attribution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<
    (
        StructuralCursorPage,
        Option<ExecutionTrace>,
        ScalarExecutePhaseAttribution,
    ),
    InternalError,
> {
    let entity_path = prepared.entity_path();

    // Phase 1: run the monomorphic scalar runtime spine.
    let ((runtime_local_instructions, execution), direct_data_row_phase_attribution) =
        with_direct_data_row_phase_attribution(|| {
            measure_scalar_execute_phase(|| execute_prepared_scalar_path_execution(prepared))
        });
    let execution = execution?;

    // Phase 2: finalize the structural page and observability payload.
    let (finalize_local_instructions, finalized) = measure_scalar_execute_phase(|| {
        Ok::<(StructuralCursorPage, Option<ExecutionTrace>), InternalError>(
            finalize_scalar_structural_path_execution(entity_path, execution),
        )
    });
    let (page, trace) = finalized?;

    Ok((
        page,
        trace,
        ScalarExecutePhaseAttribution {
            runtime_local_instructions,
            finalize_local_instructions,
            direct_data_row_scan_local_instructions: direct_data_row_phase_attribution
                .scan_local_instructions,
            direct_data_row_key_stream_local_instructions: direct_data_row_phase_attribution
                .key_stream_local_instructions,
            direct_data_row_row_read_local_instructions: direct_data_row_phase_attribution
                .row_read_local_instructions,
            direct_data_row_key_encode_local_instructions: direct_data_row_phase_attribution
                .key_encode_local_instructions,
            direct_data_row_store_get_local_instructions: direct_data_row_phase_attribution
                .store_get_local_instructions,
            direct_data_row_order_window_local_instructions: direct_data_row_phase_attribution
                .order_window_local_instructions,
            direct_data_row_page_window_local_instructions: direct_data_row_phase_attribution
                .page_window_local_instructions,
        },
    ))
}
