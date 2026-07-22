//! Module: executor::pipeline::entrypoints::scalar::materialized
//! Responsibility: scalar materialized runtime execution spine.
//! Does not own: runtime bundle construction, streaming aggregate execution, or finalization.
//! Boundary: converts prepared scalar runtime into payload, metrics, trace, and timing.

use crate::{
    db::{
        executor::{
            ExecutionKernel, ExecutionTrace,
            pipeline::{
                contracts::{ExecutionOutcomeMetrics, StructuralCursorPage},
                entrypoints::scalar::{
                    execution::{
                        execute_prepared_scalar_kernel, finish_scalar_kernel_observability,
                    },
                    finalize::finalize_scalar_structural_path_execution,
                    hints::ScalarRouteTerminal,
                    runtime::PreparedScalarRouteRuntime,
                },
            },
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};

// Shared scalar runtime output tuple:
// 1) final materialized payload
// 2) path-outcome observability metrics
// 3) optional execution trace
// 4) elapsed execution time for finalization
pub(super) type ScalarPathExecution = (
    StructuralCursorPage,
    ExecutionOutcomeMetrics,
    Option<ExecutionTrace>,
    u64,
);

// Execute one prepared scalar runtime bundle through the canonical monomorphic
// scalar spine without re-entering typed executor state.
pub(super) fn execute_prepared_scalar_path_execution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<ScalarPathExecution, InternalError> {
    let execution = execute_prepared_scalar_kernel(
        prepared,
        ScalarRouteTerminal::MaterializedPage,
        |execution_inputs, route_plan, continuation| {
            ExecutionKernel::materialize_with_optional_residual_retry(
                execution_inputs,
                route_plan,
                continuation,
                IndexCompilePolicy::ConservativeSubset,
            )
        },
    )?;
    let execution_stats = execution.execution_stats;
    let mut execution_trace = execution.execution_trace;
    let execution_time_micros = execution.execution_time_micros;
    let materialized = execution.attempt;
    let (payload, metrics) = materialized.into_payload_and_metrics();
    finish_scalar_kernel_observability(
        &mut execution_trace,
        execution_stats,
        &metrics,
        payload.row_count(),
    );

    Ok((
        payload,
        metrics,
        execution_trace.take(),
        execution_time_micros,
    ))
}

// Execute one prepared scalar runtime bundle and finalize the shared
// structural page boundary in the common non-attributed path.
pub(super) fn execute_prepared_scalar_structural_page(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    let entity_path = prepared.entity_path();

    Ok(finalize_scalar_structural_path_execution(
        entity_path,
        execute_prepared_scalar_path_execution(prepared)?,
    ))
}

/// Execute one prepared scalar runtime bundle and finalize the structural page.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, Option<ExecutionTrace>), InternalError> {
    execute_prepared_scalar_structural_page(prepared)
}

/// Execute one prepared scalar plan while retaining its authoritative scan count.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime_with_scan_count(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, usize), InternalError> {
    let entity_path = prepared.entity_path();
    let execution = execute_prepared_scalar_path_execution(prepared)?;
    let rows_scanned = execution.1.rows_scanned;
    let (page, _) = finalize_scalar_structural_path_execution(entity_path, execution);

    Ok((page, rows_scanned))
}
