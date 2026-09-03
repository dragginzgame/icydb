//! Module: executor::pipeline::entrypoints::scalar::materialized
//! Responsibility: scalar materialized runtime execution spine.
//! Does not own: runtime bundle construction, streaming aggregate execution, or finalization.
//! Boundary: converts prepared scalar runtime into a payload and scan count.

use crate::{
    db::{
        executor::{
            ExecutionKernel,
            pipeline::{
                contracts::{ExecutionOutcomeMetrics, StructuralCursorPage},
                entrypoints::scalar::{
                    execution::execute_prepared_scalar_kernel, hints::ScalarRouteTerminal,
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
pub(super) type ScalarPathExecution = (StructuralCursorPage, ExecutionOutcomeMetrics);

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
    let materialized = execution.attempt;
    let (payload, metrics) = materialized.into_payload_and_metrics();

    Ok((payload, metrics))
}

// Execute one prepared scalar runtime bundle and finalize the shared
// structural page boundary in the common non-attributed path.

/// Execute one prepared scalar plan while retaining its authoritative scan count.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime_with_scan_count(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, usize), InternalError> {
    let execution = execute_prepared_scalar_path_execution(prepared)?;
    let rows_scanned = execution.1.rows_scanned;
    let page = execution.0;

    Ok((page, rows_scanned))
}
