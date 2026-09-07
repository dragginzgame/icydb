//! Module: executor::pipeline::entrypoints::scalar::materialized
//! Responsibility: scalar materialized runtime execution spine.
//! Does not own: runtime bundle construction, streaming aggregate execution, or finalization.
//! Boundary: converts prepared scalar runtime into a payload and scan count.

use crate::{
    db::{
        executor::{
            ExecutionKernel,
            pipeline::{
                contracts::{MaterializedExecutionAttempt, StructuralCursorPage},
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

/// Execute one prepared scalar plan while retaining its authoritative scan count.
pub(in crate::db::executor) fn execute_prepared_scalar_route_runtime_with_scan_count(
    prepared: PreparedScalarRouteRuntime,
) -> Result<(StructuralCursorPage, usize), InternalError> {
    let MaterializedExecutionAttempt { payload, metrics } = execute_prepared_scalar_kernel(
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

    // The retry kernel has already accumulated all attempts' scan work.
    Ok((payload, metrics.rows_scanned))
}
