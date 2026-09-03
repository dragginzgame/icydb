//! Module: executor::pipeline::entrypoints::scalar::streaming
//! Responsibility: scalar streaming aggregate kernel row sink execution.
//! Does not own: page materialization or public scalar entrypoint setup.
//! Boundary: executes scalar route windows up to post-access kernel rows.

use crate::{
    db::{
        executor::{
            ExecutionKernel,
            pipeline::{
                contracts::KernelRowsExecutionAttempt,
                entrypoints::scalar::{
                    execution::execute_prepared_scalar_kernel, hints::ScalarRouteTerminal,
                    runtime::PreparedScalarRouteRuntime,
                },
            },
            terminal::KernelRow,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};

// Shared scalar aggregate row-sink output tuple:
// 1) post-access/windowed rows fed into the sink
// Execute one prepared scalar runtime bundle through the canonical scalar spine,
// stopping after post-access/windowed kernel rows for aggregate reducers.
pub(super) fn execute_prepared_scalar_kernel_row_sink_execution(
    prepared: PreparedScalarRouteRuntime,
    mut row_sink: impl FnMut(&KernelRow) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let execution = execute_prepared_scalar_kernel(
        prepared,
        ScalarRouteTerminal::KernelRows,
        |execution_inputs, route_plan, continuation| {
            ExecutionKernel::materialize_kernel_rows_with_optional_residual_retry(
                execution_inputs,
                route_plan,
                continuation,
                IndexCompilePolicy::ConservativeSubset,
            )
        },
    )?;
    let KernelRowsExecutionAttempt { rows, .. } = execution.attempt;
    for row in &rows {
        row_sink(row)?;
    }

    Ok(())
}
