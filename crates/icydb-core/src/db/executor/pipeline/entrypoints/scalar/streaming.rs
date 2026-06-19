//! Module: executor::pipeline::entrypoints::scalar::streaming
//! Responsibility: scalar streaming aggregate kernel row sink execution.
//! Does not own: page materialization or public scalar entrypoint setup.
//! Boundary: executes scalar route windows up to post-access kernel rows.

use crate::{
    db::{
        executor::{
            ExecutionKernel, ExecutionTrace,
            pipeline::{
                contracts::{ExecutionOutcomeMetrics, KernelRowsExecutionAttempt},
                entrypoints::scalar::{
                    execution::{attach_execution_stats_to_trace, execute_prepared_scalar_kernel},
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
// 2) path-outcome observability metrics
// 3) optional execution trace
// 4) elapsed execution time for finalization-compatible attribution
type ScalarKernelRowSinkExecution = (usize, ExecutionOutcomeMetrics, Option<ExecutionTrace>, u64);

// Execute one prepared scalar runtime bundle through the canonical scalar spine,
// stopping after post-access/windowed kernel rows for aggregate reducers.
pub(super) fn execute_prepared_scalar_kernel_row_sink_execution(
    prepared: PreparedScalarRouteRuntime,
    mut row_sink: impl FnMut(&KernelRow) -> Result<(), InternalError>,
) -> Result<ScalarKernelRowSinkExecution, InternalError> {
    let execution = execute_prepared_scalar_kernel(
        prepared,
        |_, _, _, _| {},
        |execution_inputs, route_plan, continuation| {
            ExecutionKernel::materialize_kernel_rows_with_optional_residual_retry(
                execution_inputs,
                route_plan,
                continuation,
                IndexCompilePolicy::ConservativeSubset,
            )
        },
    )?;
    let mut execution_stats = execution.execution_stats;
    let mut execution_trace = execution.execution_trace;
    let execution_time_micros = execution.execution_time_micros;
    let KernelRowsExecutionAttempt {
        rows,
        rows_scanned,
        post_access_rows,
        optimization,
        index_predicate_applied,
        index_predicate_keys_rejected,
        distinct_keys_deduped,
    } = execution.attempt;
    let projected_rows = rows.len();
    for row in &rows {
        row_sink(row)?;
    }
    let metrics = ExecutionOutcomeMetrics {
        optimization,
        rows_scanned,
        post_access_rows,
        index_predicate_applied,
        index_predicate_keys_rejected,
        distinct_keys_deduped,
    };
    if let Some(stats) = execution_stats.as_mut() {
        stats.apply_scalar_outcome(
            metrics.rows_scanned,
            metrics.post_access_rows,
            projected_rows,
            metrics.distinct_keys_deduped,
        );
    }
    attach_execution_stats_to_trace(&mut execution_trace, execution_stats);

    Ok((
        projected_rows,
        metrics,
        execution_trace.take(),
        execution_time_micros,
    ))
}
