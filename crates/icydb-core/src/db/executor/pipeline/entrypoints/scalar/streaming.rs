//! Module: executor::pipeline::entrypoints::scalar::streaming
//! Responsibility: scalar streaming aggregate kernel row sink execution.
//! Does not own: page materialization or public scalar entrypoint setup.
//! Boundary: executes scalar route windows up to post-access kernel rows.

use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutionKernel, ExecutionTrace, ScalarContinuationContext,
            TraversalRuntime,
            diagnostics::execution_trace_for_access,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntimeAdapter,
                    KernelRowsExecutionAttempt, PreparedExecutionInputParts,
                },
                entrypoints::scalar::{
                    hints::apply_unpaged_top_n_seek_hints, runtime::PreparedScalarRouteRuntime,
                },
            },
            plan_metrics::record_plan_metrics,
            planning::route::top_n_seek_lookahead_required_for_shape,
            terminal::KernelRow,
            with_execution_stats_capture,
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

// Apply route hints and continuation invariants shared by aggregate row sinks
// before the kernel receives the route plan.
const fn prepare_scalar_sink_route_for_execution(
    route_plan: &mut crate::db::executor::ExecutionPlan,
    continuation: &ScalarContinuationContext,
    unpaged_rows_mode: bool,
    top_n_seek_requires_lookahead: bool,
    suppress_route_scan_hints: bool,
) {
    apply_unpaged_top_n_seek_hints(
        continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        route_plan,
    );
    if suppress_route_scan_hints {
        route_plan.scan_hints.physical_fetch_hint = None;
        route_plan.scan_hints.load_scan_budget_hint = None;
    }
}

// Execute one prepared scalar runtime bundle through the canonical scalar spine,
// stopping after post-access/windowed kernel rows for aggregate reducers.
#[expect(
    clippy::too_many_lines,
    reason = "aggregate row sinks intentionally mirror scalar page entrypoint setup so route/window semantics stay aligned"
)]
pub(super) fn execute_prepared_scalar_kernel_row_sink_execution(
    prepared: PreparedScalarRouteRuntime,
    mut row_sink: impl FnMut(&KernelRow) -> Result<(), InternalError>,
) -> Result<ScalarKernelRowSinkExecution, InternalError> {
    let PreparedScalarRouteRuntime {
        store,
        authority,
        plan_core,
        mut route_plan,
        prep,
        projection,
        continuation,
        unpaged_rows_mode,
        cursor_emission,
        projection_runtime_mode,
        suppress_route_scan_hints,
        debug,
    } = prepared;
    let entity_path = authority.entity_path();
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime_parts(
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority,
    );
    let plan = plan_core.plan();
    let index_prefix_specs = plan_core.index_prefix_specs()?;
    let index_range_specs = plan_core.index_range_specs()?;

    // Phase 1: keep aggregate row sinks on the same route-hint path as scalar
    // page materialization so bounded windows observe identical input rows.
    let top_n_seek_requires_lookahead = plan
        .access_capabilities()
        .single_path_capabilities()
        .is_some_and(top_n_seek_lookahead_required_for_shape);
    prepare_scalar_sink_route_for_execution(
        &mut route_plan,
        &continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        suppress_route_scan_hints,
    );

    // Phase 2: project continuation invariants and optional trace setup once.
    let route_continuation = route_plan.continuation();
    let continuation_applied = route_continuation.applied();
    continuation.debug_assert_route_continuation_invariants(plan, route_continuation);
    let direction = route_plan.direction();
    let mut execution_trace =
        debug.then(|| execution_trace_for_access(&plan.access, direction, continuation_applied));
    let execution_started_at = start_execution_timer();

    // Phase 3: run the shared scalar kernel to post-access rows, but skip
    // structural page cursor/final payload construction.
    let executable_access = plan.access.executable_contract();
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputParts {
        runtime: &runtime,
        plan,
        executable_access,
        stream_bindings: AccessStreamBindings {
            index_prefix_specs,
            index_range_specs,
            continuation: continuation.access_scan_input(direction),
        },
        execution_preparation: &prep,
        projection_materialization: projection_runtime_mode,
        prepared_projection: projection,
        emit_cursor: cursor_emission.enabled(),
    });
    record_plan_metrics(entity_path, &plan.access);
    let (attempt, mut execution_stats) = with_execution_stats_capture(debug, || {
        ExecutionKernel::materialize_kernel_rows_with_optional_residual_retry(
            &execution_inputs,
            &route_plan,
            &continuation,
            IndexCompilePolicy::ConservativeSubset,
        )
    });
    let KernelRowsExecutionAttempt {
        rows,
        rows_scanned,
        post_access_rows,
        optimization,
        index_predicate_applied,
        index_predicate_keys_rejected,
        distinct_keys_deduped,
    } = attempt?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
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
    if let Some(trace) = execution_trace.as_mut() {
        trace.set_execution_stats(
            execution_stats.map(crate::db::executor::ExecutionProfileStats::into_execution_stats),
        );
    }

    Ok((
        projected_rows,
        metrics,
        execution_trace.take(),
        execution_time_micros,
    ))
}
