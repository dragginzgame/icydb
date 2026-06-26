//! Module: executor::pipeline::entrypoints::scalar::execution
//! Responsibility: shared scalar route execution setup.
//! Does not own: materialized-page finalization or aggregate row sinking.
//! Boundary: prepares route hints, continuation checks, traces, and execution inputs.

use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutionPlan, ExecutionProfileStats, ExecutionTrace,
            ScalarContinuationContext, TraversalRuntime,
            diagnostics::execution_trace_for_access,
            pipeline::timing::{elapsed_execution_micros, start_execution_timer},
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionOutcomeMetrics, ExecutionRuntimeAdapter,
                    PreparedExecutionInputContext,
                },
                entrypoints::scalar::{
                    hints::apply_unpaged_top_n_seek_hints, runtime::PreparedScalarRouteRuntime,
                },
            },
            plan_metrics::record_plan_metrics,
            planning::route::top_n_seek_lookahead_required_for_shape,
            with_execution_stats_capture,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

///
/// PreparedScalarKernelExecution
///
/// PreparedScalarKernelExecution carries one completed scalar kernel attempt
/// plus the observability state that callers finish after they know the
/// materialized/page or aggregate row-sink outcome counters.
///

pub(super) struct PreparedScalarKernelExecution<T> {
    pub(super) attempt: T,
    pub(super) execution_stats: Option<ExecutionProfileStats>,
    pub(super) execution_trace: Option<ExecutionTrace>,
    pub(super) execution_time_micros: u64,
}

pub(super) fn attach_execution_stats_to_trace(
    execution_trace: &mut Option<ExecutionTrace>,
    execution_stats: Option<ExecutionProfileStats>,
) {
    if let Some(trace) = execution_trace.as_mut() {
        trace.set_execution_stats(execution_stats.map(ExecutionProfileStats::into_execution_stats));
    }
}

// Finish scalar-kernel observability once for materialized page execution and
// aggregate row-sink execution. Both terminals share the same scanned,
// post-access, projected-row, distinct-key, and trace-stat contract.
pub(super) fn finish_scalar_kernel_observability(
    execution_trace: &mut Option<ExecutionTrace>,
    execution_stats: Option<ExecutionProfileStats>,
    metrics: &ExecutionOutcomeMetrics,
    projected_rows: usize,
) {
    let mut execution_stats = execution_stats;
    if let Some(stats) = execution_stats.as_mut() {
        stats.apply_scalar_outcome(
            metrics.rows_scanned,
            metrics.post_access_rows,
            projected_rows,
            metrics.distinct_keys_deduped,
        );
    }
    attach_execution_stats_to_trace(execution_trace, execution_stats);
}

// Apply route hints and continuation invariants shared by scalar materialized
// pages and aggregate row sinks before the kernel receives the route plan.
const fn prepare_scalar_route_for_execution(
    route_plan: &mut ExecutionPlan,
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

// Run one prepared scalar runtime through shared route/input setup, then let
// the caller choose which scalar kernel terminal to invoke.
pub(super) fn execute_prepared_scalar_kernel<T>(
    prepared: PreparedScalarRouteRuntime,
    adjust_route: impl FnOnce(
        &mut ExecutionPlan,
        &AccessPlannedQuery,
        &ScalarContinuationContext,
        &crate::db::executor::ExecutionPreparation,
    ),
    execute: impl FnOnce(
        &ExecutionInputs<'_>,
        &ExecutionPlan,
        &ScalarContinuationContext,
    ) -> Result<T, InternalError>,
) -> Result<PreparedScalarKernelExecution<T>, InternalError> {
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
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime(
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority,
    )?;
    let plan = plan_core.plan();
    let index_prefix_specs = plan_core.index_prefix_specs()?;
    let index_range_specs = plan_core.index_range_specs()?;
    let top_n_seek_requires_lookahead = plan
        .access_shape_facts()
        .single_path_facts()
        .is_some_and(|shape_facts| top_n_seek_lookahead_required_for_shape(&shape_facts));
    prepare_scalar_route_for_execution(
        &mut route_plan,
        &continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        suppress_route_scan_hints,
    );

    let route_continuation = route_plan.continuation();
    let continuation_applied = route_continuation.applied();
    continuation.debug_assert_route_continuation_invariants(plan, route_continuation);
    let direction = route_plan.direction();
    adjust_route(&mut route_plan, plan, &continuation, &prep);
    let mut execution_trace =
        debug.then(|| execution_trace_for_access(&plan.access, direction, continuation_applied));
    let execution_started_at = start_execution_timer();

    let executable_access = plan.access.executable_contract();
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputContext {
        runtime: &runtime,
        plan,
        executable_access,
        stream_bindings: AccessStreamBindings::new(
            index_prefix_specs,
            index_range_specs,
            continuation.access_scan_input(direction, plan),
        )
        .with_index_prefix_child_expansion(route_plan.scan_hints.index_prefix_child_expansion),
        execution_preparation: &prep,
        projection_materialization: projection_runtime_mode,
        prepared_projection: projection,
        emit_cursor: cursor_emission.enabled(),
    });
    record_plan_metrics(entity_path, plan);
    let (attempt, execution_stats) = with_execution_stats_capture(debug, || {
        execute(&execution_inputs, &route_plan, &continuation)
    });
    let attempt = attempt?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);

    Ok(PreparedScalarKernelExecution {
        attempt,
        execution_stats,
        execution_trace: execution_trace.take(),
        execution_time_micros,
    })
}
