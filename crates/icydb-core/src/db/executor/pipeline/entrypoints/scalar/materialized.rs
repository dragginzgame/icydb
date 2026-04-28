//! Module: executor::pipeline::entrypoints::scalar::materialized
//! Responsibility: scalar materialized runtime execution spine.
//! Does not own: runtime bundle construction, streaming aggregate execution, or finalization.
//! Boundary: converts prepared scalar runtime into payload, metrics, trace, and timing.

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
                    MaterializedExecutionPayload, PreparedExecutionInputParts,
                    StructuralCursorPage,
                },
                entrypoints::scalar::{
                    finalize::finalize_scalar_structural_path_execution,
                    hints::apply_unpaged_top_n_seek_hints, runtime::PreparedScalarRouteRuntime,
                },
            },
            plan_metrics::record_plan_metrics,
            planning::route::top_n_seek_lookahead_required_for_shape,
            with_execution_stats_capture,
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
    MaterializedExecutionPayload,
    ExecutionOutcomeMetrics,
    Option<ExecutionTrace>,
    u64,
);

// Apply route hints and continuation invariants shared by scalar materialized
// execution before the kernel receives the route plan.
fn prepare_scalar_route_for_execution(
    route_plan: &mut crate::db::executor::ExecutionPlan,
    plan: &crate::db::query::plan::AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    unpaged_rows_mode: bool,
    suppress_route_scan_hints: bool,
) {
    let top_n_seek_requires_lookahead = plan
        .access_capabilities()
        .single_path_capabilities()
        .is_some_and(top_n_seek_lookahead_required_for_shape);
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

// Execute one prepared scalar runtime bundle through the canonical monomorphic
// scalar spine without re-entering typed executor state.
pub(super) fn execute_prepared_scalar_path_execution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<ScalarPathExecution, InternalError> {
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
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime_parts(
        TraversalRuntime::new(store, authority.entity_tag()),
        store,
        authority,
    );
    let plan = plan_core.plan();
    let index_prefix_specs = plan_core.index_prefix_specs()?;
    let index_range_specs = plan_core.index_range_specs()?;

    // Phase 1: apply structural route hints derived from the scalar load plan.
    prepare_scalar_route_for_execution(
        &mut route_plan,
        plan,
        &continuation,
        unpaged_rows_mode,
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

    // Phase 3: build canonical execution inputs and materialize the scalar route.
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
    record_plan_metrics(&plan.access);
    let (materialized, mut execution_stats) = with_execution_stats_capture(debug, || {
        ExecutionKernel::materialize_with_optional_residual_retry(
            &execution_inputs,
            &route_plan,
            &continuation,
            IndexCompilePolicy::ConservativeSubset,
        )
    });
    let materialized = materialized?;
    let execution_time_micros = elapsed_execution_micros(execution_started_at);
    let (payload, metrics) = materialized.into_payload_and_metrics();
    if let Some(stats) = execution_stats.as_mut() {
        stats.apply_scalar_outcome(
            metrics.rows_scanned,
            metrics.post_access_rows,
            payload.row_count(),
            metrics.distinct_keys_deduped,
        );
    }
    if let Some(trace) = execution_trace.as_mut() {
        trace.set_execution_stats(
            execution_stats.map(crate::db::executor::ExecutionProfileStats::into_execution_stats),
        );
    }

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
