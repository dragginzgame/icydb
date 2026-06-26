//! Module: executor::pipeline::entrypoints::scalar::materialized
//! Responsibility: scalar materialized runtime execution spine.
//! Does not own: runtime bundle construction, streaming aggregate execution, or finalization.
//! Boundary: converts prepared scalar runtime into payload, metrics, trace, and timing.

use crate::{
    db::{
        executor::{
            ExecutionKernel, ExecutionTrace, ScalarContinuationContext,
            pipeline::{
                contracts::{
                    ExecutionOutcomeMetrics, MaterializedExecutionPayload, StructuralCursorPage,
                },
                entrypoints::scalar::{
                    execution::{
                        execute_prepared_scalar_kernel, finish_scalar_kernel_observability,
                    },
                    finalize::finalize_scalar_structural_path_execution,
                    runtime::PreparedScalarRouteRuntime,
                },
            },
            route::{
                access_order_satisfied_by_route_mode, branch_set_page_keep_cap_shape_supported,
                index_prefix_set_page_fetch_hint_shape_supported,
            },
        },
        index::IndexCompilePolicy,
        query::plan::AccessPlannedQuery,
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

fn apply_index_set_page_fetch_hint(
    route_plan: &mut crate::db::executor::ExecutionPlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    residual_filter_present: bool,
) {
    let access_shape_facts = plan.access_shape_facts();
    let single_path_facts = access_shape_facts.single_path_facts();
    let branch_set_page = single_path_facts
        .as_ref()
        .is_some_and(branch_set_page_keep_cap_shape_supported);
    let index_prefix_set_page = single_path_facts
        .as_ref()
        .is_some_and(index_prefix_set_page_fetch_hint_shape_supported);
    if route_plan.scan_hints.physical_fetch_hint.is_some()
        || residual_filter_present
        || !index_prefix_set_page
        || !plan.scalar_plan().mode.is_load()
        || plan.scalar_plan().distinct
        || (branch_set_page
            && plan
                .scalar_plan()
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty()))
        || !access_order_satisfied_by_route_mode(plan)
        || !route_plan.load_order_route_mode().allows_streaming_load()
    {
        return;
    }

    let Some(limit) = plan.scalar_plan().page.as_ref().and_then(|page| page.limit) else {
        return;
    };

    let fetch = if limit == 0 {
        0
    } else {
        continuation
            .keep_count_for_limit_window(plan, limit)
            .saturating_add(1)
    };
    route_plan.scan_hints.physical_fetch_hint = Some(fetch);
}

// Execute one prepared scalar runtime bundle through the canonical monomorphic
// scalar spine without re-entering typed executor state.
pub(super) fn execute_prepared_scalar_path_execution(
    prepared: PreparedScalarRouteRuntime,
) -> Result<ScalarPathExecution, InternalError> {
    let execution = execute_prepared_scalar_kernel(
        prepared,
        |route_plan, plan, continuation, prep| {
            apply_index_set_page_fetch_hint(
                route_plan,
                plan,
                continuation,
                prep.effective_runtime_filter_program().is_some(),
            );
        },
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
