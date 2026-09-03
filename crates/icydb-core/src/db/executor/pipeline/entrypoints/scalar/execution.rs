//! Module: executor::pipeline::entrypoints::scalar::execution
//! Responsibility: shared scalar route execution setup.
//! Does not own: materialized-page finalization or aggregate row sinking.
//! Boundary: prepares route hints, continuation checks, and execution inputs.

use crate::{
    db::{
        executor::{
            AccessStreamBindings, ExecutionRoutePlan, ScalarContinuationContext, TraversalRuntime,
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionRuntimeAdapter, PreparedExecutionInputContext,
                },
                entrypoints::scalar::{
                    hints::{ScalarRouteTerminal, normalize_scalar_route_for_execution},
                    runtime::PreparedScalarRouteRuntime,
                },
            },
        },
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
};

///
/// PreparedScalarKernelExecution
///
/// PreparedScalarKernelExecution carries one completed scalar kernel attempt.
///

pub(super) struct PreparedScalarKernelExecution<T> {
    pub(super) attempt: T,
}

// Run one prepared scalar runtime through shared route/input setup, then let
// the caller choose which scalar kernel terminal to invoke.
pub(super) fn execute_prepared_scalar_kernel<T>(
    prepared: PreparedScalarRouteRuntime,
    terminal: ScalarRouteTerminal,
    execute: impl FnOnce(
        &ExecutionInputs<'_>,
        &ExecutionRoutePlan,
        ScalarContinuationContext,
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
        enforced_scan_probe_limit,
    } = prepared;
    let accepted_schema = authority.accepted_schema_authority()?;
    let accepted_root = CardinalityAcceptedRootIdentity::new(
        accepted_schema.revision(),
        accepted_schema.fingerprint(),
    )?;
    let runtime = ExecutionRuntimeAdapter::from_scalar_runtime(
        TraversalRuntime::new(
            store,
            authority.entity_tag(),
            authority
                .accepted_runtime_root_identity()
                .database_incarnation(),
            accepted_root,
        ),
        store,
        authority,
    )?;
    let plan = plan_core.plan();
    let index_prefix_specs = plan_core.index_prefix_specs();
    let index_range_specs = plan_core.index_range_specs();
    normalize_scalar_route_for_execution(
        &mut route_plan,
        plan,
        &continuation,
        unpaged_rows_mode,
        suppress_route_scan_hints,
        terminal,
        &prep,
    );
    if enforced_scan_probe_limit.is_some() {
        // Exact selection must observe one authoritative bounded traversal,
        // never an incomplete top-N/index-window probe followed by fallback.
        route_plan.index_range_limit_spec = None;
        route_plan.top_n_seek_spec = None;
        route_plan.scan_hints.physical_fetch_hint = None;
        route_plan.scan_hints.load_scan_budget_hint = None;
    }

    let route_continuation = route_plan.continuation();
    continuation.debug_assert_route_continuation_invariants(plan, route_continuation);
    let direction = route_plan.direction();

    let executable_access = plan.access.executable_contract();
    let access_continuation = continuation.clone();
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputContext {
        runtime: &runtime,
        plan,
        executable_access,
        stream_bindings: AccessStreamBindings::new(
            index_prefix_specs,
            index_range_specs,
            access_continuation.access_scan_input(direction, plan),
        )
        .with_index_prefix_child_expansion(route_plan.scan_hints.index_prefix_child_expansion),
        execution_preparation: &prep,
        projection_materialization: projection_runtime_mode,
        prepared_projection: projection,
        emit_cursor: cursor_emission.enabled(),
        enforced_scan_probe_limit,
    });
    let attempt = execute(&execution_inputs, &route_plan, continuation)?;

    Ok(PreparedScalarKernelExecution { attempt })
}
