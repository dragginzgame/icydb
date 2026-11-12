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
                    hints::{ScalarRouteTerminal, finalize_scalar_route_for_execution},
                    runtime::PreparedScalarRouteRuntime,
                },
            },
        },
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
};

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
) -> Result<T, InternalError> {
    let PreparedScalarRouteRuntime {
        store,
        authority,
        plan_core,
        route_plan,
        prep,
        projection,
        continuation,
        unpaged_rows_mode: _,
        cursor_emission,
        projection_runtime_mode,
        suppress_route_scan_hints: _,
        enforced_scan_probe_limit,
    } = finalize_scalar_route_for_execution(prepared, terminal);
    let accepted_schema = authority.accepted_schema_authority();
    let accepted_root = CardinalityAcceptedRootIdentity::new(
        accepted_schema.revision(),
        accepted_schema.fingerprint(),
    )?;
    let secondary_index_anchor = continuation.secondary_index_resume_anchor(
        plan_core.plan(),
        authority.accepted_schema_info(),
        plan_core.single_index_prefix_specs(),
        plan_core.single_index_range_specs(),
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
    );
    let plan = plan_core.plan();
    let route_continuation = route_plan.continuation();
    continuation.debug_assert_route_continuation_invariants(plan, route_continuation);
    let direction = route_plan.direction();

    let executable_access = plan.access.executable_contract();
    let access_continuation = continuation.clone();
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputContext {
        runtime: &runtime,
        plan,
        executable_access,
        stream_bindings: AccessStreamBindings::new(access_continuation.access_scan_input(
            direction,
            plan,
            secondary_index_anchor.as_ref(),
        ))
        .with_index_prefix_child_expansion(route_plan.scan_hints.index_prefix_child_expansion),
        execution_preparation: &prep,
        projection_materialization: projection_runtime_mode,
        prepared_projection: projection,
        emit_cursor: cursor_emission.enabled(),
        enforced_scan_probe_limit,
    });
    execute(&execution_inputs, &route_plan, continuation)
}
