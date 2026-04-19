//! Module: db::executor::planning::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation,
            planning::{
                continuation::ScalarContinuationContext, preparation::slot_map_for_model_plan,
            },
            route::{
                AggregateRouteShape, ExecutionRoutePlan, LoadTerminalFastPathContract,
                derive_execution_capabilities_for_model,
                derive_load_terminal_fast_path_contract_for_plan,
                pk_order_stream_fast_path_shape_supported,
            },
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

use crate::db::executor::planning::route::planner::{
    RouteIntentStage, build_execution_route_plan_from_stages, derive_aggregate_route_intent_stage,
    derive_execution_feasibility_stage_for_model, derive_grouped_route_intent_stage,
    derive_load_route_intent_stage, ensure_mutation_route_plan_is_delete,
};

/// Build canonical execution routing for load execution from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_load(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, InternalError> {
    if pk_order_stream_fast_path_shape_supported(plan) {
        continuation.validate_pk_fast_path_boundary(authority.primary_key_name())?;
    }

    Ok(build_execution_route_plan(
        plan,
        continuation,
        probe_fetch_hint,
        derive_load_route_intent_stage(),
        derive_load_terminal_fast_path_contract_for_plan(authority, plan),
    ))
}

/// Build canonical execution routing for one initial load execution.
#[expect(
    clippy::unnecessary_wraps,
    reason = "keeps initial and resumed load-route call sites on the same fallible boundary"
)]
pub(in crate::db::executor) fn build_initial_execution_route_plan_for_load(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, InternalError> {
    Ok(build_initial_execution_route_plan(
        plan,
        probe_fetch_hint,
        derive_load_route_intent_stage(),
        derive_load_terminal_fast_path_contract_for_plan(authority, plan),
    ))
}

/// Build canonical execution routing for one initial load execution from a
/// pre-derived terminal fast-path contract.
///
/// This narrower entrypoint exists for surfaces that never carry an inbound
/// cursor boundary. Keeping the initial-continuation path separate lets those
/// callers avoid retaining PK cursor-boundary validation in their route setup
/// while preserving the shared fallible route-entry boundary.
#[expect(
    clippy::unnecessary_wraps,
    reason = "keeps explain and runtime route entrypoints on the same fallible boundary"
)]
pub(in crate::db::executor) fn build_initial_execution_route_plan_for_load_with_fast_path(
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> Result<ExecutionPlan, InternalError> {
    Ok(build_initial_execution_route_plan(
        plan,
        probe_fetch_hint,
        derive_load_route_intent_stage(),
        load_terminal_fast_path,
    ))
}

/// Build canonical execution routing for mutation execution from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
    _authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, InternalError> {
    ensure_mutation_route_plan_is_delete(plan)?;

    let capabilities = derive_execution_capabilities_for_model(plan, Direction::Asc, None);

    Ok(ExecutionRoutePlan::for_mutation(capabilities))
}

/// Build canonical aggregate execution routing from planner-frozen query metadata.
pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    execution_preparation: &ExecutionPreparation,
) -> ExecutionPlan {
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_aggregate_route_intent_stage(aggregate, execution_preparation);
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        &ScalarContinuationContext::initial(),
        None,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, None)
}

fn build_execution_route_plan(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    intent_stage: RouteIntentStage<'_>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let planner_route_profile = plan.planner_route_profile();
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        continuation,
        probe_fetch_hint,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, load_terminal_fast_path)
}

pub(in crate::db::executor) fn build_execution_route_plan_for_grouped_plan(
    plan: &AccessPlannedQuery,
    grouped_plan_strategy: crate::db::query::plan::GroupedPlanStrategy,
) -> ExecutionPlan {
    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage =
        derive_grouped_route_intent_stage(grouped_plan_strategy, &execution_preparation);
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        &ScalarContinuationContext::initial(),
        None,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, None)
}

// Entry points without inbound cursors all share the same initial continuation
// contract before route-stage derivation.
fn build_initial_execution_route_plan(
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
    intent_stage: RouteIntentStage<'_>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let continuation = ScalarContinuationContext::initial();

    build_execution_route_plan(
        plan,
        &continuation,
        probe_fetch_hint,
        intent_stage,
        load_terminal_fast_path,
    )
}
