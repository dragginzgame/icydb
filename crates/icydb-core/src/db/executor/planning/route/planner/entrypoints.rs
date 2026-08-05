//! Module: executor::planning::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for scalar loads,
//! grouped execution, and EXPLAIN-only scalar aggregates.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

#[cfg(feature = "sql")]
use crate::db::executor::planning::route::AggregateRouteShape;
#[cfg(feature = "sql")]
use crate::db::executor::planning::route::planner::derive_aggregate_route_intent_stage;
use crate::db::executor::planning::route::planner::{
    build_execution_route_plan_from_stages, derive_execution_feasibility_stage_for_model,
    derive_grouped_route_intent_stage, derive_load_route_intent_stage,
};
use crate::db::{
    executor::{
        EntityAuthority, ExecutionPreparation, ExecutionRoutePlan,
        planning::{continuation::ScalarContinuationContext, preparation::slot_map_for_model_plan},
        route::derive_load_terminal_fast_path_contract_for_plan,
    },
    query::plan::{AccessPlannedQuery, CoveringReadExecutionPlan, GroupedPlanStrategy},
};

///
/// RoutePlanRequest
///
/// Canonical borrowed-state-free runtime route-build request surface.
/// Scalar aggregate EXPLAIN has a feature-gated entrypoint because its route
/// shape borrows preparation state that runtime load/grouped requests do not.
///
pub(in crate::db::executor) enum RoutePlanRequest {
    Load {
        continuation: ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
        authority: Option<Box<EntityAuthority>>,
        load_terminal_fast_path: Option<CoveringReadExecutionPlan>,
    },
    Grouped {
        grouped_plan_strategy: GroupedPlanStrategy,
    },
}

/// Build canonical staged execution routing from one structural route request.
pub(in crate::db::executor) fn build_execution_route_plan(
    plan: &AccessPlannedQuery,
    request: RoutePlanRequest,
) -> ExecutionRoutePlan {
    match request {
        RoutePlanRequest::Load {
            continuation,
            probe_fetch_hint,
            authority,
            load_terminal_fast_path,
        } => build_load_execution_route_plan(
            plan,
            continuation,
            probe_fetch_hint,
            authority.map(|authority| *authority),
            load_terminal_fast_path,
        ),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy,
        } => build_grouped_execution_route_plan(plan, grouped_plan_strategy),
    }
}

/// Build canonical execution routing for load execution from structural model authority.
fn build_load_execution_route_plan(
    plan: &AccessPlannedQuery,
    continuation: ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    authority: Option<EntityAuthority>,
    load_terminal_fast_path: Option<CoveringReadExecutionPlan>,
) -> ExecutionRoutePlan {
    let load_terminal_fast_path = load_terminal_fast_path.or_else(|| {
        authority
            .and_then(|authority| derive_load_terminal_fast_path_contract_for_plan(authority, plan))
    });

    // Load still derives feasibility through the shared staged planner path.
    // The only load-local work left here is the optional terminal fast-path
    // override/derivation contract.
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_load_route_intent_stage();
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        continuation,
        probe_fetch_hint,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, load_terminal_fast_path)
}

/// Build canonical aggregate EXPLAIN routing from planner-frozen query metadata.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn build_aggregate_execution_route_plan_for_explain(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    execution_preparation: &ExecutionPreparation,
) -> ExecutionRoutePlan {
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_aggregate_route_intent_stage(aggregate, execution_preparation);
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        ScalarContinuationContext::initial(),
        None,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, None)
}

fn build_grouped_execution_route_plan(
    plan: &AccessPlannedQuery,
    grouped_plan_strategy: GroupedPlanStrategy,
) -> ExecutionRoutePlan {
    let execution_preparation =
        ExecutionPreparation::from_plan(plan, slot_map_for_model_plan(plan));
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage =
        derive_grouped_route_intent_stage(grouped_plan_strategy, &execution_preparation);
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        ScalarContinuationContext::initial(),
        None,
        planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, None)
}
