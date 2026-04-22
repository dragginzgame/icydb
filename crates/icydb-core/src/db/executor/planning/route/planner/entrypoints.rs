//! Module: db::executor::planning::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation,
            planning::{
                continuation::ScalarContinuationContext, preparation::slot_map_for_model_plan,
            },
            route::{
                AggregateRouteShape, ExecutionRoutePlan, LoadTerminalFastPathContract,
                derive_load_terminal_fast_path_contract_for_plan,
                pk_order_stream_fast_path_shape_supported,
            },
        },
        query::plan::{AccessPlannedQuery, GroupedPlanStrategy},
    },
    error::InternalError,
};

use crate::db::executor::planning::route::planner::{
    RouteIntentStage, build_execution_route_plan_from_stages, derive_aggregate_route_intent_stage,
    derive_execution_feasibility_stage_for_model, derive_grouped_route_intent_stage,
    derive_load_route_intent_stage, derive_mutation_execution_feasibility_stage_for_model,
    derive_mutation_route_intent_stage,
};

///
/// RoutePlanRequest
///
/// Canonical staged route-build request surface.
/// Callers select one structural route family here instead of choosing among
/// multiple public route-builder entrypoints with overlapping staged behavior.
///
pub(in crate::db::executor) enum RoutePlanRequest<'a> {
    Load {
        continuation: &'a ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
        authority: Option<EntityAuthority>,
        load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
    },
    MutationDelete,
    Aggregate {
        aggregate: AggregateRouteShape<'a>,
        execution_preparation: &'a ExecutionPreparation,
    },
    Grouped {
        grouped_plan_strategy: GroupedPlanStrategy,
    },
}

/// Build canonical staged execution routing from one structural route request.
pub(in crate::db::executor) fn build_execution_route_plan(
    plan: &AccessPlannedQuery,
    request: RoutePlanRequest<'_>,
) -> Result<ExecutionPlan, InternalError> {
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
            authority,
            load_terminal_fast_path,
        ),
        RoutePlanRequest::MutationDelete => build_mutation_execution_route_plan(plan),
        RoutePlanRequest::Aggregate {
            aggregate,
            execution_preparation,
        } => Ok(build_aggregate_execution_route_plan(
            plan,
            aggregate,
            execution_preparation,
        )),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy,
        } => Ok(build_grouped_execution_route_plan(
            plan,
            grouped_plan_strategy,
        )),
    }
}

/// Build canonical execution routing for load execution from structural model authority.
fn build_load_execution_route_plan(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    authority: Option<EntityAuthority>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> Result<ExecutionPlan, InternalError> {
    if let Some(authority) = authority
        && pk_order_stream_fast_path_shape_supported(plan)
    {
        continuation.validate_pk_fast_path_boundary(authority.primary_key_name())?;
    }
    let load_terminal_fast_path = load_terminal_fast_path.or_else(|| {
        authority
            .and_then(|authority| derive_load_terminal_fast_path_contract_for_plan(authority, plan))
    });

    Ok(build_load_route_plan_from_stages(
        plan,
        continuation,
        probe_fetch_hint,
        derive_load_route_intent_stage(),
        load_terminal_fast_path,
    ))
}

/// Build canonical execution routing for mutation execution from structural model authority.
fn build_mutation_execution_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, InternalError> {
    let intent_stage = derive_mutation_route_intent_stage(plan)?;
    let feasibility_stage = derive_mutation_execution_feasibility_stage_for_model(plan);

    Ok(build_execution_route_plan_from_stages(
        intent_stage,
        feasibility_stage,
        None,
    ))
}

/// Build canonical aggregate execution routing from planner-frozen query metadata.
fn build_aggregate_execution_route_plan(
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

fn build_load_route_plan_from_stages(
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

fn build_grouped_execution_route_plan(
    plan: &AccessPlannedQuery,
    grouped_plan_strategy: GroupedPlanStrategy,
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
