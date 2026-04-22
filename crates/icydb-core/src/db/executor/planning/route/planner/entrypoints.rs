//! Module: db::executor::planning::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        access::PushdownApplicability,
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation,
            planning::{
                continuation::ScalarContinuationContext, preparation::slot_map_for_model_plan,
            },
            route::{
                AggregateRouteShape, LoadTerminalFastPathContract, RouteContinuationPlan,
                ScanHintPlan, derive_execution_capabilities_for_model,
                derive_load_terminal_fast_path_contract_for_plan,
                pk_order_stream_fast_path_shape_supported,
            },
        },
        query::plan::{AccessPlannedQuery, GroupedPlanStrategy},
    },
    error::InternalError,
};

use crate::db::executor::planning::route::planner::stages::{
    RouteCountPushdownState, RouteDerivationSupport,
};
use crate::db::executor::planning::route::planner::{
    RouteDerivationContext, RouteFeasibilityStage, build_execution_route_plan_from_stages,
    derive_aggregate_route_intent_stage, derive_execution_feasibility_stage_for_model,
    derive_grouped_route_intent_stage, derive_load_route_intent_stage,
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

    // Load still derives feasibility through the shared staged planner path.
    // The only load-local work left here is the PK fast-path boundary check
    // and the optional terminal fast-path override/derivation contract.
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_load_route_intent_stage();
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        plan,
        continuation,
        probe_fetch_hint,
        planner_route_profile,
        &intent_stage,
    );

    Ok(build_execution_route_plan_from_stages(
        intent_stage,
        feasibility_stage,
        load_terminal_fast_path,
    ))
}

/// Build canonical execution routing for mutation execution from structural model authority.
fn build_mutation_execution_route_plan(
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, InternalError> {
    let intent_stage = derive_mutation_route_intent_stage(plan)?;

    // Mutation now uses the same staged assembly surface as the other route
    // families, but it still carries mutation-safe defaults instead of
    // borrowing load scan-hint or continuation-window semantics.
    let continuation = RouteContinuationPlan::initial_for_mutation();
    let capabilities = derive_execution_capabilities_for_model(plan, Direction::Asc, None);
    let feasibility_stage = RouteFeasibilityStage {
        continuation,
        derivation: RouteDerivationContext {
            direction: Direction::Asc,
            capabilities,
            support: RouteDerivationSupport {
                desc_physical_reverse_supported: false,
                index_range_limit_pushdown_shape_supported: false,
            },
            count_pushdown: RouteCountPushdownState {
                existing_rows_shape_supported: false,
                eligible: false,
            },
            secondary_pushdown_applicability: PushdownApplicability::NotApplicable,
            scan_hints: ScanHintPlan {
                physical_fetch_hint: None,
                load_scan_budget_hint: None,
            },
            top_n_seek_spec: None,
            aggregate_physical_fetch_hint: None,
            aggregate_seek_spec: None,
            grouped_execution_mode: None,
        },
        index_range_limit_spec: None,
    };

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
