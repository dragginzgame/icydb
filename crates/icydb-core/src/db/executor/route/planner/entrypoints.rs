//! Module: executor::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionPlan, ExecutionPreparation,
            continuation::ScalarContinuationContext,
            preparation::resolved_index_slots_for_access_path,
            route::{
                ExecutionRoutePlan, LoadTerminalFastPathContract, RouteIntent,
                aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation,
                derive_execution_capabilities_for_model,
                derive_load_terminal_fast_path_contract_for_model_plan,
                pk_order_stream_fast_path_shape_supported_for_model,
            },
        },
        query::{
            builder::AggregateExpr,
            plan::{AccessPlannedQuery, PlannerRouteProfile},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
};

use crate::db::executor::route::planner::{
    RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
    derive_execution_feasibility_stage_for_model, derive_route_execution_stage,
    derive_route_intent_stage,
};

/// Build canonical execution routing for load execution from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_load_with_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, InternalError> {
    if pk_order_stream_fast_path_shape_supported_for_model(model, plan) {
        continuation.validate_pk_fast_path_boundary_for_model(model)?;
    }

    Ok(build_execution_route_plan_for_model(
        model,
        plan,
        continuation,
        probe_fetch_hint,
        RouteIntent::Load,
    ))
}

/// Build canonical execution routing for one initial load execution.
///
/// This narrower entrypoint exists for surfaces that never carry an inbound
/// cursor boundary. Keeping the initial-continuation path separate lets those
/// callers avoid retaining PK cursor-boundary validation in their route setup.
#[expect(
    clippy::unnecessary_wraps,
    reason = "keeps initial and resumed load-route call sites on the same fallible boundary"
)]
pub(in crate::db::executor) fn build_initial_execution_route_plan_for_load_with_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
) -> Result<ExecutionPlan, InternalError> {
    Ok(build_initial_execution_route_plan_for_model(
        model,
        plan,
        probe_fetch_hint,
        RouteIntent::Load,
    ))
}

/// Build canonical execution routing for mutation execution from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_mutation_with_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, InternalError> {
    if !plan.scalar_plan().mode.is_delete() {
        return Err(InternalError::query_executor_invariant(
            "mutation route planning requires delete plans",
        ));
    }

    let capabilities = derive_execution_capabilities_for_model(model, plan, Direction::Asc, None);

    Ok(ExecutionRoutePlan::for_mutation(capabilities))
}

/// Build canonical aggregate execution routing from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec_with_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: AggregateExpr,
    execution_preparation: &ExecutionPreparation,
) -> ExecutionPlan {
    build_initial_execution_route_plan_for_model(
        model,
        plan,
        None,
        RouteIntent::Aggregate {
            aggregate,
            aggregate_force_materialized_due_to_predicate_uncertainty:
                aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                    execution_preparation,
                ),
        },
    )
}

fn build_execution_route_plan_for_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    intent: RouteIntent,
) -> ExecutionRoutePlan {
    let load_terminal_fast_path = match &intent {
        RouteIntent::Load => derive_load_terminal_fast_path_contract_for_model_plan(model, plan),
        RouteIntent::Aggregate { .. } | RouteIntent::AggregateGrouped { .. } => None,
    };
    let planner_route_profile = derive_planner_route_profile(model, plan);
    let intent_stage = derive_route_intent_stage(intent);
    let feasibility_stage = derive_execution_feasibility_stage_for_model(
        model,
        plan,
        continuation,
        probe_fetch_hint,
        &planner_route_profile,
        &intent_stage,
    );

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage, load_terminal_fast_path)
}

fn derive_planner_route_profile(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> PlannerRouteProfile {
    plan.planner_route_profile(model)
}

pub(in crate::db::executor) fn build_execution_route_plan_for_grouped_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    grouped_plan_strategy_hint: crate::db::query::plan::GroupedPlanStrategyHint,
) -> ExecutionPlan {
    let execution_preparation = ExecutionPreparation::from_plan(
        model,
        plan,
        resolved_index_slots_for_access_path(model, plan.access.resolve_strategy().executable()),
    );
    build_initial_execution_route_plan_for_model(
        model,
        plan,
        None,
        RouteIntent::AggregateGrouped {
            grouped_plan_strategy_hint,
            aggregate_force_materialized_due_to_predicate_uncertainty:
                aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                    &execution_preparation,
                ),
        },
    )
}

// Entry points without inbound cursors all share the same initial continuation
// contract before route-stage derivation.
fn build_initial_execution_route_plan_for_model(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    probe_fetch_hint: Option<usize>,
    intent: RouteIntent,
) -> ExecutionRoutePlan {
    let continuation = ScalarContinuationContext::initial();

    build_execution_route_plan_for_model(model, plan, &continuation, probe_fetch_hint, intent)
}

// Build one shared execution route contract from intent + feasibility stages.
fn build_execution_route_plan_from_stages(
    intent_stage: RouteIntentStage,
    feasibility_stage: RouteFeasibilityStage,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    // Phase 1: resolve execution mode and fold-mode from feasibility + intent.
    let execution_stage = derive_route_execution_stage(&intent_stage, &feasibility_stage);

    // Phase 2: assemble one immutable route contract.
    assemble_execution_route_plan(
        intent_stage,
        feasibility_stage,
        execution_stage,
        load_terminal_fast_path,
    )
}

fn assemble_execution_route_plan(
    intent_stage: RouteIntentStage,
    feasibility_stage: RouteFeasibilityStage,
    execution_stage: RouteExecutionStage,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let RouteFeasibilityStage {
        continuation,
        derivation,
        index_range_limit_spec: _,
    } = feasibility_stage;

    ExecutionRoutePlan {
        direction: derivation.direction,
        route_shape_kind: execution_stage.route_shape_kind,
        continuation,
        execution_mode: execution_stage.execution_mode,
        execution_mode_case: execution_stage.execution_mode_case,
        secondary_pushdown_applicability: derivation.secondary_pushdown_applicability,
        index_range_limit_spec: execution_stage.index_range_limit_spec,
        capabilities: derivation.capabilities,
        fast_path_order: intent_stage.fast_path_order,
        top_n_seek_spec: derivation.top_n_seek_spec,
        aggregate_seek_spec: derivation.aggregate_seek_spec,
        scan_hints: derivation.scan_hints,
        aggregate_fold_mode: execution_stage.aggregate_fold_mode,
        grouped_execution_strategy: derivation.grouped_execution_strategy,
        load_terminal_fast_path,
    }
}
