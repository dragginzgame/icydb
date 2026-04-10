//! Module: executor::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        direction::Direction,
        executor::{
            EntityAuthority, ExecutionPlan, ExecutionPreparation,
            continuation::ScalarContinuationContext,
            preparation::slot_map_for_model_plan,
            route::{
                AggregateRouteShape, ExecutionRoutePlan, GroupedExecutionMode,
                GroupedExecutionModeProjection, LoadTerminalFastPathContract, RouteIntent,
                aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation,
                derive_execution_capabilities_for_model,
                derive_load_terminal_fast_path_contract_for_plan,
                pk_order_stream_fast_path_shape_supported,
            },
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

use crate::db::executor::route::planner::{
    RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
    derive_execution_feasibility_stage_for_model, derive_route_execution_stage,
    derive_route_intent_stage,
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
        RouteIntent::Load,
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
        RouteIntent::Load,
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
        RouteIntent::Load,
        load_terminal_fast_path,
    ))
}

/// Build canonical execution routing for mutation execution from structural model authority.
pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
    _authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<ExecutionPlan, InternalError> {
    if !plan.scalar_plan().mode.is_delete() {
        return Err(InternalError::query_executor_invariant(
            "mutation route planning requires delete plans",
        ));
    }

    let planner_route_profile = plan.planner_route_profile();
    let capabilities =
        derive_execution_capabilities_for_model(plan, planner_route_profile, Direction::Asc, None);

    Ok(ExecutionRoutePlan::for_mutation(capabilities))
}

/// Build canonical aggregate execution routing from planner-frozen query metadata.
pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    execution_preparation: &ExecutionPreparation,
) -> ExecutionPlan {
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_route_intent_stage(RouteIntent::Aggregate {
        aggregate,
        aggregate_force_materialized_due_to_predicate_uncertainty:
            aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                execution_preparation,
            ),
    });
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
    intent: RouteIntent<'_>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let planner_route_profile = plan.planner_route_profile();
    let intent_stage = derive_route_intent_stage(intent);
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
    let intent_stage = derive_route_intent_stage(RouteIntent::AggregateGrouped {
        grouped_plan_strategy,
        aggregate_force_materialized_due_to_predicate_uncertainty:
            aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                &execution_preparation,
            ),
    });
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
    intent: RouteIntent<'_>,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let continuation = ScalarContinuationContext::initial();

    build_execution_route_plan(
        plan,
        &continuation,
        probe_fetch_hint,
        intent,
        load_terminal_fast_path,
    )
}

// Build one shared execution route contract from intent + feasibility stages.
fn build_execution_route_plan_from_stages(
    intent_stage: RouteIntentStage<'_>,
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
    intent_stage: RouteIntentStage<'_>,
    feasibility_stage: RouteFeasibilityStage,
    execution_stage: RouteExecutionStage,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let RouteFeasibilityStage {
        continuation,
        derivation,
        index_range_limit_spec: _,
    } = feasibility_stage;
    debug_assert!(
        intent_stage.grouped == derivation.grouped_execution_mode.is_some(),
        "grouped route assembly must align grouped intent with grouped execution-mode projection",
    );
    if let Some(grouped_plan_strategy) = intent_stage.grouped_plan_strategy {
        debug_assert!(
            derivation.grouped_execution_mode
                == Some(GroupedExecutionMode::from_planner_strategy(
                    grouped_plan_strategy,
                    GroupedExecutionModeProjection::from_route_capabilities(
                        derivation.direction,
                        derivation.capabilities,
                    ),
                )),
            "grouped route assembly must not drift from the canonical grouped execution-mode projection",
        );
    }

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
        grouped_plan_strategy: intent_stage.grouped_plan_strategy,
        grouped_execution_mode: derivation.grouped_execution_mode,
        load_terminal_fast_path,
    }
}
