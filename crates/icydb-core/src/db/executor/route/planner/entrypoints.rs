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
            pipeline::contracts::LoadExecutor,
            preparation::resolved_index_slots_for_access_path,
            route::{
                ExecutionRoutePlan, RouteIntent,
                aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation,
                pk_order_stream_fast_path_shape_supported_for_model,
            },
        },
        query::{
            builder::AggregateExpr,
            plan::{AccessPlannedQuery, GroupedExecutorHandoff, PlannerRouteProfile},
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::planner::{
    RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
    derive_execution_feasibility_stage_for_model, derive_route_execution_stage,
    derive_route_intent_stage,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Build canonical execution routing for load execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_load(
        plan: &AccessPlannedQuery,
        continuation: &ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
    ) -> Result<ExecutionPlan, InternalError> {
        if pk_order_stream_fast_path_shape_supported_for_model(E::MODEL, plan) {
            continuation.validate_pk_fast_path_boundary::<E>()?;
        }

        Ok(build_execution_route_plan_for_model(
            E::MODEL,
            plan,
            continuation,
            probe_fetch_hint,
            RouteIntent::Load,
        ))
    }

    /// Build canonical execution routing for mutation execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
        plan: &AccessPlannedQuery,
    ) -> Result<ExecutionPlan, InternalError> {
        if !plan.scalar_plan().mode.is_delete() {
            return Err(crate::db::error::query_executor_invariant(
                "mutation route planning requires delete plans",
            ));
        }

        let capabilities = Self::derive_execution_capabilities(plan, Direction::Asc, None);

        Ok(ExecutionRoutePlan::for_mutation(capabilities))
    }

    /// Build canonical aggregate execution routing using one precomputed preparation bundle.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec_with_preparation(
        plan: &AccessPlannedQuery,
        aggregate: AggregateExpr,
        execution_preparation: &ExecutionPreparation,
    ) -> ExecutionPlan {
        let continuation = ScalarContinuationContext::initial();

        build_execution_route_plan_for_model(
            E::MODEL,
            plan,
            &continuation,
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

    /// Build canonical grouped aggregate routing from one grouped executor handoff.
    pub(in crate::db::executor) fn build_execution_route_plan_for_grouped_handoff(
        grouped: GroupedExecutorHandoff<'_>,
    ) -> ExecutionPlan {
        build_execution_route_plan_for_grouped_plan(
            E::MODEL,
            grouped.base(),
            grouped.grouped_plan_strategy_hint(),
        )
    }
}

fn build_execution_route_plan_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    intent: RouteIntent,
) -> ExecutionRoutePlan {
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

    build_execution_route_plan_from_stages(intent_stage, feasibility_stage)
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
    let continuation = ScalarContinuationContext::initial();

    build_execution_route_plan_for_model(
        model,
        plan,
        &continuation,
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

// Build one shared execution route contract from intent + feasibility stages.
fn build_execution_route_plan_from_stages(
    intent_stage: RouteIntentStage,
    feasibility_stage: RouteFeasibilityStage,
) -> ExecutionRoutePlan {
    // Phase 1: resolve execution mode and fold-mode from feasibility + intent.
    let execution_stage = derive_route_execution_stage(&intent_stage, &feasibility_stage);

    // Phase 2: assemble one immutable route contract.
    assemble_execution_route_plan(intent_stage, feasibility_stage, execution_stage)
}

fn assemble_execution_route_plan(
    intent_stage: RouteIntentStage,
    feasibility_stage: RouteFeasibilityStage,
    execution_stage: RouteExecutionStage,
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
    }
}
