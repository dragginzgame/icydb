//! Module: executor::route::planner::entrypoints
//! Responsibility: route-planner entrypoint orchestration for load/aggregate/mutation.
//! Does not own: intent/feasibility/execution stage semantics.
//! Boundary: consumes staged planner contracts and assembles execution route plans.

use crate::{
    db::{
        direction::Direction,
        executor::{
            Context, ExecutionPlan, ExecutionPreparation, OrderedKeyStreamBox,
            continuation::ScalarContinuationContext,
            route::{ExecutionRoutePlan, RouteIntent},
            shared::load_contracts::LoadExecutor,
        },
        query::{
            builder::AggregateExpr,
            plan::{AccessPlannedQuery, GroupedExecutorHandoff, PlannerRouteProfile},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

#[cfg(test)]
use crate::db::executor::aggregate::AggregateKind;
use crate::db::executor::route::planner::{
    RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one routed key stream through the canonical stream-construction
    /// facade so route consumers do not call context stream builders directly.
    pub(in crate::db::executor) fn resolve_routed_key_stream(
        ctx: &Context<'_, E>,
        request: crate::db::executor::route::RoutedKeyStreamRequest<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        match request {
            crate::db::executor::route::RoutedKeyStreamRequest::AccessDescriptor(descriptor) => {
                ctx.ordered_key_stream_from_access_descriptor(descriptor)
            }
        }
    }

    /// Build canonical execution routing for load execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_load(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: &ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
    ) -> Result<ExecutionPlan, InternalError> {
        if Self::pk_order_stream_fast_path_shape_supported(plan) {
            continuation.validate_pk_fast_path_boundary::<E>()?;
        }

        Ok(Self::build_execution_route_plan(
            plan,
            continuation,
            probe_fetch_hint,
            RouteIntent::Load,
        ))
    }

    /// Build canonical execution routing for mutation execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> Result<ExecutionPlan, InternalError> {
        if !plan.scalar_plan().mode.is_delete() {
            return Err(crate::db::error::query_executor_invariant(
                "mutation route planning requires delete plans",
            ));
        }

        let capabilities = Self::derive_execution_capabilities(plan, Direction::Asc, None);

        Ok(ExecutionRoutePlan::for_mutation(capabilities))
    }

    // Build canonical execution routing for aggregate execution.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate(
        plan: &AccessPlannedQuery<E::Key>,
        kind: AggregateKind,
    ) -> ExecutionPlan {
        Self::build_execution_route_plan_for_aggregate_spec(plan, aggregate_terminal_expr(kind))
    }

    // Build canonical execution routing for aggregate execution via spec.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: AggregateExpr,
    ) -> ExecutionPlan {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);

        Self::build_execution_route_plan_for_aggregate_spec_with_preparation(
            plan,
            aggregate,
            &execution_preparation,
        )
    }

    /// Build canonical aggregate execution routing using one precomputed preparation bundle.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec_with_preparation(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: AggregateExpr,
        execution_preparation: &ExecutionPreparation,
    ) -> ExecutionPlan {
        let continuation = ScalarContinuationContext::initial();

        Self::build_execution_route_plan(
            plan,
            &continuation,
            None,
            RouteIntent::Aggregate {
                aggregate,
                aggregate_force_materialized_due_to_predicate_uncertainty:
                    Self::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                        execution_preparation,
                    ),
            },
        )
    }

    /// Build canonical grouped aggregate routing from one grouped executor handoff.
    pub(in crate::db::executor) fn build_execution_route_plan_for_grouped_handoff(
        grouped: GroupedExecutorHandoff<'_, E::Key>,
    ) -> ExecutionPlan {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(grouped.base());
        let continuation = ScalarContinuationContext::initial();

        Self::build_execution_route_plan(
            grouped.base(),
            &continuation,
            None,
            RouteIntent::AggregateGrouped {
                grouped_plan_strategy_hint: grouped.grouped_plan_strategy_hint(),
                aggregate_force_materialized_due_to_predicate_uncertainty:
                    Self::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                        &execution_preparation,
                    ),
            },
        )
    }

    // Shared route gate for load + aggregate execution.
    fn build_execution_route_plan(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: &ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        // Phase 1: project one planner-owned route profile consumed by route derivation.
        let planner_route_profile = Self::derive_planner_route_profile(plan);

        // Phase 2: normalize route intent into one immutable intent stage.
        let intent_stage = Self::derive_route_intent_stage(intent);

        // Phase 3: derive continuation/window/capability feasibility.
        let feasibility_stage = Self::derive_execution_feasibility_stage(
            plan,
            continuation,
            probe_fetch_hint,
            &planner_route_profile,
            &intent_stage,
        );

        // Phase 4: resolve execution mode and fold-mode from feasibility + intent.
        let execution_stage = Self::derive_route_execution_stage(&intent_stage, &feasibility_stage);

        // Phase 5: assemble the final immutable route contract.
        Self::assemble_execution_route_plan(intent_stage, feasibility_stage, execution_stage)
    }

    // Build one planner-projected route profile from one validated access plan.
    fn derive_planner_route_profile(plan: &AccessPlannedQuery<E::Key>) -> PlannerRouteProfile {
        plan.planner_route_profile(E::MODEL)
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
}

#[cfg(test)]
fn aggregate_terminal_expr(kind: AggregateKind) -> AggregateExpr {
    match kind {
        AggregateKind::Count => crate::db::query::builder::aggregate::count(),
        AggregateKind::Sum => {
            unreachable!("aggregate route-terminal helper must not construct SUM(fieldless) intent")
        }
        AggregateKind::Exists => crate::db::query::builder::aggregate::exists(),
        AggregateKind::Min => crate::db::query::builder::aggregate::min(),
        AggregateKind::Max => crate::db::query::builder::aggregate::max(),
        AggregateKind::First => crate::db::query::builder::aggregate::first(),
        AggregateKind::Last => crate::db::query::builder::aggregate::last(),
    }
}
