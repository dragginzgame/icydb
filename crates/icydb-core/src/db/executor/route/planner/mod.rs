//! Module: executor::route::planner
//! Responsibility: derive immutable execution-route plans from validated inputs.
//! Does not own: logical plan construction or physical key-stream execution.
//! Boundary: route planning contracts consumed by load/aggregate/mutation executors.

mod execution;
mod feasibility;
mod intent;

#[cfg(test)]
pub(in crate::db::executor) use feasibility::grouped_ordered_runtime_revalidation_flag_count_guard;

use crate::{
    db::{
        access::PushdownApplicability,
        direction::Direction,
        executor::{
            Context, ContinuationCapabilities, ExecutionPlan, ExecutionPreparation,
            OrderedKeyStreamBox,
            aggregate::{AggregateFoldMode, AggregateKind},
            continuation::ScalarContinuationContext,
            load::LoadExecutor,
        },
        query::builder::AggregateExpr,
        query::plan::{
            AccessPlannedQuery, ContinuationPolicy, GroupedExecutorHandoff,
            GroupedPlanStrategyHint, PlannerRouteProfile,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AggregateSeekSpec, ContinuationMode, ExecutionModeRouteCase, ExecutionRoutePlan, FastPathOrder,
    GroupedExecutionStrategy, GroupedRouteDecisionOutcome, GroupedRouteObservability,
    GroupedRouteRejectionReason, IndexRangeLimitSpec, MUTATION_FAST_PATH_ORDER, RouteCapabilities,
    RouteContinuationPlan, RouteExecutionMode, RouteIntent, RouteShapeKind, ScanHintPlan,
    TopNSeekSpec,
};

///
/// RouteDerivationContext
///
/// Immutable route-owned derivation bundle for one validated plan + intent.
/// Keeps direction, capability snapshot, scan hints, and secondary-order
/// pushdown applicability aligned under one boundary.
///

pub(in crate::db::executor::route::planner) struct RouteDerivationContext {
    pub(in crate::db::executor::route::planner) direction: Direction,
    pub(in crate::db::executor::route::planner) capabilities: RouteCapabilities,
    pub(in crate::db::executor::route::planner) secondary_pushdown_applicability:
        PushdownApplicability,
    pub(in crate::db::executor::route::planner) scan_hints: ScanHintPlan,
    pub(in crate::db::executor::route::planner) top_n_seek_spec: Option<TopNSeekSpec>,
    pub(in crate::db::executor::route::planner) count_pushdown_eligible: bool,
    pub(in crate::db::executor::route::planner) aggregate_physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor::route::planner) aggregate_seek_spec: Option<AggregateSeekSpec>,
    pub(in crate::db::executor::route::planner) grouped_execution_strategy:
        Option<GroupedExecutionStrategy>,
}

///
/// RouteIntentStage
///
/// Immutable route-intent normalization for staged route derivation.
/// Captures aggregate presence, canonical fast-path order, and materialization
/// forcing policy in one typed boundary.
///

pub(in crate::db::executor::route::planner) struct RouteIntentStage {
    pub(in crate::db::executor::route::planner) aggregate_expr: Option<AggregateExpr>,
    pub(in crate::db::executor::route::planner) grouped: bool,
    pub(in crate::db::executor::route::planner) grouped_plan_strategy_hint:
        Option<GroupedPlanStrategyHint>,
    pub(in crate::db::executor::route::planner) fast_path_order: &'static [FastPathOrder],
    pub(in crate::db::executor::route::planner) aggregate_force_materialized_due_to_predicate_uncertainty:
        bool,
}

impl RouteIntentStage {
    /// Return aggregate kind carried by this intent stage, if any.
    pub(in crate::db::executor::route::planner) fn kind(&self) -> Option<AggregateKind> {
        self.aggregate_expr.as_ref().map(AggregateExpr::kind)
    }
}

///
/// RouteFeasibilityStage
///
/// Immutable route feasibility derivation stage.
/// Captures continuation/window policy, capability snapshot, scan hints, and
/// index-range limit feasibility before execution-mode resolution.
///

pub(in crate::db::executor::route::planner) struct RouteFeasibilityStage {
    pub(in crate::db::executor::route::planner) continuation: RouteContinuationPlan,
    pub(in crate::db::executor::route::planner) derivation: RouteDerivationContext,
    pub(in crate::db::executor::route::planner) index_range_limit_spec: Option<IndexRangeLimitSpec>,
}

///
/// RouteExecutionStage
///
/// Immutable execution-mode stage derived from feasibility and intent.
/// Captures final execution mode, aggregate fold mode, and post-mode
/// index-range limit routing.
///

pub(in crate::db::executor::route::planner) struct RouteExecutionStage {
    pub(in crate::db::executor::route::planner) route_shape_kind: RouteShapeKind,
    pub(in crate::db::executor::route::planner) execution_mode_case: ExecutionModeRouteCase,
    pub(in crate::db::executor::route::planner) execution_mode: RouteExecutionMode,
    pub(in crate::db::executor::route::planner) aggregate_fold_mode: AggregateFoldMode,
    pub(in crate::db::executor::route::planner) index_range_limit_spec: Option<IndexRangeLimitSpec>,
}

impl ExecutionRoutePlan {
    /// Construct one mutation-route plan with mutation-safe defaults.
    pub(in crate::db::executor::route) const fn for_mutation(
        capabilities: RouteCapabilities,
    ) -> Self {
        Self {
            direction: Direction::Asc,
            route_shape_kind: RouteShapeKind::MutationDelete,
            continuation: RouteContinuationPlan::new(
                ContinuationCapabilities::new(
                    ContinuationMode::Initial,
                    ContinuationPolicy::new(true, true, true),
                ),
                0,
                crate::db::executor::route::AccessWindow::new(0, None, None, None),
                crate::db::executor::route::AccessWindow::new(0, None, None, None),
            ),
            execution_mode: RouteExecutionMode::Materialized,
            execution_mode_case: ExecutionModeRouteCase::Load,
            secondary_pushdown_applicability: PushdownApplicability::NotApplicable,
            index_range_limit_spec: None,
            capabilities,
            fast_path_order: &MUTATION_FAST_PATH_ORDER,
            top_n_seek_spec: None,
            aggregate_seek_spec: None,
            scan_hints: ScanHintPlan {
                physical_fetch_hint: None,
                load_scan_budget_hint: None,
            },
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
            grouped_execution_strategy: None,
        }
    }

    // Grouped route observability projection.
    // Non-grouped routes intentionally report no grouped diagnostics payload.
    /// Project grouped route observability payload when grouped routing is active.
    pub(in crate::db::executor) const fn grouped_observability(
        &self,
    ) -> Option<GroupedRouteObservability> {
        debug_assert!(
            matches!(
                (self.route_shape_kind, self.execution_mode_case),
                (
                    RouteShapeKind::LoadScalar | RouteShapeKind::MutationDelete,
                    ExecutionModeRouteCase::Load
                ) | (
                    RouteShapeKind::AggregateCount,
                    ExecutionModeRouteCase::AggregateCount
                ) | (
                    RouteShapeKind::AggregateNonCount,
                    ExecutionModeRouteCase::AggregateNonCount
                ) | (
                    RouteShapeKind::AggregateGrouped,
                    ExecutionModeRouteCase::AggregateGrouped
                )
            ),
            "route invariant: route shape kind and execution-mode case must remain aligned",
        );

        match self.execution_mode_case {
            ExecutionModeRouteCase::AggregateGrouped => {
                let grouped_execution_strategy = match self.grouped_execution_strategy {
                    Some(strategy) => strategy,
                    None => GroupedExecutionStrategy::HashMaterialized,
                };
                let eligible = self.fast_path_order.is_empty();
                let (outcome, rejection_reason) = if eligible {
                    match self.execution_mode {
                        RouteExecutionMode::Materialized => {
                            (GroupedRouteDecisionOutcome::MaterializedFallback, None)
                        }
                        RouteExecutionMode::Streaming => {
                            (GroupedRouteDecisionOutcome::Selected, None)
                        }
                    }
                } else {
                    (
                        GroupedRouteDecisionOutcome::Rejected,
                        Some(GroupedRouteRejectionReason::CapabilityMismatch),
                    )
                };

                Some(GroupedRouteObservability {
                    outcome,
                    rejection_reason,
                    eligible,
                    execution_mode: self.execution_mode,
                    grouped_execution_strategy,
                })
            }
            ExecutionModeRouteCase::Load
            | ExecutionModeRouteCase::AggregateCount
            | ExecutionModeRouteCase::AggregateNonCount => None,
        }
    }
}

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
            return Err(crate::db::error::executor_invariant(
                "mutation route planning requires delete plans",
            ));
        }

        let capabilities = Self::derive_route_capabilities(plan, Direction::Asc, None);

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
        let feasibility_stage = Self::derive_route_feasibility_stage(
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
