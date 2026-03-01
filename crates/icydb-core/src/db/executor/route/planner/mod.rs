//! Module: executor::route::planner
//! Responsibility: derive immutable execution-route plans from validated inputs.
//! Does not own: logical plan construction or physical key-stream execution.
//! Boundary: route planning contracts consumed by load/aggregate/mutation executors.

mod execution;
mod feasibility;
mod intent;

use crate::{
    db::{
        access::PushdownApplicability,
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            Context, ExecutionPlan, ExecutionPreparation, OrderedKeyStreamBox, RangeToken,
            aggregate::{AggregateFoldMode, AggregateKind, AggregateSpec},
            compute_page_window,
            load::LoadExecutor,
        },
        query::plan::{AccessPlannedQuery, GroupedExecutorHandoff},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    ContinuationMode, ExecutionMode, ExecutionModeRouteCase, ExecutionRoutePlan, FastPathOrder,
    GroupedRouteDecisionOutcome, GroupedRouteObservability, GroupedRouteRejectionReason,
    IndexRangeLimitSpec, MUTATION_FAST_PATH_ORDER, RouteCapabilities, RouteIntent, RouteWindowPlan,
    ScanHintPlan,
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
    pub(in crate::db::executor::route::planner) count_pushdown_eligible: bool,
    pub(in crate::db::executor::route::planner) aggregate_physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor::route::planner) aggregate_secondary_extrema_probe_fetch_hint:
        Option<usize>,
}

///
/// RouteIntentStage
///
/// Immutable route-intent normalization for staged route derivation.
/// Captures aggregate presence, canonical fast-path order, and materialization
/// forcing policy in one typed boundary.
///

pub(in crate::db::executor::route::planner) struct RouteIntentStage {
    pub(in crate::db::executor::route::planner) aggregate_spec: Option<AggregateSpec>,
    pub(in crate::db::executor::route::planner) grouped: bool,
    pub(in crate::db::executor::route::planner) fast_path_order: &'static [FastPathOrder],
    pub(in crate::db::executor::route::planner) aggregate_force_materialized_due_to_predicate_uncertainty:
        bool,
}

impl RouteIntentStage {
    /// Return aggregate kind carried by this intent stage, if any.
    pub(in crate::db::executor::route::planner) fn kind(&self) -> Option<AggregateKind> {
        self.aggregate_spec.as_ref().map(AggregateSpec::kind)
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
    pub(in crate::db::executor::route::planner) continuation_mode: ContinuationMode,
    pub(in crate::db::executor::route::planner) route_window: RouteWindowPlan,
    pub(in crate::db::executor::route::planner) derivation: RouteDerivationContext,
    pub(in crate::db::executor::route::planner) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    pub(in crate::db::executor::route::planner) page_limit_is_zero: bool,
}

///
/// RouteExecutionStage
///
/// Immutable execution-mode stage derived from feasibility and intent.
/// Captures final execution mode, aggregate fold mode, and post-mode
/// index-range limit routing.
///

pub(in crate::db::executor::route::planner) struct RouteExecutionStage {
    pub(in crate::db::executor::route::planner) execution_mode_case: ExecutionModeRouteCase,
    pub(in crate::db::executor::route::planner) execution_mode: ExecutionMode,
    pub(in crate::db::executor::route::planner) aggregate_fold_mode: AggregateFoldMode,
    pub(in crate::db::executor::route::planner) index_range_limit_spec: Option<IndexRangeLimitSpec>,
}

impl RouteWindowPlan {
    // Build the canonical route window payload from effective offset + optional
    // page limit, keeping keep/fetch counts aligned with shared page math.
    pub(in crate::db::executor::route) fn new(effective_offset: u32, limit: Option<u32>) -> Self {
        let (keep_count, fetch_count) = match limit {
            Some(limit) => {
                let keep = compute_page_window(effective_offset, limit, false).keep_count;
                let fetch = compute_page_window(effective_offset, limit, true).fetch_count;
                (Some(keep), Some(fetch))
            }
            None => (None, None),
        };

        Self {
            effective_offset,
            limit,
            keep_count,
            fetch_count,
        }
    }
}

impl ExecutionRoutePlan {
    /// Construct one mutation-route plan with mutation-safe defaults.
    pub(in crate::db::executor::route) const fn for_mutation(
        capabilities: RouteCapabilities,
    ) -> Self {
        Self {
            direction: Direction::Asc,
            continuation_mode: ContinuationMode::Initial,
            window: RouteWindowPlan {
                effective_offset: 0,
                limit: None,
                keep_count: None,
                fetch_count: None,
            },
            execution_mode: ExecutionMode::Materialized,
            execution_mode_case: ExecutionModeRouteCase::Load,
            secondary_pushdown_applicability: PushdownApplicability::NotApplicable,
            index_range_limit_spec: None,
            capabilities,
            fast_path_order: &MUTATION_FAST_PATH_ORDER,
            aggregate_secondary_extrema_probe_fetch_hint: None,
            scan_hints: ScanHintPlan {
                physical_fetch_hint: None,
                load_scan_budget_hint: None,
            },
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
        }
    }

    // Grouped route observability projection.
    // Non-grouped routes intentionally report no grouped diagnostics payload.
    /// Project grouped route observability payload when grouped routing is active.
    pub(in crate::db::executor) const fn grouped_observability(
        &self,
    ) -> Option<GroupedRouteObservability> {
        match self.execution_mode_case {
            ExecutionModeRouteCase::AggregateGrouped => {
                let eligible = self.fast_path_order.is_empty();
                let (outcome, rejection_reason) = if !eligible {
                    (
                        GroupedRouteDecisionOutcome::Rejected,
                        Some(GroupedRouteRejectionReason::CapabilityMismatch),
                    )
                } else if matches!(self.execution_mode, ExecutionMode::Materialized) {
                    (GroupedRouteDecisionOutcome::MaterializedFallback, None)
                } else {
                    (GroupedRouteDecisionOutcome::Selected, None)
                };

                Some(GroupedRouteObservability {
                    outcome,
                    rejection_reason,
                    eligible,
                    execution_mode: self.execution_mode,
                })
            }
            _ => None,
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
            crate::db::executor::route::RoutedKeyStreamRequest::AccessPlan(stream_request) => {
                ctx.ordered_key_stream_from_access_plan_with_index_range_anchor(stream_request)
            }
            crate::db::executor::route::RoutedKeyStreamRequest::AccessPath {
                access,
                constraints,
                direction,
                hints,
            } => ctx.ordered_key_stream_from_access(access, constraints, direction, hints),
        }
    }

    /// Build canonical execution routing for load execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_load(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<ExecutionPlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(Self::build_execution_route_plan(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            RouteIntent::Load,
        ))
    }

    /// Build canonical execution routing for mutation execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> Result<ExecutionPlan, InternalError> {
        if !plan.scalar_plan().mode.is_delete() {
            return Err(InternalError::query_executor_invariant(
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
        Self::build_execution_route_plan_for_aggregate_spec(plan, AggregateSpec::for_terminal(kind))
    }

    // Build canonical execution routing for aggregate execution via spec.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
    ) -> ExecutionPlan {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);

        Self::build_execution_route_plan_for_aggregate_spec_with_preparation(
            plan,
            spec,
            &execution_preparation,
        )
    }

    /// Build canonical aggregate execution routing using one precomputed preparation bundle.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec_with_preparation(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
        execution_preparation: &ExecutionPreparation,
    ) -> ExecutionPlan {
        Self::build_execution_route_plan(
            plan,
            None,
            None,
            None,
            RouteIntent::Aggregate {
                spec,
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

        Self::build_execution_route_plan(
            grouped.base(),
            None,
            None,
            None,
            RouteIntent::AggregateGrouped {
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
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        // Phase 1: normalize route intent into one immutable intent stage.
        let intent_stage = Self::derive_route_intent_stage(intent);

        // Phase 2: derive continuation/window/capability feasibility.
        let feasibility_stage = Self::derive_route_feasibility_stage(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            &intent_stage,
        );

        // Phase 3: resolve execution mode and fold-mode from feasibility + intent.
        let execution_stage = Self::derive_route_execution_stage(&intent_stage, &feasibility_stage);

        // Phase 4: assemble the final immutable route contract.
        Self::assemble_execution_route_plan(intent_stage, feasibility_stage, execution_stage)
    }

    fn assemble_execution_route_plan(
        intent_stage: RouteIntentStage,
        feasibility_stage: RouteFeasibilityStage,
        execution_stage: RouteExecutionStage,
    ) -> ExecutionRoutePlan {
        let RouteFeasibilityStage {
            continuation_mode,
            route_window,
            derivation,
            index_range_limit_spec: _,
            page_limit_is_zero: _,
        } = feasibility_stage;

        ExecutionRoutePlan {
            direction: derivation.direction,
            continuation_mode,
            window: route_window,
            execution_mode: execution_stage.execution_mode,
            execution_mode_case: execution_stage.execution_mode_case,
            secondary_pushdown_applicability: derivation.secondary_pushdown_applicability,
            index_range_limit_spec: execution_stage.index_range_limit_spec,
            capabilities: derivation.capabilities,
            fast_path_order: intent_stage.fast_path_order,
            aggregate_secondary_extrema_probe_fetch_hint: derivation
                .aggregate_secondary_extrema_probe_fetch_hint,
            scan_hints: derivation.scan_hints,
            aggregate_fold_mode: execution_stage.aggregate_fold_mode,
        }
    }
}
