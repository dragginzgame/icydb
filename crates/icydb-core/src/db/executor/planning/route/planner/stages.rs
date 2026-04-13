//! Module: executor::route::planner::stages
//! Responsibility: immutable staged route-planning contracts shared by planner phases.
//! Does not own: stage derivation orchestration or route execution assembly.
//! Boundary: exposes the typed stage bundles consumed by planner entrypoint, intent, feasibility, and execution modules.

use crate::db::executor::planning::route::planner::execution::derive_route_execution_stage;
use crate::db::{
    access::PushdownApplicability,
    direction::Direction,
    executor::{
        aggregate::{AggregateFoldMode, AggregateKind},
        route::{
            AggregateRouteShape, AggregateSeekSpec, ExecutionRoutePlan, FastPathOrder,
            GroupedExecutionMode, GroupedExecutionModeProjection, IndexRangeLimitSpec,
            LoadTerminalFastPathContract, RouteCapabilities, RouteContinuationPlan,
            RouteExecutionMode, RouteShapeKind, ScanHintPlan, TopNSeekSpec,
        },
    },
    query::plan::GroupedPlanStrategy,
};

///
/// RouteDerivationContext
///
/// Immutable route-owned derivation bundle for one validated plan + intent.
/// Keeps direction, capability snapshot, scan hints, and secondary-order
/// pushdown applicability aligned under one boundary.
///

pub(in crate::db::executor::planning::route::planner) struct RouteDerivationContext {
    pub(in crate::db::executor::planning::route::planner) direction: Direction,
    pub(in crate::db::executor::planning::route::planner) capabilities: RouteCapabilities,
    pub(in crate::db::executor::planning::route::planner) support: RouteDerivationSupport,
    pub(in crate::db::executor::planning::route::planner) count_pushdown: RouteCountPushdownState,
    pub(in crate::db::executor::planning::route::planner) secondary_pushdown_applicability:
        PushdownApplicability,
    pub(in crate::db::executor::planning::route::planner) scan_hints: ScanHintPlan,
    pub(in crate::db::executor::planning::route::planner) top_n_seek_spec: Option<TopNSeekSpec>,
    pub(in crate::db::executor::planning::route::planner) aggregate_physical_fetch_hint:
        Option<usize>,
    pub(in crate::db::executor::planning::route::planner) aggregate_seek_spec:
        Option<AggregateSeekSpec>,
    pub(in crate::db::executor::planning::route::planner) grouped_execution_mode:
        Option<GroupedExecutionMode>,
}

///
/// RouteDerivationSupport
///
/// Bundles route-shape support bits that travel together through feasibility
/// and execution assembly. Keeps the main derivation context from carrying
/// several unrelated free-floating booleans.
///

pub(in crate::db::executor::planning::route::planner) struct RouteDerivationSupport {
    pub(in crate::db::executor::planning::route::planner) desc_physical_reverse_supported: bool,
    pub(in crate::db::executor::planning::route::planner) index_range_limit_pushdown_shape_supported:
        bool,
}

///
/// RouteCountPushdownState
///
/// Captures COUNT-specific shape support and final eligibility under one
/// boundary so planner stages can reason about COUNT routing without separate
/// boolean fields.
///

pub(in crate::db::executor::planning::route::planner) struct RouteCountPushdownState {
    pub(in crate::db::executor::planning::route::planner) existing_rows_shape_supported: bool,
    pub(in crate::db::executor::planning::route::planner) eligible: bool,
}

///
/// RouteIntentStage
///
/// Immutable route-intent normalization for staged route derivation.
/// Captures aggregate presence, canonical fast-path order, and materialization
/// forcing policy in one typed boundary.
///

pub(in crate::db::executor::planning::route::planner) struct RouteIntentStage<'a> {
    pub(in crate::db::executor::planning::route::planner) aggregate_shape:
        Option<AggregateRouteShape<'a>>,
    pub(in crate::db::executor::planning::route::planner) grouped: bool,
    pub(in crate::db::executor::planning::route::planner) route_shape_kind: RouteShapeKind,
    pub(in crate::db::executor::planning::route::planner) grouped_plan_strategy:
        Option<GroupedPlanStrategy>,
    pub(in crate::db::executor::planning::route::planner) fast_path_order:
        &'static [FastPathOrder],
    pub(in crate::db::executor::planning::route::planner) aggregate_force_materialized_due_to_predicate_uncertainty:
        bool,
}

impl RouteIntentStage<'_> {
    /// Return aggregate kind carried by this intent stage, if any.
    pub(in crate::db::executor::planning::route::planner) fn kind(&self) -> Option<AggregateKind> {
        self.aggregate_shape.map(AggregateRouteShape::kind)
    }
}

///
/// RouteFeasibilityStage
///
/// Immutable route feasibility derivation stage.
/// Captures continuation/window policy, capability snapshot, scan hints, and
/// index-range limit feasibility before execution-mode resolution.
///

pub(in crate::db::executor::planning::route::planner) struct RouteFeasibilityStage {
    pub(in crate::db::executor::planning::route::planner) continuation: RouteContinuationPlan,
    pub(in crate::db::executor::planning::route::planner) derivation: RouteDerivationContext,
    pub(in crate::db::executor::planning::route::planner) index_range_limit_spec:
        Option<IndexRangeLimitSpec>,
}

///
/// RouteExecutionStage
///
/// Immutable execution-mode stage derived from feasibility and intent.
/// Captures final execution mode, aggregate fold mode, and post-mode
/// index-range limit routing.
///

pub(in crate::db::executor::planning::route::planner) struct RouteExecutionStage {
    pub(in crate::db::executor::planning::route::planner) route_shape_kind: RouteShapeKind,
    pub(in crate::db::executor::planning::route::planner) execution_mode: RouteExecutionMode,
    pub(in crate::db::executor::planning::route::planner) aggregate_fold_mode: AggregateFoldMode,
    pub(in crate::db::executor::planning::route::planner) index_range_limit_spec:
        Option<IndexRangeLimitSpec>,
}

// Keep grouped route-plan assembly invariants local to the route stage owner
// so entrypoints only pass through already-derived staged values.
fn debug_assert_grouped_route_plan_alignment(
    intent_stage: &RouteIntentStage<'_>,
    derivation: &RouteDerivationContext,
) {
    debug_assert!(
        intent_stage.grouped == derivation.grouped_execution_mode.is_some(),
        "grouped route assembly must align grouped intent with grouped execution-mode projection",
    );
    if let Some(grouped_plan_strategy) = intent_stage.grouped_plan_strategy {
        debug_assert!(
            derivation.grouped_execution_mode
                == Some(GroupedExecutionMode::from_planner_strategy(
                    grouped_plan_strategy,
                    GroupedExecutionModeProjection::from_route_inputs(
                        derivation.direction,
                        derivation.support.desc_physical_reverse_supported,
                        derivation
                            .capabilities
                            .load_order_route_contract
                            .allows_ordered_group_projection(),
                    ),
                )),
            "grouped route assembly must not drift from the canonical grouped execution-mode projection",
        );
    }
}

// Assemble one immutable route plan from already-derived intent, feasibility,
// and execution stages. This owner only wires the decided stage contracts
// together; it does not select execution modes or route variants.
pub(in crate::db::executor::planning::route::planner) fn assemble_execution_route_plan(
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
    debug_assert_grouped_route_plan_alignment(&intent_stage, &derivation);

    ExecutionRoutePlan {
        direction: derivation.direction,
        route_shape_kind: execution_stage.route_shape_kind,
        continuation,
        execution_mode: execution_stage.execution_mode,
        desc_physical_reverse_supported: derivation.support.desc_physical_reverse_supported,
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

// Build one immutable route plan from already-derived intent and feasibility
// stages. Execution-stage selection remains execution-owned; this helper only
// sequences that stage handoff and then assembles the final route contract.
pub(in crate::db::executor::planning::route::planner) fn build_execution_route_plan_from_stages(
    intent_stage: RouteIntentStage<'_>,
    feasibility_stage: RouteFeasibilityStage,
    load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
) -> ExecutionRoutePlan {
    let execution_stage = derive_route_execution_stage(&intent_stage, &feasibility_stage);

    assemble_execution_route_plan(
        intent_stage,
        feasibility_stage,
        execution_stage,
        load_terminal_fast_path,
    )
}
