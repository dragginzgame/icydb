//! Module: executor::route::planner::stages
//! Responsibility: immutable staged route-planning contracts shared by planner phases.
//! Does not own: stage derivation orchestration or route execution assembly.
//! Boundary: exposes the typed stage bundles consumed by planner entrypoint, intent, feasibility, and execution modules.

use crate::db::{
    access::PushdownApplicability,
    direction::Direction,
    executor::{
        aggregate::{AggregateFoldMode, AggregateKind},
        route::{
            AggregateSeekSpec, ExecutionModeRouteCase, FastPathOrder, GroupedExecutionStrategy,
            IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan, RouteExecutionMode,
            RouteShapeKind, ScanHintPlan, TopNSeekSpec,
        },
    },
    query::{builder::AggregateExpr, plan::GroupedPlanStrategyHint},
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
    pub(in crate::db::executor::route::planner) route_shape_kind: RouteShapeKind,
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
