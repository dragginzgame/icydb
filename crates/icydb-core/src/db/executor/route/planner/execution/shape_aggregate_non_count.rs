//! Module: db::executor::route::planner::execution::shape_aggregate_non_count
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::execution::shape_aggregate_non_count.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::AggregateFoldMode,
    route::aggregate_non_count_streaming_allowed,
    route::{
        ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
        planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
    },
};

pub(in crate::db::executor::route::planner) fn derive_execution_mode_for_aggregate_non_count(
    intent_stage: &RouteIntentStage,
    feasibility_stage: &RouteFeasibilityStage,
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteExecutionMode {
    let streaming_allowed = aggregate_non_count_streaming_allowed(
        intent_stage.aggregate_expr.as_ref(),
        feasibility_stage.derivation.capabilities,
        feasibility_stage
            .derivation
            .secondary_pushdown_applicability
            .is_eligible(),
        feasibility_stage.index_range_limit_spec.is_some(),
    );

    match (
        aggregate_force_materialized_due_to_predicate_uncertainty,
        streaming_allowed,
    ) {
        (true, _) | (_, false) => RouteExecutionMode::Materialized,
        (false, true) => RouteExecutionMode::Streaming,
    }
}

pub(in crate::db::executor::route::planner) fn build_execution_stage_for_aggregate_non_count(
    intent_stage: &RouteIntentStage,
    feasibility_stage: &RouteFeasibilityStage,
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteExecutionStage {
    // Non-count scalar aggregates preserve index-range pushdown only for streaming execution.
    let execution_mode = derive_execution_mode_for_aggregate_non_count(
        intent_stage,
        feasibility_stage,
        aggregate_force_materialized_due_to_predicate_uncertainty,
    );
    let index_range_limit_spec =
        crate::db::executor::route::planner::execution::index_range_limit_spec_for_execution_mode(
            feasibility_stage,
            execution_mode,
        );
    debug_assert!(
        index_range_limit_spec.is_none() || matches!(execution_mode, RouteExecutionMode::Streaming),
        "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
    );

    RouteExecutionStage {
        route_shape_kind: RouteShapeKind::AggregateNonCount,
        execution_mode_case: ExecutionModeRouteCase::AggregateNonCount,
        execution_mode,
        aggregate_fold_mode: AggregateFoldMode::ExistingRows,
        index_range_limit_spec,
    }
}
