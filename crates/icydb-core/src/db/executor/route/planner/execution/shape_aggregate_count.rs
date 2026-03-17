//! Module: db::executor::route::planner::execution::shape_aggregate_count
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::execution::shape_aggregate_count.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::AggregateFoldMode,
    route::{
        ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
        planner::{RouteExecutionStage, RouteFeasibilityStage},
    },
};

pub(in crate::db::executor::route::planner) const fn derive_execution_mode_for_aggregate_count(
    feasibility_stage: &RouteFeasibilityStage,
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteExecutionMode {
    match (
        aggregate_force_materialized_due_to_predicate_uncertainty,
        feasibility_stage.derivation.count_pushdown_eligible,
    ) {
        (true, _) | (_, false) => RouteExecutionMode::Materialized,
        (false, true) => RouteExecutionMode::Streaming,
    }
}

pub(in crate::db::executor::route::planner) fn build_execution_stage_for_aggregate_count(
    feasibility_stage: &RouteFeasibilityStage,
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteExecutionStage {
    // COUNT routes can stream. Index-range bounded pushdown remains route-gated.
    let execution_mode = derive_execution_mode_for_aggregate_count(
        feasibility_stage,
        aggregate_force_materialized_due_to_predicate_uncertainty,
    );
    let index_range_limit_spec =
        crate::db::executor::route::planner::execution::index_range_limit_spec_for_execution_mode(
            feasibility_stage,
            execution_mode,
        );
    let aggregate_fold_mode = match (
        feasibility_stage
            .derivation
            .capabilities
            .count_pushdown_shape_supported,
        feasibility_stage
            .derivation
            .capabilities
            .count_pushdown_existing_rows_shape_supported,
    ) {
        (true, _) | (false, false) => AggregateFoldMode::KeysOnly,
        (false, true) => AggregateFoldMode::ExistingRows,
    };
    debug_assert!(
        !matches!(execution_mode, RouteExecutionMode::Streaming)
            || matches!(
                aggregate_fold_mode,
                AggregateFoldMode::KeysOnly | AggregateFoldMode::ExistingRows
            ),
        "route invariant: streaming COUNT execution must select one supported fold mode",
    );

    RouteExecutionStage {
        route_shape_kind: RouteShapeKind::AggregateCount,
        execution_mode_case: ExecutionModeRouteCase::AggregateCount,
        execution_mode,
        aggregate_fold_mode,
        index_range_limit_spec,
    }
}
