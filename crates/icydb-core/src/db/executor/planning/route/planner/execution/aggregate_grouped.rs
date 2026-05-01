//! Module: db::executor::planning::route::planner::execution::aggregate_grouped
//! Shapes executor plans for grouped aggregate routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::{AggregateFoldMode, AggregateKind},
    route::{
        RouteExecutionMode, RouteShapeKind,
        planner::{RouteExecutionStage, RouteIntentStage},
    },
};

pub(super) fn build_execution_stage_for_aggregate_grouped(
    intent_stage: &RouteIntentStage<'_>,
) -> RouteExecutionStage {
    debug_assert!(
        intent_stage.grouped,
        "route invariant: grouped execution shape builder requires grouped intent stage",
    );
    // Grouped aggregate routes are always materialized at this boundary.
    let aggregate_fold_mode = if intent_stage.kind().is_some_and(AggregateKind::is_count) {
        AggregateFoldMode::KeysOnly
    } else {
        AggregateFoldMode::ExistingRows
    };

    RouteExecutionStage {
        route_shape_kind: RouteShapeKind::AggregateGrouped,
        execution_mode: RouteExecutionMode::Materialized,
        aggregate_fold_mode,
        index_range_limit_spec: None,
    }
}
