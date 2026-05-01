//! Module: db::executor::planning::route::planner::execution::load_scalar
//! Shapes executor plans for scalar load routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::{
    aggregate::AggregateFoldMode,
    route::load_streaming_allowed,
    route::{
        RouteExecutionMode, RouteShapeKind,
        planner::{RouteExecutionStage, RouteFeasibilityStage},
    },
};

pub(super) const fn derive_execution_mode_for_load(
    feasibility_stage: &RouteFeasibilityStage,
) -> RouteExecutionMode {
    if load_streaming_allowed(
        feasibility_stage.derivation.capabilities,
        feasibility_stage.index_range_limit_spec.is_some(),
    ) {
        RouteExecutionMode::Streaming
    } else {
        RouteExecutionMode::Materialized
    }
}

pub(super) const fn build_execution_stage_for_load(
    feasibility_stage: &RouteFeasibilityStage,
) -> RouteExecutionStage {
    // Load routes keep index-range limit contracts intact.
    RouteExecutionStage {
        route_shape_kind: RouteShapeKind::LoadScalar,
        execution_mode: derive_execution_mode_for_load(feasibility_stage),
        aggregate_fold_mode: AggregateFoldMode::ExistingRows,
        index_range_limit_spec: feasibility_stage.index_range_limit_spec,
    }
}
