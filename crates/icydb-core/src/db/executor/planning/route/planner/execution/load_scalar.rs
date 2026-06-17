//! Module: executor::planning::route::planner::execution::load_scalar
//! Responsibility: scalar load execution-stage derivation.
//! Does not own: load execution or feasibility derivation.
//! Boundary: maps staged route facts into scalar load execution mode.

use crate::db::executor::{
    aggregate::AggregateFoldMode,
    route::{
        RouteExecutionMode, RouteShapeKind, load_streaming_allowed,
        planner::{RouteExecutionStage, RouteFeasibilityStage},
    },
};

/// Derive the execution mode for scalar load routes.
pub(super) const fn derive_execution_mode_for_load(
    feasibility_stage: &RouteFeasibilityStage,
) -> RouteExecutionMode {
    if load_streaming_allowed(
        feasibility_stage.derivation.capability_facts,
        feasibility_stage.index_range_limit_spec.is_some(),
    ) {
        RouteExecutionMode::Streaming
    } else {
        RouteExecutionMode::Materialized
    }
}

/// Build the execution stage for scalar load routes.
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
