//! Module: db::executor::route::planner::execution::shape_load
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::execution::shape_load.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        aggregate::AggregateFoldMode,
        route::{
            ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
            planner::{RouteExecutionStage, RouteFeasibilityStage},
        },
        shared::load_contracts::LoadExecutor,
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) const fn derive_execution_mode_for_load(
        feasibility_stage: &RouteFeasibilityStage,
    ) -> RouteExecutionMode {
        if Self::load_streaming_allowed(
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
            execution_mode_case: ExecutionModeRouteCase::Load,
            execution_mode: Self::derive_execution_mode_for_load(feasibility_stage),
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
            index_range_limit_spec: feasibility_stage.index_range_limit_spec,
        }
    }
}
