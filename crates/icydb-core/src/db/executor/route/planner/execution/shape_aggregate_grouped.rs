//! Module: db::executor::route::planner::execution::shape_aggregate_grouped
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::execution::shape_aggregate_grouped.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        aggregate::{AggregateFoldMode, AggregateKind},
        route::{
            ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
            planner::{RouteExecutionStage, RouteIntentStage},
        },
        shared::load_contracts::LoadExecutor,
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) fn build_execution_stage_for_aggregate_grouped(
        intent_stage: &RouteIntentStage,
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
            execution_mode_case: ExecutionModeRouteCase::AggregateGrouped,
            execution_mode: RouteExecutionMode::Materialized,
            aggregate_fold_mode,
            index_range_limit_spec: None,
        }
    }
}
