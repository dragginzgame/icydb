//! Module: executor::planning::route::planner::execution::aggregate_grouped
//! Responsibility: grouped aggregate execution-stage derivation.
//! Does not own: grouped aggregate execution or feasibility derivation.
//! Boundary: maps grouped intent into the materialized grouped execution stage.

use crate::db::executor::route::{
    RouteExecutionMode,
    planner::{RouteExecutionStage, RouteIntentStage},
};
#[cfg(feature = "sql-explain")]
use crate::db::executor::{
    aggregate::{AggregateFoldMode, AggregateKind},
    route::RouteShapeKind,
};

/// Build the execution stage for grouped aggregate routes.
pub(super) fn build_execution_stage_for_aggregate_grouped(
    intent_stage: &RouteIntentStage<'_>,
) -> RouteExecutionStage {
    debug_assert!(
        intent_stage.grouped,
        "route invariant: grouped execution shape builder requires grouped intent stage",
    );
    // Grouped aggregate routes are always materialized at this boundary.
    #[cfg(feature = "sql-explain")]
    let aggregate_fold_mode = if intent_stage.kind().is_some_and(AggregateKind::is_count) {
        AggregateFoldMode::KeysOnly
    } else {
        AggregateFoldMode::ExistingRows
    };

    RouteExecutionStage {
        #[cfg(feature = "sql-explain")]
        route_shape_kind: RouteShapeKind::AggregateGrouped,
        execution_mode: RouteExecutionMode::Materialized,
        #[cfg(feature = "sql-explain")]
        aggregate_fold_mode,
        index_range_limit_spec: None,
    }
}
