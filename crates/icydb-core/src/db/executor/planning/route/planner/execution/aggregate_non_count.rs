//! Module: executor::planning::route::planner::execution::aggregate_non_count
//! Responsibility: non-`COUNT` scalar aggregate execution-stage derivation.
//! Does not own: aggregate terminal execution or feasibility derivation.
//! Boundary: maps staged route facts into non-`COUNT` execution mode.

use crate::db::executor::{
    aggregate::AggregateFoldMode,
    route::{
        RouteExecutionMode, RouteShapeKind, aggregate_non_count_streaming_allowed,
        planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
    },
};

/// Derive the execution mode for non-`COUNT` scalar aggregate routes.
pub(super) const fn derive_execution_mode_for_aggregate_non_count(
    intent_stage: &RouteIntentStage<'_>,
    feasibility_stage: &RouteFeasibilityStage,
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteExecutionMode {
    let streaming_allowed = aggregate_non_count_streaming_allowed(
        intent_stage.aggregate_shape,
        feasibility_stage.derivation.capability_facts,
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

/// Build the execution stage for non-`COUNT` scalar aggregate routes.
pub(super) fn build_execution_stage_for_aggregate_non_count(
    intent_stage: &RouteIntentStage<'_>,
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
        crate::db::executor::planning::route::planner::execution::index_range_limit_spec_for_execution_mode(
            feasibility_stage,
            execution_mode,
        );
    debug_assert!(
        index_range_limit_spec.is_none() || matches!(execution_mode, RouteExecutionMode::Streaming),
        "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
    );

    RouteExecutionStage {
        route_shape_kind: RouteShapeKind::AggregateNonCount,
        execution_mode,
        aggregate_fold_mode: AggregateFoldMode::ExistingRows,
        index_range_limit_spec,
    }
}
