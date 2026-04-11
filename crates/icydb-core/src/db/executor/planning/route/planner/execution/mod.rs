//! Module: executor::route::planner::execution
//! Responsibility: map staged intent + feasibility into execution mode.
//! Does not own: intent normalization or feasibility derivation.
//! Boundary: execution-stage derivation for route planning.

mod shape_aggregate_count;
mod shape_aggregate_grouped;
mod shape_aggregate_non_count;
mod shape_load;

use crate::db::executor::planning::route::{
    IndexRangeLimitSpec, RouteExecutionMode, RouteShapeKind,
    planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
};

use crate::db::executor::planning::route::planner::execution::{
    shape_aggregate_count::build_execution_stage_for_aggregate_count,
    shape_aggregate_grouped::build_execution_stage_for_aggregate_grouped,
    shape_aggregate_non_count::build_execution_stage_for_aggregate_non_count,
    shape_load::build_execution_stage_for_load,
};

pub(in crate::db::executor::planning::route::planner) const fn index_range_limit_spec_for_execution_mode(
    feasibility_stage: &RouteFeasibilityStage,
    execution_mode: RouteExecutionMode,
) -> Option<IndexRangeLimitSpec> {
    match execution_mode {
        RouteExecutionMode::Streaming => feasibility_stage.index_range_limit_spec,
        RouteExecutionMode::Materialized => None,
    }
}

fn debug_assert_non_mutation_shape(route_shape_kind: RouteShapeKind) {
    debug_assert!(
        !matches!(route_shape_kind, RouteShapeKind::MutationDelete),
        "route invariant: mutation route shape is not valid in scalar execution-stage derivation",
    );
}

fn aggregate_force_materialized_due_to_predicate_uncertainty(
    intent_stage: &RouteIntentStage<'_>,
) -> bool {
    let kind = intent_stage.kind();
    (kind.is_some() || intent_stage.grouped)
        && intent_stage.aggregate_force_materialized_due_to_predicate_uncertainty
}

fn debug_assert_probe_hint_contract(feasibility_stage: &RouteFeasibilityStage) {
    let keep_access_window = *feasibility_stage.continuation.keep_access_window();
    debug_assert!(
        feasibility_stage
            .derivation
            .capabilities
            .bounded_probe_hint_safe
            || feasibility_stage
                .derivation
                .aggregate_physical_fetch_hint
                .is_none()
            || keep_access_window.is_zero_window(),
        "route invariant: DISTINCT+offset must disable bounded aggregate probe hints",
    );
}

pub(in crate::db::executor::planning::route::planner) fn derive_route_execution_stage(
    intent_stage: &RouteIntentStage<'_>,
    feasibility_stage: &RouteFeasibilityStage,
) -> RouteExecutionStage {
    // Phase 1: derive shape and enforce scalar-route shape constraints.
    let route_shape_kind = intent_stage.route_shape_kind;
    debug_assert_non_mutation_shape(route_shape_kind);
    debug_assert_probe_hint_contract(feasibility_stage);

    // Phase 2: dispatch to one shape-local stage builder.
    let aggregate_force_materialized_due_to_predicate_uncertainty =
        aggregate_force_materialized_due_to_predicate_uncertainty(intent_stage);

    match route_shape_kind {
        RouteShapeKind::LoadScalar => build_execution_stage_for_load(feasibility_stage),
        RouteShapeKind::AggregateCount => build_execution_stage_for_aggregate_count(
            feasibility_stage,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        ),
        RouteShapeKind::AggregateNonCount => build_execution_stage_for_aggregate_non_count(
            intent_stage,
            feasibility_stage,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        ),
        RouteShapeKind::AggregateGrouped => {
            build_execution_stage_for_aggregate_grouped(intent_stage)
        }
        RouteShapeKind::MutationDelete => unreachable!(
            "route invariant: mutation route shape is not valid in scalar execution-stage derivation"
        ),
    }
}
