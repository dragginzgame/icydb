//! Module: executor::route::planner::execution
//! Responsibility: map staged intent + feasibility into execution mode.
//! Does not own: intent normalization or feasibility derivation.
//! Boundary: execution-stage derivation for route planning.

mod shape_aggregate_count;
mod shape_aggregate_grouped;
mod shape_aggregate_non_count;
mod shape_load;

use crate::{
    db::executor::{
        aggregate::AggregateKind,
        load::LoadExecutor,
        route::{
            RouteShapeKind,
            planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    fn derive_route_shape_kind(intent_stage: &RouteIntentStage) -> RouteShapeKind {
        let kind = intent_stage.kind();

        if intent_stage.grouped {
            RouteShapeKind::AggregateGrouped
        } else if kind.is_some_and(AggregateKind::is_count) {
            RouteShapeKind::AggregateCount
        } else {
            kind.map_or(RouteShapeKind::LoadScalar, |_| {
                RouteShapeKind::AggregateNonCount
            })
        }
    }

    fn debug_assert_non_mutation_shape(route_shape_kind: RouteShapeKind) {
        debug_assert!(
            !matches!(route_shape_kind, RouteShapeKind::MutationDelete),
            "route invariant: mutation route shape is not valid in scalar execution-stage derivation",
        );
    }

    fn aggregate_force_materialized_due_to_predicate_uncertainty(
        intent_stage: &RouteIntentStage,
    ) -> bool {
        let kind = intent_stage.kind();
        (kind.is_some() || intent_stage.grouped)
            && intent_stage.aggregate_force_materialized_due_to_predicate_uncertainty
    }

    fn debug_assert_probe_hint_contract(feasibility_stage: &RouteFeasibilityStage) {
        debug_assert!(
            feasibility_stage
                .derivation
                .capabilities
                .bounded_probe_hint_safe
                || feasibility_stage
                    .derivation
                    .aggregate_physical_fetch_hint
                    .is_none()
                || feasibility_stage.page_limit_is_zero,
            "route invariant: DISTINCT+offset must disable bounded aggregate probe hints",
        );
    }

    pub(in crate::db::executor::route::planner) fn derive_route_execution_stage(
        intent_stage: &RouteIntentStage,
        feasibility_stage: &RouteFeasibilityStage,
    ) -> RouteExecutionStage {
        // Phase 1: derive shape and enforce scalar-route shape constraints.
        let route_shape_kind = Self::derive_route_shape_kind(intent_stage);
        Self::debug_assert_non_mutation_shape(route_shape_kind);
        Self::debug_assert_probe_hint_contract(feasibility_stage);

        // Phase 2: dispatch to one shape-local stage builder.
        let aggregate_force_materialized_due_to_predicate_uncertainty =
            Self::aggregate_force_materialized_due_to_predicate_uncertainty(intent_stage);

        match route_shape_kind {
            RouteShapeKind::LoadScalar => Self::build_execution_stage_for_load(feasibility_stage),
            RouteShapeKind::AggregateCount => Self::build_execution_stage_for_aggregate_count(
                feasibility_stage,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            ),
            RouteShapeKind::AggregateNonCount => {
                Self::build_execution_stage_for_aggregate_non_count(
                    intent_stage,
                    feasibility_stage,
                    aggregate_force_materialized_due_to_predicate_uncertainty,
                )
            }
            RouteShapeKind::AggregateGrouped => {
                Self::build_execution_stage_for_aggregate_grouped(intent_stage)
            }
            RouteShapeKind::MutationDelete => unreachable!(
                "route invariant: mutation route shape is not valid in scalar execution-stage derivation"
            ),
        }
    }
}
