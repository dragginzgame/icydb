//! Module: executor::route::planner::intent
//! Responsibility: normalize route intent into canonical staged intent state.
//! Does not own: feasibility or execution-mode derivation.
//! Boundary: pure intent derivation for staged route planning.

use crate::{
    db::executor::{
        load::LoadExecutor,
        route::{
            AGGREGATE_FAST_PATH_ORDER, GROUPED_AGGREGATE_FAST_PATH_ORDER, LOAD_FAST_PATH_ORDER,
            RouteIntent, planner::RouteIntentStage,
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route::planner) fn derive_route_intent_stage(
        intent: RouteIntent,
    ) -> RouteIntentStage {
        let stage = match intent {
            RouteIntent::Load => RouteIntentStage {
                aggregate_spec: None,
                grouped: false,
                fast_path_order: &LOAD_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty: false,
            },
            RouteIntent::Aggregate {
                spec,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            } => RouteIntentStage {
                aggregate_spec: Some(spec),
                grouped: false,
                fast_path_order: &AGGREGATE_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            },
            RouteIntent::AggregateGrouped {
                aggregate_force_materialized_due_to_predicate_uncertainty,
            } => RouteIntentStage {
                aggregate_spec: None,
                grouped: true,
                fast_path_order: &GROUPED_AGGREGATE_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            },
        };
        let kind = stage.kind();
        debug_assert!(
            (kind.is_none()
                && !stage.grouped
                && stage.fast_path_order == LOAD_FAST_PATH_ORDER.as_slice())
                || (kind.is_some()
                    && !stage.grouped
                    && stage.fast_path_order == AGGREGATE_FAST_PATH_ORDER.as_slice())
                || (kind.is_none()
                    && stage.grouped
                    && stage.fast_path_order == GROUPED_AGGREGATE_FAST_PATH_ORDER.as_slice()),
            "route invariant: route intent must map to the canonical fast-path order contract",
        );
        debug_assert!(
            !stage.grouped || stage.aggregate_spec.is_none() && stage.fast_path_order.is_empty(),
            "route invariant: grouped intent must not carry scalar aggregate specs or fast-path routes",
        );

        stage
    }
}
