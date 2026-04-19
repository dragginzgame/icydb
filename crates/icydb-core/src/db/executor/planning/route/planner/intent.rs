//! Module: db::executor::planning::route::planner::intent
//! Responsibility: normalize route intent into canonical staged intent state.
//! Does not own: feasibility or execution-mode derivation.
//! Boundary: pure intent derivation for staged route planning.

use crate::{
    db::executor::route::{
        AGGREGATE_FAST_PATH_ORDER, GROUPED_AGGREGATE_FAST_PATH_ORDER, LOAD_FAST_PATH_ORDER,
        RouteIntent, RouteShapeKind,
        aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation,
        planner::RouteIntentStage,
    },
    db::{
        executor::ExecutionPreparation,
        query::plan::{AccessPlannedQuery, AggregateKind, GroupedPlanStrategy},
    },
    error::InternalError,
};

// Keep route-shape intent mapping in one helper so grouped/count/non-count
// classification cannot drift between stage construction and invariant checks.
const fn route_shape_kind_for_intent(grouped: bool, kind: Option<AggregateKind>) -> RouteShapeKind {
    match (grouped, kind) {
        (true, _) => RouteShapeKind::AggregateGrouped,
        (false, Some(aggregate_kind)) if aggregate_kind.is_count() => {
            RouteShapeKind::AggregateCount
        }
        (false, Some(_)) => RouteShapeKind::AggregateNonCount,
        (false, None) => RouteShapeKind::LoadScalar,
    }
}

pub(in crate::db::executor::planning::route::planner) fn derive_route_intent_stage(
    intent: RouteIntent<'_>,
) -> RouteIntentStage<'_> {
    let stage = match intent {
        RouteIntent::Load => RouteIntentStage {
            aggregate_shape: None,
            grouped: false,
            route_shape_kind: route_shape_kind_for_intent(false, None),
            grouped_plan_strategy: None,
            fast_path_order: &LOAD_FAST_PATH_ORDER,
            aggregate_force_materialized_due_to_predicate_uncertainty: false,
        },
        RouteIntent::Aggregate {
            aggregate,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        } => {
            let aggregate_kind = aggregate.kind();
            RouteIntentStage {
                aggregate_shape: Some(aggregate),
                grouped: false,
                route_shape_kind: route_shape_kind_for_intent(false, Some(aggregate_kind)),
                grouped_plan_strategy: None,
                fast_path_order: &AGGREGATE_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            }
        }
        RouteIntent::AggregateGrouped {
            grouped_plan_strategy,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        } => RouteIntentStage {
            aggregate_shape: None,
            grouped: true,
            route_shape_kind: route_shape_kind_for_intent(true, None),
            grouped_plan_strategy: Some(grouped_plan_strategy),
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
        !stage.grouped || stage.aggregate_shape.is_none() && stage.fast_path_order.is_empty(),
        "route invariant: grouped intent must not carry scalar aggregate specs or fast-path routes",
    );
    let expected_route_shape_kind = route_shape_kind_for_intent(stage.grouped, stage.kind());
    debug_assert!(
        stage.route_shape_kind == expected_route_shape_kind,
        "route invariant: route intent shape kind must remain aligned with grouped + aggregate intent",
    );
    debug_assert!(
        stage.grouped == stage.grouped_plan_strategy.is_some(),
        "route invariant: grouped intents must carry planner grouped strategies, scalar intents must not",
    );

    stage
}

// Derive the canonical staged load intent without exposing RouteIntent wiring
// at route entrypoints that only need the load shape contract.
pub(in crate::db::executor::planning::route::planner) fn derive_load_route_intent_stage()
-> RouteIntentStage<'static> {
    derive_route_intent_stage(RouteIntent::Load)
}

// Derive the canonical staged aggregate intent, including the one
// preparation-owned materialization forcing policy input.
pub(in crate::db::executor::planning::route::planner) fn derive_aggregate_route_intent_stage<'a>(
    aggregate: crate::db::executor::route::AggregateRouteShape<'a>,
    execution_preparation: &ExecutionPreparation,
) -> RouteIntentStage<'a> {
    derive_route_intent_stage(RouteIntent::Aggregate {
        aggregate,
        aggregate_force_materialized_due_to_predicate_uncertainty:
            aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                execution_preparation,
            ),
    })
}

// Derive the canonical staged grouped-aggregate intent from planner strategy
// plus the same preparation-owned materialization forcing policy contract.
pub(in crate::db::executor::planning::route::planner) fn derive_grouped_route_intent_stage(
    grouped_plan_strategy: GroupedPlanStrategy,
    execution_preparation: &ExecutionPreparation,
) -> RouteIntentStage<'static> {
    derive_route_intent_stage(RouteIntent::AggregateGrouped {
        grouped_plan_strategy,
        aggregate_force_materialized_due_to_predicate_uncertainty:
            aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                execution_preparation,
            ),
    })
}

// Mutation routing only accepts delete-mode scalar plans. Keep that admission
// decision under the route-intent owner instead of re-encoding it inline at
// entrypoint call sites.
pub(in crate::db::executor::planning::route::planner) fn ensure_mutation_route_plan_is_delete(
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    if plan.scalar_plan().mode.is_delete() {
        return Ok(());
    }

    Err(InternalError::query_executor_invariant(
        "mutation route planning requires delete plans",
    ))
}
