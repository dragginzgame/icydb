//! Module: db::executor::planning::route::planner::intent
//! Responsibility: normalize route intent into canonical staged intent state.
//! Does not own: feasibility or execution-mode derivation.
//! Boundary: pure intent derivation for staged route planning.

use crate::{
    db::executor::planning::route::contracts::MUTATION_FAST_PATH_ORDER,
    db::executor::route::{
        AGGREGATE_FAST_PATH_ORDER, AggregateRouteShape, FastPathOrder,
        GROUPED_AGGREGATE_FAST_PATH_ORDER, LOAD_FAST_PATH_ORDER, RouteShapeKind,
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

// Build the canonical intent-stage record from one already-selected route
// family. Keeping the invariant checks here preserves one route-shape authority
// while avoiding a second private enum that mirrors RoutePlanRequest.
fn route_intent_stage<'a>(
    aggregate_shape: Option<AggregateRouteShape<'a>>,
    grouped_plan_strategy: Option<GroupedPlanStrategy>,
    route_shape_kind: RouteShapeKind,
    fast_path_order: &'static [FastPathOrder],
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
) -> RouteIntentStage<'a> {
    let stage = RouteIntentStage {
        aggregate_shape,
        grouped: grouped_plan_strategy.is_some(),
        route_shape_kind,
        grouped_plan_strategy,
        fast_path_order,
        aggregate_force_materialized_due_to_predicate_uncertainty,
    };
    let kind = stage.kind();
    debug_assert!(
        (kind.is_none()
            && !stage.grouped
            && stage.fast_path_order == LOAD_FAST_PATH_ORDER.as_slice())
            || (matches!(stage.route_shape_kind, RouteShapeKind::MutationDelete)
                && kind.is_none()
                && !stage.grouped
                && stage.fast_path_order == MUTATION_FAST_PATH_ORDER.as_slice())
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
    if !matches!(stage.route_shape_kind, RouteShapeKind::MutationDelete) {
        let expected_route_shape_kind = route_shape_kind_for_intent(stage.grouped, stage.kind());
        debug_assert!(
            stage.route_shape_kind == expected_route_shape_kind,
            "route invariant: route intent shape kind must remain aligned with grouped + aggregate intent",
        );
    }
    debug_assert!(
        stage.grouped == stage.grouped_plan_strategy.is_some(),
        "route invariant: grouped intents must carry planner grouped strategies, scalar intents must not",
    );

    stage
}

// Derive the canonical staged load intent at route entrypoints that only need
// the load shape contract.
pub(super) fn derive_load_route_intent_stage() -> RouteIntentStage<'static> {
    route_intent_stage(
        None,
        None,
        route_shape_kind_for_intent(false, None),
        &LOAD_FAST_PATH_ORDER,
        false,
    )
}

// Derive the canonical staged aggregate intent, including the one
// preparation-owned materialization forcing policy input.
pub(super) fn derive_aggregate_route_intent_stage<'a>(
    aggregate: AggregateRouteShape<'a>,
    execution_preparation: &ExecutionPreparation,
) -> RouteIntentStage<'a> {
    route_intent_stage(
        Some(aggregate),
        None,
        route_shape_kind_for_intent(false, Some(aggregate.kind())),
        &AGGREGATE_FAST_PATH_ORDER,
        aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
            execution_preparation,
        ),
    )
}

// Derive the canonical staged grouped-aggregate intent from planner strategy
// plus the same preparation-owned materialization forcing policy contract.
pub(super) fn derive_grouped_route_intent_stage(
    grouped_plan_strategy: GroupedPlanStrategy,
    execution_preparation: &ExecutionPreparation,
) -> RouteIntentStage<'static> {
    route_intent_stage(
        None,
        Some(grouped_plan_strategy),
        route_shape_kind_for_intent(true, None),
        &GROUPED_AGGREGATE_FAST_PATH_ORDER,
        aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
            execution_preparation,
        ),
    )
}

// Derive the canonical staged mutation intent once delete-only admission has
// been validated at the route-intent boundary.
pub(super) fn derive_mutation_route_intent_stage(
    plan: &AccessPlannedQuery,
) -> Result<RouteIntentStage<'static>, InternalError> {
    if !plan.scalar_plan().mode.is_delete() {
        return Err(InternalError::query_executor_invariant(
            "mutation route planning requires delete plans",
        ));
    }

    Ok(route_intent_stage(
        None,
        None,
        RouteShapeKind::MutationDelete,
        &MUTATION_FAST_PATH_ORDER,
        false,
    ))
}
