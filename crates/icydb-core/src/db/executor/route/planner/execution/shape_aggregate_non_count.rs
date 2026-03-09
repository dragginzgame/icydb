use crate::{
    db::executor::{
        aggregate::AggregateFoldMode,
        load::LoadExecutor,
        route::{
            ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
            planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) fn derive_execution_mode_for_aggregate_non_count(
        intent_stage: &RouteIntentStage,
        feasibility_stage: &RouteFeasibilityStage,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    ) -> RouteExecutionMode {
        let aggregate_non_count_streaming_allowed = Self::aggregate_non_count_streaming_allowed(
            intent_stage.aggregate_expr.as_ref(),
            feasibility_stage.derivation.capabilities,
            feasibility_stage
                .derivation
                .secondary_pushdown_applicability
                .is_eligible(),
            feasibility_stage.index_range_limit_spec.is_some(),
        );

        match (
            aggregate_force_materialized_due_to_predicate_uncertainty,
            aggregate_non_count_streaming_allowed,
        ) {
            (true, _) | (_, false) => RouteExecutionMode::Materialized,
            (false, true) => RouteExecutionMode::Streaming,
        }
    }

    pub(super) fn build_execution_stage_for_aggregate_non_count(
        intent_stage: &RouteIntentStage,
        feasibility_stage: &RouteFeasibilityStage,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    ) -> RouteExecutionStage {
        // Non-count scalar aggregates preserve index-range pushdown only for streaming execution.
        let execution_mode = Self::derive_execution_mode_for_aggregate_non_count(
            intent_stage,
            feasibility_stage,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        );
        let index_range_limit_spec =
            Self::index_range_limit_spec_for_execution_mode(feasibility_stage, execution_mode);
        debug_assert!(
            index_range_limit_spec.is_none()
                || matches!(execution_mode, RouteExecutionMode::Streaming),
            "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
        );

        RouteExecutionStage {
            route_shape_kind: RouteShapeKind::AggregateNonCount,
            execution_mode_case: ExecutionModeRouteCase::AggregateNonCount,
            execution_mode,
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
            index_range_limit_spec,
        }
    }
}
