use crate::{
    db::executor::{
        aggregate::AggregateFoldMode,
        load::LoadExecutor,
        route::{
            ExecutionModeRouteCase, RouteExecutionMode, RouteShapeKind,
            planner::{RouteExecutionStage, RouteFeasibilityStage},
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) const fn derive_execution_mode_for_aggregate_count(
        feasibility_stage: &RouteFeasibilityStage,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    ) -> RouteExecutionMode {
        if aggregate_force_materialized_due_to_predicate_uncertainty {
            RouteExecutionMode::Materialized
        } else if feasibility_stage.derivation.count_pushdown_eligible {
            RouteExecutionMode::Streaming
        } else {
            RouteExecutionMode::Materialized
        }
    }

    pub(super) fn build_execution_stage_for_aggregate_count(
        feasibility_stage: &RouteFeasibilityStage,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    ) -> RouteExecutionStage {
        // COUNT routes can stream. Index-range bounded pushdown remains route-gated.
        let execution_mode = Self::derive_execution_mode_for_aggregate_count(
            feasibility_stage,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        );
        let index_range_limit_spec =
            Self::index_range_limit_spec_for_execution_mode(feasibility_stage, execution_mode);
        let aggregate_fold_mode = if feasibility_stage
            .derivation
            .capabilities
            .count_pushdown_access_shape_supported
        {
            AggregateFoldMode::KeysOnly
        } else if feasibility_stage
            .derivation
            .capabilities
            .count_pushdown_existing_rows_shape_supported
        {
            AggregateFoldMode::ExistingRows
        } else {
            AggregateFoldMode::KeysOnly
        };
        debug_assert!(
            !matches!(execution_mode, RouteExecutionMode::Streaming)
                || matches!(
                    aggregate_fold_mode,
                    AggregateFoldMode::KeysOnly | AggregateFoldMode::ExistingRows
                ),
            "route invariant: streaming COUNT execution must select one supported fold mode",
        );

        RouteExecutionStage {
            route_shape_kind: RouteShapeKind::AggregateCount,
            execution_mode_case: ExecutionModeRouteCase::AggregateCount,
            execution_mode,
            aggregate_fold_mode,
            index_range_limit_spec,
        }
    }
}
