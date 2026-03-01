//! Module: executor::route::planner::execution
//! Responsibility: map staged intent + feasibility into execution mode.
//! Does not own: intent normalization or feasibility derivation.
//! Boundary: execution-stage derivation for route planning.

use crate::{
    db::executor::{
        aggregate::{AggregateFoldMode, AggregateKind},
        load::LoadExecutor,
        route::{
            ExecutionMode, ExecutionModeRouteCase,
            planner::{RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage},
        },
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route::planner) fn derive_route_execution_stage(
        intent_stage: &RouteIntentStage,
        feasibility_stage: &RouteFeasibilityStage,
    ) -> RouteExecutionStage {
        let kind = intent_stage.kind();
        let aggregate_force_materialized_due_to_predicate_uncertainty = (kind.is_some()
            || intent_stage.grouped)
            && intent_stage.aggregate_force_materialized_due_to_predicate_uncertainty;
        let count_terminal = kind.is_some_and(AggregateKind::is_count);
        let execution_case = if intent_stage.grouped {
            ExecutionModeRouteCase::AggregateGrouped
        } else if kind.is_some_and(AggregateKind::is_count) {
            ExecutionModeRouteCase::AggregateCount
        } else {
            kind.map_or(ExecutionModeRouteCase::Load, |_| {
                ExecutionModeRouteCase::AggregateNonCount
            })
        };
        let execution_mode = match execution_case {
            ExecutionModeRouteCase::Load => {
                if Self::load_streaming_allowed(
                    feasibility_stage.derivation.capabilities,
                    feasibility_stage.index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if feasibility_stage.derivation.count_pushdown_eligible {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateNonCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if Self::aggregate_non_count_streaming_allowed(
                    intent_stage.aggregate_spec.as_ref(),
                    feasibility_stage.derivation.capabilities,
                    feasibility_stage
                        .derivation
                        .secondary_pushdown_applicability
                        .is_eligible(),
                    feasibility_stage.index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateGrouped => ExecutionMode::Materialized,
        };
        let index_range_limit_spec = if (kind.is_some() || intent_stage.grouped)
            && matches!(execution_mode, ExecutionMode::Materialized)
        {
            None
        } else {
            feasibility_stage.index_range_limit_spec
        };

        debug_assert!(
            (kind.is_none() && !intent_stage.grouped)
                || index_range_limit_spec.is_none()
                || matches!(execution_mode, ExecutionMode::Streaming),
            "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
        );
        debug_assert!(
            !count_terminal || index_range_limit_spec.is_none(),
            "route invariant: COUNT terminals must not route through index-range limit pushdown",
        );
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

        let aggregate_fold_mode = if count_terminal {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };

        RouteExecutionStage {
            execution_mode_case: execution_case,
            execution_mode,
            aggregate_fold_mode,
            index_range_limit_spec,
        }
    }
}
