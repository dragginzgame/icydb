//! Module: executor::load::grouped_route
//! Responsibility: grouped route-stage derivation and layout invariant checks.
//! Does not own: grouped stream folding or grouped output materialization.
//! Boundary: planner handoff extraction + route observability normalization.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace,
            load::{
                GroupedExecutionContext, GroupedPlannerPayload, GroupedRoutePayload,
                GroupedRouteStage, IndexSpecBundle, LoadExecutor,
            },
            plan_metrics::GroupedPlanMetricsStrategy,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Map route-owned grouped strategy labels into grouped plan-metrics labels.
    pub(in crate::db::executor) const fn grouped_plan_metrics_strategy_for_execution_strategy(
        grouped_execution_strategy: crate::db::executor::route::GroupedExecutionStrategy,
    ) -> GroupedPlanMetricsStrategy {
        match grouped_execution_strategy {
            crate::db::executor::route::GroupedExecutionStrategy::HashMaterialized => {
                GroupedPlanMetricsStrategy::HashMaterialized
            }
            crate::db::executor::route::GroupedExecutionStrategy::OrderedMaterialized => {
                GroupedPlanMetricsStrategy::OrderedMaterialized
            }
        }
    }

    // Resolve grouped handoff/route metadata into one grouped route-stage payload.
    pub(super) fn resolve_grouped_route(
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
        debug: bool,
    ) -> Result<GroupedRouteStage<E>, InternalError> {
        let grouped_handoff = plan.grouped_handoff()?;
        if let Some(reason) = grouped_handoff.distinct_policy_violation_for_executor() {
            return Err(super::invariant(reason.invariant_message()));
        }
        let grouped_execution = grouped_handoff.execution();
        let group_fields = grouped_handoff.group_fields().to_vec();
        let grouped_aggregate_exprs = grouped_handoff.aggregate_exprs().to_vec();
        let projection_layout = grouped_handoff.projection_layout().clone();
        debug_assert!(
            grouped_handoff.projection_layout_valid(),
            "planner grouped projection layout invariants must hold at executor boundary",
        );
        let grouped_distinct_execution_strategy =
            grouped_handoff.distinct_execution_strategy().clone();
        let grouped_having = grouped_handoff.having().cloned();
        let grouped_route_plan =
            Self::build_execution_route_plan_for_grouped_handoff(grouped_handoff);
        let grouped_route_observability =
            grouped_route_plan.grouped_observability().ok_or_else(|| {
                super::invariant("grouped route planning must emit grouped observability payload")
            })?;
        let grouped_route_outcome = grouped_route_observability.outcome();
        let grouped_route_rejection_reason = grouped_route_observability.rejection_reason();
        let grouped_route_eligible = grouped_route_observability.eligible();
        let grouped_route_execution_mode = grouped_route_observability.execution_mode();
        let grouped_plan_metrics_strategy =
            Self::grouped_plan_metrics_strategy_for_execution_strategy(
                grouped_route_observability.grouped_execution_strategy(),
            );
        debug_assert!(
            grouped_route_eligible == grouped_route_rejection_reason.is_none(),
            "grouped route eligibility and rejection reason must stay aligned",
        );
        debug_assert!(
            grouped_route_outcome
                != crate::db::executor::route::GroupedRouteDecisionOutcome::Rejected
                || grouped_route_rejection_reason.is_some(),
            "grouped rejected outcomes must carry a rejection reason",
        );
        debug_assert!(
            matches!(
                grouped_route_execution_mode,
                crate::db::executor::route::ExecutionMode::Materialized
            ),
            "grouped execution must remain materialized",
        );

        let direction = grouped_route_plan.direction();
        let continuation_applied = !cursor.is_empty();
        let execution_trace =
            debug.then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let continuation_signature = plan.continuation_signature_for_runtime()?;
        let continuation_boundary_arity = plan.grouped_cursor_boundary_arity()?;
        let grouped_continuation_window = plan.grouped_continuation_window(&cursor)?;
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let plan = plan.into_inner();

        Ok(GroupedRouteStage {
            planner_payload: GroupedPlannerPayload {
                plan,
                grouped_execution,
                group_fields,
                grouped_aggregate_exprs,
                projection_layout,
                grouped_having,
                grouped_distinct_execution_strategy,
            },
            route_payload: GroupedRoutePayload { grouped_route_plan },
            index_specs: IndexSpecBundle {
                index_prefix_specs,
                index_range_specs,
            },
            execution_context: GroupedExecutionContext {
                direction,
                continuation_signature,
                continuation_boundary_arity,
                grouped_continuation_window,
                grouped_plan_metrics_strategy,
                execution_trace,
            },
        })
    }
}
