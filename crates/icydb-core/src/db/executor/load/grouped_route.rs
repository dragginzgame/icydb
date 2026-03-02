//! Module: executor::load::grouped_route
//! Responsibility: grouped route-stage derivation and layout invariant checks.
//! Does not own: grouped stream folding or grouped output materialization.
//! Boundary: planner handoff extraction + route observability normalization.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutablePlan, ExecutionTrace,
            load::{GroupedRouteStage, LoadExecutor},
            plan_metrics::GroupedPlanMetricsStrategy,
            validate_executor_plan,
        },
        query::plan::grouped_executor_handoff,
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
        validate_executor_plan::<E>(plan.as_inner())?;
        let grouped_handoff = grouped_executor_handoff(plan.as_inner())?;
        let grouped_execution = grouped_handoff.execution();
        let group_fields = grouped_handoff.group_fields().to_vec();
        let grouped_aggregate_exprs = grouped_handoff.aggregate_exprs().to_vec();
        let projection_layout = grouped_handoff.projection_layout().clone();
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
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let plan = plan.into_inner();

        Ok(GroupedRouteStage {
            plan,
            cursor,
            direction,
            continuation_signature,
            index_prefix_specs,
            index_range_specs,
            grouped_execution,
            group_fields,
            grouped_aggregate_exprs,
            projection_layout,
            grouped_having,
            grouped_route_plan,
            grouped_plan_metrics_strategy,
            grouped_distinct_execution_strategy,
            execution_trace,
        })
    }

    // Validate planner-provided grouped projection layout against grouped handoff vectors.
    pub(super) fn ensure_grouped_projection_layout_matches_handoff(
        route: &GroupedRouteStage<E>,
    ) -> Result<(), InternalError> {
        let group_positions = route.projection_layout.group_field_positions();
        let aggregate_positions = route.projection_layout.aggregate_positions();
        if group_positions.len() != route.group_fields.len() {
            return Err(super::invariant(format!(
                "grouped projection layout group-field count mismatch: layout={}, handoff={}",
                group_positions.len(),
                route.group_fields.len()
            )));
        }
        if aggregate_positions.len() != route.grouped_aggregate_exprs.len() {
            return Err(super::invariant(format!(
                "grouped projection layout aggregate count mismatch: layout={}, handoff={}",
                aggregate_positions.len(),
                route.grouped_aggregate_exprs.len()
            )));
        }

        // Projection position vectors must be strictly increasing and non-overlapping.
        if !group_positions
            .windows(2)
            .all(|window| window[0] < window[1])
        {
            return Err(super::invariant(
                "grouped projection layout group-field positions must be strictly increasing",
            ));
        }
        if !aggregate_positions
            .windows(2)
            .all(|window| window[0] < window[1])
        {
            return Err(super::invariant(
                "grouped projection layout aggregate positions must be strictly increasing",
            ));
        }
        if let (Some(last_group_position), Some(first_aggregate_position)) =
            (group_positions.last(), aggregate_positions.first())
            && last_group_position >= first_aggregate_position
        {
            return Err(super::invariant(
                "grouped projection layout must keep group fields before aggregate terminals",
            ));
        }

        Ok(())
    }
}
