//! Module: db::executor::pipeline::grouped_runtime::route_stage
//! Responsibility: grouped runtime route-stage payload assembly from route authority.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: pipeline runtime projection over immutable route/planner contracts.

use crate::{
    db::{
        cursor::GroupedPlannedCursor,
        executor::{
            ExecutionTrace, GroupedContinuationCapabilities, GroupedContinuationContext,
            PreparedLoadPlan,
            pipeline::contracts::{
                GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, IndexSpecBundle,
                LoadExecutor,
            },
            pipeline::grouped_runtime::GroupedExecutionContext,
            route::{
                RouteExecutionMode, build_execution_route_plan_for_grouped_plan,
                grouped_route_observability_for_runtime,
            },
            validate_executor_plan_for_authority,
        },
        query::plan::{grouped_aggregate_execution_specs_with_model, grouped_executor_handoff},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Resolve grouped handoff/route metadata into one grouped route-stage payload.
    pub(in crate::db::executor) fn resolve_grouped_route(
        plan: PreparedLoadPlan,
        cursor: GroupedPlannedCursor,
        debug: bool,
    ) -> Result<GroupedRouteStage, InternalError> {
        let authority = plan.authority();

        validate_executor_plan_for_authority(authority, plan.logical_plan())?;
        let grouped_handoff = grouped_executor_handoff(plan.logical_plan())?;
        if let Some(reason) = grouped_handoff.distinct_policy_violation_for_executor() {
            return Err(reason.into_grouped_route_internal_error());
        }
        let grouped_execution = grouped_handoff.execution();
        let grouped_plan_strategy = grouped_handoff.grouped_plan_strategy();
        let grouped_fold_path = grouped_handoff.grouped_fold_path();
        let group_fields = grouped_handoff.group_fields().to_vec();
        let grouped_aggregate_execution_specs = grouped_aggregate_execution_specs_with_model(
            authority.model(),
            grouped_handoff.aggregate_projection_specs(),
        )?;
        let projection_layout = grouped_handoff.projection_layout().clone();
        debug_assert!(
            grouped_handoff.projection_layout_valid(),
            "planner grouped projection layout invariants must hold at executor boundary",
        );
        let grouped_distinct_execution_strategy =
            grouped_handoff.distinct_execution_strategy().clone();
        let grouped_having = grouped_handoff.having().cloned();
        let grouped_route_plan = build_execution_route_plan_for_grouped_plan(
            authority.model(),
            grouped_handoff.base(),
            grouped_plan_strategy,
        );
        let grouped_route_observability =
            grouped_route_observability_for_runtime(&grouped_route_plan)?;
        let grouped_route_execution_mode = grouped_route_observability.execution_mode();
        let grouped_metrics_execution_mode =
            grouped_route_observability.grouped_execution_mode().into();
        debug_assert!(
            matches!(
                grouped_route_execution_mode,
                RouteExecutionMode::Materialized
            ),
            "grouped execution must remain materialized",
        );

        let direction = grouped_route_plan.direction();
        let grouped_pagination_window = plan.grouped_pagination_window(&cursor)?;
        let continuation_capabilities = GroupedContinuationCapabilities::from_window(
            !cursor.is_empty(),
            &grouped_pagination_window,
        );
        let continuation_applied = continuation_capabilities.applied();
        let execution_trace = debug.then(|| {
            ExecutionTrace::new(&plan.logical_plan().access, direction, continuation_applied)
        });
        let continuation_signature = plan.continuation_signature_for_runtime()?;
        let continuation_boundary_arity = plan.grouped_cursor_boundary_arity()?;
        let continuation = GroupedContinuationContext::new(
            continuation_capabilities,
            continuation_signature,
            continuation_boundary_arity,
            grouped_pagination_window,
        );
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let plan = plan.into_plan();

        Ok(GroupedRouteStage {
            planner_payload: GroupedPlannerPayload {
                plan,
                entity_model: authority.model(),
                grouped_execution,
                grouped_fold_path,
                group_fields,
                grouped_aggregate_execution_specs,
                projection_layout,
                grouped_having,
                grouped_distinct_execution_strategy,
            },
            route_payload: GroupedRoutePayload { grouped_route_plan },
            index_specs: IndexSpecBundle {
                index_prefix_specs,
                index_range_specs,
            },
            execution_context: GroupedExecutionContext::new(
                continuation,
                direction,
                grouped_metrics_execution_mode,
                execution_trace,
            ),
        })
    }
}
