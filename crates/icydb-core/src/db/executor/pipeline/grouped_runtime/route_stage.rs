//! Module: db::executor::pipeline::grouped_runtime::route_stage
//! Responsibility: grouped runtime route-stage payload assembly from route authority.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: pipeline runtime projection over immutable route/planner contracts.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, LoweredKey, lower_access_with_schema_info},
        cursor::ValidatedGroupedCursor,
        direction::Direction,
        executor::{
            GroupedContinuationContext, PreparedLoadPlan,
            pipeline::contracts::{GroupedPlannerPayload, GroupedRouteStage, IndexSpecBundle},
            route::{
                GroupedExecutionMode, RouteExecutionMode, RoutePlanRequest,
                build_execution_route_plan,
            },
            validate_executor_plan_for_authority,
        },
        query::plan::{GroupFieldSet, grouped_executor_handoff},
        resource::{budget::ExecutionConstructionBudget, construction::ConstructionBudget},
    },
    error::InternalError,
    value::Value,
};
use std::ops::Bound;

// Resolve grouped handoff/route metadata into one grouped route-stage payload.
pub(in crate::db::executor) fn resolve_grouped_route_for_plan(
    plan: PreparedLoadPlan,
    cursor: ValidatedGroupedCursor,
) -> Result<GroupedRouteStage, InternalError> {
    let authority = plan.authority();

    validate_executor_plan_for_authority(&authority, plan.logical_plan())?;
    let grouped_handoff = grouped_executor_handoff(plan.logical_plan())?;
    if grouped_handoff
        .group_fields()
        .iter()
        .any(|field| !field.is_resolved())
    {
        return Err(InternalError::query_executor_invariant());
    }
    if let Some(reason) = grouped_handoff.distinct_policy_violation_for_executor() {
        return Err(reason.into_grouped_route_internal_error());
    }
    let grouped_execution = grouped_handoff.execution();
    let grouped_plan_strategy = grouped_handoff.grouped_plan_strategy();
    let grouped_execution_route = grouped_handoff.grouped_execution_route();
    let group_fields = grouped_handoff.group_fields().clone();
    let projection_is_identity = grouped_handoff.projection_is_identity();
    let grouped_route_plan = build_execution_route_plan(
        grouped_handoff.base(),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy,
        },
    )?;

    // The route plan remains the single owner of grouped execution mode.
    grouped_route_plan
        .grouped_execution_mode()
        .ok_or_else(InternalError::query_executor_invariant)?;
    debug_assert!(
        matches!(
            grouped_route_plan.execution_mode(),
            RouteExecutionMode::Materialized
        ),
        "grouped execution must remain materialized",
    );

    let direction = grouped_route_plan.direction();
    let grouped_pagination_window = plan.grouped_pagination_window(&cursor)?;
    let resume_anchor = if grouped_route_plan.grouped_execution_mode()
        == Some(GroupedExecutionMode::OrderedStreaming)
    {
        grouped_pagination_window
            .resume_boundary()
            .map(|boundary| lower_grouped_resume_anchor(&plan, &group_fields, boundary, direction))
            .transpose()?
    } else {
        None
    };
    let continuation_signature = plan.continuation_signature_for_runtime()?;
    let continuation_boundary_arity = plan.grouped_cursor_boundary_arity()?;
    let continuation = GroupedContinuationContext::new(
        continuation_signature,
        continuation_boundary_arity,
        grouped_pagination_window,
        direction,
    );
    let (grouped_aggregate_execution_specs, projection_layout, grouped_distinct_execution_strategy) =
        grouped_handoff.into_route_stage_residents();
    let prepared = plan.into_access_plan_handoff();

    Ok(GroupedRouteStage {
        planner_payload: GroupedPlannerPayload {
            plan: prepared.plan,
            grouped_execution,
            grouped_execution_route,
            group_fields,
            grouped_aggregate_execution_specs,
            projection_layout,
            projection_is_identity,
            grouped_distinct_execution_strategy,
        },
        grouped_route_plan,
        index_specs: IndexSpecBundle {
            index_prefix_specs: prepared.index_prefix_specs,
            index_range_specs: prepared.index_range_specs,
        },
        continuation,
        resume_anchor,
    })
}

// The ordered-route proof guarantees that unfixed index components match the
// grouping tuple. Reuse accepted prefix lowering to exclude the entire completed
// group, including every primary-key suffix, without resetting work counters.
fn lower_grouped_resume_anchor(
    plan: &PreparedLoadPlan,
    group_fields: &GroupFieldSet,
    boundary: &Value,
    direction: Direction,
) -> Result<LoweredKey, InternalError> {
    let AccessPlan::Path(path) = &plan.logical_plan().access else {
        return Err(InternalError::query_executor_invariant());
    };
    let (index, fixed) = match path.as_ref() {
        AccessPath::IndexPrefix { index, values } => (index, values.as_slice()),
        AccessPath::IndexRange { spec } => (spec.index_ref(), spec.prefix_values()),
        _ => return Err(InternalError::query_executor_invariant()),
    };
    let Value::List(keys) = boundary else {
        return Err(InternalError::query_executor_invariant());
    };
    if keys.len() != group_fields.iter().count() {
        return Err(InternalError::query_executor_invariant());
    }
    let budget: &dyn ConstructionBudget = &ExecutionConstructionBudget;
    let mut values = budget.vec_with_capacity(index.key_arity())?;
    let mut grouped = group_fields.iter().zip(keys).peekable();
    for position in 0..index.key_arity() {
        if grouped.peek().is_none() && position >= fixed.len() {
            break;
        }
        let value = if grouped
            .peek()
            .is_some_and(|(field, _)| index.key_field_at(position) == Some(field.field()))
        {
            grouped.next().map(|(_, value)| value)
        } else {
            fixed.get(position)
        }
        .ok_or_else(InternalError::query_executor_invariant)?;
        values.push(budget.copy_value(value)?);
    }
    if grouped.next().is_some() {
        return Err(InternalError::query_executor_invariant());
    }
    let prefix = AccessPlan::<Value>::index_prefix_from_contract(index.clone(), values);
    let authority = plan.authority();
    let (prefixes, _) = lower_access_with_schema_info(
        authority.entity_tag(),
        &prefix,
        authority.accepted_schema_info(),
        budget,
    )
    .map_err(crate::db::access::LoweredAccessError::into_internal_error)?
    .into_index_specs();
    let [prefix] = prefixes.as_slice() else {
        return Err(InternalError::query_executor_invariant());
    };
    let (lower, upper) = prefix.raw_bounds(budget)?;
    match match direction {
        Direction::Asc => upper,
        Direction::Desc => lower,
    } {
        Bound::Included(key) | Bound::Excluded(key) => Ok(key.clone()),
        Bound::Unbounded => Err(InternalError::query_executor_invariant()),
    }
}
