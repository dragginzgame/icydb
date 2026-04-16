//! Module: db::executor::pipeline::contracts::grouped::route_stage::payload
//! Defines grouped route-stage payload contracts carried into grouped runtime
//! execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::{
        ExecutionPlan, ExecutionTrace, GroupedContinuationContext, route::GroupedExecutionMode,
    },
    query::plan::{
        AccessPlannedQuery, GroupHavingExpr, GroupHavingSpec, GroupedAggregateExecutionSpec,
        GroupedDistinctExecutionStrategy, GroupedExecutionConfig, GroupedFoldPath,
        PlannedProjectionLayout,
    },
};

///
/// IndexSpecBundle
///
/// Grouped execution lowered index-spec bundle used by grouped stream
/// resolution. Keeps prefix/range specs grouped to avoid parallel vector drift.
///

pub(in crate::db::executor) struct IndexSpecBundle {
    pub(in crate::db::executor) index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    pub(in crate::db::executor) index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
}

///
/// GroupedPlannerPayload
///
/// Planner-owned grouped execution payload consumed by grouped runtime stages.
/// Keeps logical grouped plan artifacts (projection layout, grouped fields,
/// grouped terminals, and grouped DISTINCT/HAVING policy outputs) under one
/// ownership boundary.
///

pub(in crate::db::executor) struct GroupedPlannerPayload {
    pub(in crate::db::executor) plan: AccessPlannedQuery,
    pub(in crate::db::executor) grouped_execution: GroupedExecutionConfig,
    pub(in crate::db::executor) grouped_fold_path: GroupedFoldPath,
    pub(in crate::db::executor) group_fields: Vec<crate::db::query::plan::FieldSlot>,
    pub(in crate::db::executor) grouped_aggregate_execution_specs:
        Vec<GroupedAggregateExecutionSpec>,
    pub(in crate::db::executor) projection_layout: PlannedProjectionLayout,
    pub(in crate::db::executor) projection_is_identity: bool,
    pub(in crate::db::executor) grouped_having: Option<GroupHavingSpec>,
    pub(in crate::db::executor) grouped_having_expr: Option<GroupHavingExpr>,
    pub(in crate::db::executor) grouped_distinct_execution_strategy:
        GroupedDistinctExecutionStrategy,
}

///
/// GroupedRoutePayload
///
/// Route-owned grouped execution payload produced after grouped planner handoff.
/// Keeps route-plan artifacts scoped to grouped routing and stream resolution.
///

pub(in crate::db::executor) struct GroupedRoutePayload {
    pub(in crate::db::executor) grouped_route_plan: ExecutionPlan,
}

///
/// GroupedRouteStage
///
/// Route-planning stage payload for grouped execution.
/// Owns grouped handoff extraction, grouped route contracts, and grouped
/// execution metadata before runtime stream resolution starts.
///

pub(in crate::db::executor) struct GroupedRouteStage {
    pub(in crate::db::executor) planner_payload: GroupedPlannerPayload,
    pub(in crate::db::executor) route_payload: GroupedRoutePayload,
    pub(in crate::db::executor) index_specs: IndexSpecBundle,
    pub(in crate::db::executor) continuation: GroupedContinuationContext,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) grouped_execution_mode: GroupedExecutionMode,
    pub(in crate::db::executor) execution_trace: Option<ExecutionTrace>,
}
