//! Module: db::executor::shared::load_contracts::grouped::route_stage::payload
//! Responsibility: module-local ownership and contracts for db::executor::shared::load_contracts::grouped::route_stage::payload.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutionPlan, pipeline::grouped_runtime::GroupedExecutionContext},
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedDistinctExecutionStrategy,
            GroupedExecutionConfig, PlannedProjectionLayout,
        },
    },
    traits::{EntityKind, EntityValue},
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

pub(in crate::db::executor) struct GroupedPlannerPayload<E: EntityKind + EntityValue> {
    pub(in crate::db::executor) plan: AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor) grouped_execution: GroupedExecutionConfig,
    pub(in crate::db::executor) group_fields: Vec<crate::db::query::plan::FieldSlot>,
    pub(in crate::db::executor) grouped_aggregate_exprs:
        Vec<crate::db::query::builder::AggregateExpr>,
    pub(in crate::db::executor) projection_layout: PlannedProjectionLayout,
    pub(in crate::db::executor) grouped_having: Option<GroupHavingSpec>,
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

pub(in crate::db::executor) struct GroupedRouteStage<E: EntityKind + EntityValue> {
    pub(in crate::db::executor) planner_payload: GroupedPlannerPayload<E>,
    pub(in crate::db::executor) route_payload: GroupedRoutePayload,
    pub(in crate::db::executor) index_specs: IndexSpecBundle,
    pub(in crate::db::executor) execution_context: GroupedExecutionContext,
}
