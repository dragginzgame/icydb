//! Module: db::executor::load::contracts::grouped::route_stage::payload
//! Responsibility: module-local ownership and contracts for db::executor::load::contracts::grouped::route_stage::payload.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutionPlan, load::GroupedExecutionContext},
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

pub(in crate::db::executor::load) struct IndexSpecBundle {
    pub(in crate::db::executor::load) index_prefix_specs:
        Vec<crate::db::access::LoweredIndexPrefixSpec>,
    pub(in crate::db::executor::load) index_range_specs:
        Vec<crate::db::access::LoweredIndexRangeSpec>,
}

///
/// GroupedPlannerPayload
///
/// Planner-owned grouped execution payload consumed by grouped runtime stages.
/// Keeps logical grouped plan artifacts (projection layout, grouped fields,
/// grouped terminals, and grouped DISTINCT/HAVING policy outputs) under one
/// ownership boundary.
///

pub(in crate::db::executor::load) struct GroupedPlannerPayload<E: EntityKind + EntityValue> {
    pub(in crate::db::executor::load) plan: AccessPlannedQuery<E::Key>,
    pub(in crate::db::executor::load) grouped_execution: GroupedExecutionConfig,
    pub(in crate::db::executor::load) group_fields: Vec<crate::db::query::plan::FieldSlot>,
    pub(in crate::db::executor::load) grouped_aggregate_exprs:
        Vec<crate::db::query::builder::AggregateExpr>,
    pub(in crate::db::executor::load) projection_layout: PlannedProjectionLayout,
    pub(in crate::db::executor::load) grouped_having: Option<GroupHavingSpec>,
    pub(in crate::db::executor::load) grouped_distinct_execution_strategy:
        GroupedDistinctExecutionStrategy,
}

///
/// GroupedRoutePayload
///
/// Route-owned grouped execution payload produced after grouped planner handoff.
/// Keeps route-plan artifacts scoped to grouped routing and stream resolution.
///

pub(in crate::db::executor::load) struct GroupedRoutePayload {
    pub(in crate::db::executor::load) grouped_route_plan: ExecutionPlan,
}

///
/// GroupedRouteStage
///
/// Route-planning stage payload for grouped execution.
/// Owns grouped handoff extraction, grouped route contracts, and grouped
/// execution metadata before runtime stream resolution starts.
///

pub(in crate::db::executor::load) struct GroupedRouteStage<E: EntityKind + EntityValue> {
    pub(in crate::db::executor::load) planner_payload: GroupedPlannerPayload<E>,
    pub(in crate::db::executor::load) route_payload: GroupedRoutePayload,
    pub(in crate::db::executor::load) index_specs: IndexSpecBundle,
    pub(in crate::db::executor::load) execution_context: GroupedExecutionContext,
}
