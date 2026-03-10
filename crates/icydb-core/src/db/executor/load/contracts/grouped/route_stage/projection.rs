//! Module: db::executor::load::contracts::grouped::route_stage::projection
//! Responsibility: module-local ownership and contracts for db::executor::load::contracts::grouped::route_stage::projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionTrace,
            load::{
                GroupedContinuationCapabilities, GroupedPaginationWindow,
                contracts::{PageCursor, grouped::route_stage::payload::GroupedRouteStage},
            },
            plan_metrics::GroupedPlanMetricsStrategy,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedDistinctExecutionStrategy,
            GroupedExecutionConfig, PlannedProjectionLayout,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

///
/// GroupedRouteStageProjection
///
/// Compile-time projection boundary for grouped route-stage consumers.
/// Grouped fold/runtime helpers consume this trait so grouped planner/route
/// payload internals remain opaque outside grouped route-stage assembly.
///

pub(in crate::db::executor::load) trait GroupedRouteStageProjection<E>
where
    E: EntityKind + EntityValue,
{
    /// Borrow grouped logical plan payload.
    fn plan(&self) -> &AccessPlannedQuery<E::Key>;

    /// Return planner-projected grouped execution configuration.
    fn grouped_execution(&self) -> GroupedExecutionConfig;

    /// Borrow grouped projection layout.
    fn projection_layout(&self) -> &PlannedProjectionLayout;

    /// Borrow grouped field slot projection list.
    fn group_fields(&self) -> &[crate::db::query::plan::FieldSlot];

    /// Borrow grouped aggregate expression list.
    fn grouped_aggregate_exprs(&self) -> &[crate::db::query::builder::AggregateExpr];

    /// Borrow grouped HAVING contract when present.
    fn grouped_having(&self) -> Option<&GroupHavingSpec>;

    /// Borrow grouped DISTINCT execution strategy contract.
    fn grouped_distinct_execution_strategy(&self) -> &GroupedDistinctExecutionStrategy;

    /// Borrow route-owned grouped execution plan contract.
    fn grouped_route_plan(&self) -> &crate::db::executor::ExecutionPlan;

    /// Borrow lowered grouped index-prefix specs.
    fn index_prefix_specs(&self) -> &[crate::db::access::LoweredIndexPrefixSpec];

    /// Borrow lowered grouped index-range specs.
    fn index_range_specs(&self) -> &[crate::db::access::LoweredIndexRangeSpec];

    /// Return routed grouped stream direction.
    fn direction(&self) -> Direction;

    /// Return grouped plan-metrics strategy for grouped stream observability.
    fn grouped_plan_metrics_strategy(&self) -> GroupedPlanMetricsStrategy;

    /// Borrow grouped runtime pagination projection.
    fn grouped_pagination_window(&self) -> &GroupedPaginationWindow;

    /// Return grouped row-read missing-row policy.
    fn consistency(&self) -> MissingRowPolicy;

    /// Return grouped continuation capabilities for this execution window.
    fn grouped_continuation_capabilities(&self) -> GroupedContinuationCapabilities;

    /// Build grouped next cursor after grouped boundary validation.
    fn grouped_next_cursor(&self, last_group_key: Vec<Value>) -> Result<PageCursor, InternalError>;

    /// Borrow optional grouped execution trace for observability mutation.
    fn execution_trace_mut(&mut self) -> &mut Option<ExecutionTrace>;

    /// Consume stage and return final grouped execution trace payload.
    fn into_execution_trace(self) -> Option<ExecutionTrace>;
}

impl<E> GroupedRouteStageProjection<E> for GroupedRouteStage<E>
where
    E: EntityKind + EntityValue,
{
    fn plan(&self) -> &AccessPlannedQuery<E::Key> {
        &self.planner_payload.plan
    }

    fn grouped_execution(&self) -> GroupedExecutionConfig {
        self.planner_payload.grouped_execution
    }

    fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.planner_payload.projection_layout
    }

    fn group_fields(&self) -> &[crate::db::query::plan::FieldSlot] {
        self.planner_payload.group_fields.as_slice()
    }

    fn grouped_aggregate_exprs(&self) -> &[crate::db::query::builder::AggregateExpr] {
        self.planner_payload.grouped_aggregate_exprs.as_slice()
    }

    fn grouped_having(&self) -> Option<&GroupHavingSpec> {
        self.planner_payload.grouped_having.as_ref()
    }

    fn grouped_distinct_execution_strategy(&self) -> &GroupedDistinctExecutionStrategy {
        &self.planner_payload.grouped_distinct_execution_strategy
    }

    fn grouped_route_plan(&self) -> &crate::db::executor::ExecutionPlan {
        &self.route_payload.grouped_route_plan
    }

    fn index_prefix_specs(&self) -> &[crate::db::access::LoweredIndexPrefixSpec] {
        self.index_specs.index_prefix_specs.as_slice()
    }

    fn index_range_specs(&self) -> &[crate::db::access::LoweredIndexRangeSpec] {
        self.index_specs.index_range_specs.as_slice()
    }

    fn direction(&self) -> Direction {
        self.execution_context.direction()
    }

    fn grouped_plan_metrics_strategy(&self) -> GroupedPlanMetricsStrategy {
        self.execution_context.grouped_plan_metrics_strategy()
    }

    fn grouped_pagination_window(&self) -> &GroupedPaginationWindow {
        self.execution_context
            .continuation()
            .grouped_pagination_window()
    }

    fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.planner_payload.plan)
    }

    fn grouped_continuation_capabilities(&self) -> GroupedContinuationCapabilities {
        self.execution_context.continuation().capabilities()
    }

    fn grouped_next_cursor(&self, last_group_key: Vec<Value>) -> Result<PageCursor, InternalError> {
        self.execution_context
            .continuation()
            .grouped_next_cursor(last_group_key)
    }

    fn execution_trace_mut(&mut self) -> &mut Option<ExecutionTrace> {
        self.execution_context.execution_trace_mut()
    }

    fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_context.into_execution_trace()
    }
}
