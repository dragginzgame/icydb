//! Module: db::executor::pipeline::contracts::grouped::route_stage::projection
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::grouped::route_stage::projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionTrace, GroupedContinuationCapabilities, GroupedPaginationWindow,
            pipeline::contracts::{PageCursor, grouped::GroupedRouteStage},
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

impl<E> GroupedRouteStage<E>
where
    E: EntityKind + EntityValue,
{
    /// Borrow grouped logical plan payload.
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery<E::Key> {
        &self.planner_payload.plan
    }

    /// Return planner-projected grouped execution configuration.
    pub(in crate::db::executor) const fn grouped_execution(&self) -> GroupedExecutionConfig {
        self.planner_payload.grouped_execution
    }

    /// Borrow grouped projection layout.
    pub(in crate::db::executor) const fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.planner_payload.projection_layout
    }

    /// Borrow grouped field slot projection list.
    pub(in crate::db::executor) const fn group_fields(
        &self,
    ) -> &[crate::db::query::plan::FieldSlot] {
        self.planner_payload.group_fields.as_slice()
    }

    /// Borrow grouped aggregate expression list.
    pub(in crate::db::executor) const fn grouped_aggregate_exprs(
        &self,
    ) -> &[crate::db::query::builder::AggregateExpr] {
        self.planner_payload.grouped_aggregate_exprs.as_slice()
    }

    /// Borrow grouped HAVING contract when present.
    pub(in crate::db::executor) const fn grouped_having(&self) -> Option<&GroupHavingSpec> {
        self.planner_payload.grouped_having.as_ref()
    }

    /// Borrow grouped DISTINCT execution strategy contract.
    pub(in crate::db::executor) const fn grouped_distinct_execution_strategy(
        &self,
    ) -> &GroupedDistinctExecutionStrategy {
        &self.planner_payload.grouped_distinct_execution_strategy
    }

    /// Borrow route-owned grouped execution plan contract.
    pub(in crate::db::executor) const fn grouped_route_plan(
        &self,
    ) -> &crate::db::executor::ExecutionPlan {
        &self.route_payload.grouped_route_plan
    }

    /// Borrow lowered grouped index-prefix specs.
    pub(in crate::db::executor) const fn index_prefix_specs(
        &self,
    ) -> &[crate::db::access::LoweredIndexPrefixSpec] {
        self.index_specs.index_prefix_specs.as_slice()
    }

    /// Borrow lowered grouped index-range specs.
    pub(in crate::db::executor) const fn index_range_specs(
        &self,
    ) -> &[crate::db::access::LoweredIndexRangeSpec] {
        self.index_specs.index_range_specs.as_slice()
    }

    /// Return routed grouped stream direction.
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.execution_context.direction()
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    pub(in crate::db::executor) const fn grouped_plan_metrics_strategy(
        &self,
    ) -> GroupedPlanMetricsStrategy {
        self.execution_context.grouped_plan_metrics_strategy()
    }

    /// Borrow grouped runtime pagination projection.
    pub(in crate::db::executor) const fn grouped_pagination_window(
        &self,
    ) -> &GroupedPaginationWindow {
        self.execution_context
            .continuation()
            .grouped_pagination_window()
    }

    /// Return grouped row-read missing-row policy.
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.planner_payload.plan)
    }

    /// Return grouped continuation capabilities for this execution window.
    pub(in crate::db::executor) const fn grouped_continuation_capabilities(
        &self,
    ) -> GroupedContinuationCapabilities {
        self.execution_context.continuation().capabilities()
    }

    /// Build grouped next cursor after grouped boundary validation.
    pub(in crate::db::executor) fn grouped_next_cursor(
        &self,
        last_group_key: Vec<Value>,
    ) -> Result<PageCursor, InternalError> {
        self.execution_context
            .continuation()
            .grouped_next_cursor(last_group_key)
    }

    /// Borrow optional grouped execution trace for observability mutation.
    pub(in crate::db::executor) const fn execution_trace_mut(
        &mut self,
    ) -> &mut Option<ExecutionTrace> {
        self.execution_context.execution_trace_mut()
    }

    /// Consume stage and return final grouped execution trace payload.
    pub(in crate::db::executor) fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_context.into_execution_trace()
    }
}
