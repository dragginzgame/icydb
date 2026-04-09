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
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedDistinctExecutionStrategy,
            GroupedExecutionConfig, GroupedFoldPath, PlannedProjectionLayout,
        },
    },
    error::InternalError,
    value::Value,
};

impl GroupedRouteStage {
    /// Construct one grouped route invariant for projection-layout aggregate
    /// positions that do not align with the grouped aggregate payload.
    pub(in crate::db::executor) fn aggregate_index_out_of_bounds_for_projection_layout(
        projection_index: usize,
        aggregate_index: usize,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped aggregate index out of bounds for projection layout: projection_index={projection_index}, aggregate_index={aggregate_index}",
        ))
    }

    /// Construct one grouped route invariant for grouped page-finalize keys
    /// that no longer match the canonical list-based group-key shape.
    pub(in crate::db::executor) fn canonical_group_key_must_be_list(
        value: &Value,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped canonical key must be Value::List, found {value:?}"
        ))
    }

    /// Borrow grouped logical plan payload.
    pub(in crate::db::executor) const fn plan(&self) -> &AccessPlannedQuery {
        &self.planner_payload.plan
    }

    /// Borrow the structural entity model for this grouped runtime shape.
    pub(in crate::db::executor) const fn entity_model(
        &self,
    ) -> &'static crate::model::entity::EntityModel {
        self.planner_payload.entity_model
    }

    /// Return planner-projected grouped execution configuration.
    pub(in crate::db::executor) const fn grouped_execution(&self) -> GroupedExecutionConfig {
        self.planner_payload.grouped_execution
    }

    /// Borrow planner-carried grouped fold-path selection.
    pub(in crate::db::executor) const fn grouped_fold_path(&self) -> GroupedFoldPath {
        self.planner_payload.grouped_fold_path
    }

    /// Borrow grouped projection layout.
    pub(in crate::db::executor) const fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.planner_payload.projection_layout
    }

    /// Return whether planner already proved grouped projection is row-identical.
    pub(in crate::db::executor) const fn projection_is_identity(&self) -> bool {
        self.planner_payload.projection_is_identity
    }

    /// Borrow grouped field slot projection list.
    pub(in crate::db::executor) const fn group_fields(
        &self,
    ) -> &[crate::db::query::plan::FieldSlot] {
        self.planner_payload.group_fields.as_slice()
    }

    /// Borrow planner-lowered grouped aggregate execution specs.
    pub(in crate::db::executor) const fn grouped_aggregate_execution_specs(
        &self,
    ) -> &[crate::db::query::plan::GroupedAggregateExecutionSpec] {
        self.planner_payload
            .grouped_aggregate_execution_specs
            .as_slice()
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

    /// Return grouped execution mode for grouped stream observability.
    pub(in crate::db::executor) const fn grouped_execution_mode(
        &self,
    ) -> crate::db::executor::route::GroupedExecutionMode {
        self.execution_context.grouped_execution_mode()
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
