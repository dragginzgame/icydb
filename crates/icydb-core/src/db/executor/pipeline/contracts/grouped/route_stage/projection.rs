//! Module: db::executor::pipeline::contracts::grouped::route_stage::projection
//! Defines grouped route-stage projection contracts used before grouped output
//! shaping.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionTrace, GroupedPaginationWindow,
            pipeline::contracts::{PageCursor, grouped::GroupedRouteStage},
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, GroupHavingExpr, GroupedDistinctExecutionStrategy,
            GroupedExecutionConfig, GroupedFoldPath, PlannedProjectionLayout,
        },
    },
    error::InternalError,
    value::Value,
};

impl GroupedRouteStage {
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

    /// Borrow grouped HAVING expression when present.
    pub(in crate::db::executor) const fn grouped_having_expr(&self) -> Option<&GroupHavingExpr> {
        self.planner_payload.grouped_having_expr.as_ref()
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
        self.direction
    }

    /// Return grouped execution mode for grouped stream observability.
    pub(in crate::db::executor) const fn grouped_execution_mode(
        &self,
    ) -> crate::db::executor::route::GroupedExecutionMode {
        self.grouped_execution_mode
    }

    /// Return whether the grouped route projected bounded Top-K grouped
    /// selection mechanics for this execution stage.
    pub(in crate::db::executor) const fn uses_top_k_group_selection(&self) -> bool {
        self.route_payload.top_k_group_selection
    }

    /// Borrow grouped runtime pagination projection.
    pub(in crate::db::executor) const fn grouped_pagination_window(
        &self,
    ) -> &GroupedPaginationWindow {
        self.continuation.grouped_pagination_window()
    }

    /// Return grouped row-read missing-row policy.
    pub(in crate::db::executor) const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.planner_payload.plan)
    }

    /// Return the active grouped resume boundary when continuation filtering
    /// is enabled for this execution window.
    pub(in crate::db::executor) const fn grouped_resume_boundary(&self) -> Option<&Value> {
        if self.continuation.resume_boundary_applied() {
            self.grouped_pagination_window().resume_boundary()
        } else {
            None
        }
    }

    /// Return the active grouped candidate selection bound when this execution
    /// window still needs bounded candidate retention.
    pub(in crate::db::executor) const fn grouped_selection_bound(&self) -> Option<usize> {
        if self.continuation.selection_bound_applied() {
            self.grouped_pagination_window().selection_bound()
        } else {
            None
        }
    }

    /// Build grouped next cursor after grouped boundary validation.
    pub(in crate::db::executor) fn grouped_next_cursor(
        &self,
        last_group_key: Vec<Value>,
    ) -> Result<PageCursor, InternalError> {
        self.continuation.grouped_next_cursor(last_group_key)
    }

    /// Borrow optional grouped execution trace for observability mutation.
    pub(in crate::db::executor) const fn execution_trace_mut(
        &mut self,
    ) -> &mut Option<ExecutionTrace> {
        &mut self.execution_trace
    }

    /// Consume stage and return final grouped execution trace payload.
    pub(in crate::db::executor) fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_trace
    }

    /// Build one minimal grouped route stage for grouped runtime tests that
    /// only need window-selection semantics.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn new_for_test(
        direction: Direction,
        selection_bound: Option<usize>,
    ) -> Self {
        use crate::db::{
            cursor::ContinuationSignature,
            executor::{
                GroupedContinuationContext, GroupedPaginationWindow, route::GroupedExecutionMode,
            },
            predicate::MissingRowPolicy,
            query::plan::{
                AccessPlannedQuery, GroupedDistinctExecutionStrategy, GroupedExecutionConfig,
                GroupedFoldPath, PlannedProjectionLayout,
            },
        };

        let plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        let grouped_pagination_window =
            GroupedPaginationWindow::new(None, 0, selection_bound, 0, None);
        let continuation = GroupedContinuationContext::new(
            ContinuationSignature::from_bytes([0; 32]),
            1,
            grouped_pagination_window,
            direction,
        );
        let grouped_route_plan =
            crate::db::executor::route::ExecutionRoutePlan::grouped_for_test(direction);

        Self {
            planner_payload: crate::db::executor::pipeline::contracts::GroupedPlannerPayload {
                plan,
                grouped_execution: GroupedExecutionConfig {
                    max_groups: 128,
                    max_group_bytes: 8 * 1024,
                },
                grouped_fold_path: GroupedFoldPath::CountRowsDedicated,
                group_fields: Vec::new(),
                grouped_aggregate_execution_specs: Vec::new(),
                projection_layout: PlannedProjectionLayout {
                    group_field_positions: Vec::new(),
                    aggregate_positions: Vec::new(),
                },
                projection_is_identity: true,
                grouped_having_expr: None,
                grouped_distinct_execution_strategy: GroupedDistinctExecutionStrategy::None,
            },
            route_payload: crate::db::executor::pipeline::contracts::GroupedRoutePayload {
                grouped_route_plan,
                top_k_group_selection: false,
            },
            index_specs: crate::db::executor::pipeline::contracts::IndexSpecBundle {
                index_prefix_specs: Vec::new(),
                index_range_specs: Vec::new(),
            },
            continuation,
            direction,
            grouped_execution_mode: GroupedExecutionMode::HashMaterialized,
            execution_trace: None,
        }
    }
}
