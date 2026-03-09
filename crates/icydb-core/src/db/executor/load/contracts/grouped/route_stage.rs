use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionTrace,
            load::{
                GroupedContinuationCapabilities, GroupedExecutionContext, GroupedPaginationWindow,
            },
            plan_metrics::GroupedPlanMetricsStrategy,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedDistinctExecutionStrategy,
            PlannedProjectionLayout,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

use crate::db::executor::load::contracts::PageCursor;

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
    pub(in crate::db::executor::load) grouped_execution:
        crate::db::query::plan::GroupedExecutionConfig,
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
    pub(in crate::db::executor::load) grouped_route_plan: crate::db::executor::ExecutionPlan,
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

impl<E> GroupedRouteStage<E>
where
    E: EntityKind + EntityValue,
{
    /// Borrow grouped logical plan payload.
    #[must_use]
    const fn plan(&self) -> &AccessPlannedQuery<E::Key> {
        &self.planner_payload.plan
    }

    /// Return planner-projected grouped execution configuration.
    #[must_use]
    const fn grouped_execution(&self) -> crate::db::query::plan::GroupedExecutionConfig {
        self.planner_payload.grouped_execution
    }

    /// Borrow grouped projection layout.
    #[must_use]
    const fn projection_layout(&self) -> &PlannedProjectionLayout {
        &self.planner_payload.projection_layout
    }

    /// Borrow grouped field slot projection list.
    #[must_use]
    const fn group_fields(&self) -> &[crate::db::query::plan::FieldSlot] {
        self.planner_payload.group_fields.as_slice()
    }

    /// Borrow grouped aggregate expression list.
    #[must_use]
    const fn grouped_aggregate_exprs(&self) -> &[crate::db::query::builder::AggregateExpr] {
        self.planner_payload.grouped_aggregate_exprs.as_slice()
    }

    /// Borrow grouped HAVING contract when present.
    #[must_use]
    const fn grouped_having(&self) -> Option<&GroupHavingSpec> {
        self.planner_payload.grouped_having.as_ref()
    }

    /// Borrow grouped DISTINCT execution strategy contract.
    #[must_use]
    const fn grouped_distinct_execution_strategy(&self) -> &GroupedDistinctExecutionStrategy {
        &self.planner_payload.grouped_distinct_execution_strategy
    }

    /// Borrow route-owned grouped execution plan contract.
    #[must_use]
    const fn grouped_route_plan(&self) -> &crate::db::executor::ExecutionPlan {
        &self.route_payload.grouped_route_plan
    }

    /// Borrow lowered grouped index-prefix specs.
    #[must_use]
    const fn index_prefix_specs(&self) -> &[crate::db::access::LoweredIndexPrefixSpec] {
        self.index_specs.index_prefix_specs.as_slice()
    }

    /// Borrow lowered grouped index-range specs.
    #[must_use]
    const fn index_range_specs(&self) -> &[crate::db::access::LoweredIndexRangeSpec] {
        self.index_specs.index_range_specs.as_slice()
    }

    /// Return grouped row-read missing-row policy.
    #[must_use]
    const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(self.plan())
    }
}

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
    fn grouped_execution(&self) -> crate::db::query::plan::GroupedExecutionConfig;

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
        Self::plan(self)
    }

    fn grouped_execution(&self) -> crate::db::query::plan::GroupedExecutionConfig {
        Self::grouped_execution(self)
    }

    fn projection_layout(&self) -> &PlannedProjectionLayout {
        Self::projection_layout(self)
    }

    fn group_fields(&self) -> &[crate::db::query::plan::FieldSlot] {
        Self::group_fields(self)
    }

    fn grouped_aggregate_exprs(&self) -> &[crate::db::query::builder::AggregateExpr] {
        Self::grouped_aggregate_exprs(self)
    }

    fn grouped_having(&self) -> Option<&GroupHavingSpec> {
        Self::grouped_having(self)
    }

    fn grouped_distinct_execution_strategy(&self) -> &GroupedDistinctExecutionStrategy {
        Self::grouped_distinct_execution_strategy(self)
    }

    fn grouped_route_plan(&self) -> &crate::db::executor::ExecutionPlan {
        Self::grouped_route_plan(self)
    }

    fn index_prefix_specs(&self) -> &[crate::db::access::LoweredIndexPrefixSpec] {
        Self::index_prefix_specs(self)
    }

    fn index_range_specs(&self) -> &[crate::db::access::LoweredIndexRangeSpec] {
        Self::index_range_specs(self)
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
        Self::consistency(self)
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
