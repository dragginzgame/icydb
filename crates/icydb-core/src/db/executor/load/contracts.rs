use crate::{
    db::{
        Context, Db, GroupedRow,
        cursor::{ContinuationToken, GroupedContinuationToken},
        direction::Direction,
        executor::{
            ExecutionOptimization, ExecutionPreparation, ExecutionTrace, KeyOrderComparator,
            OrderedKeyStreamBox,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot,
            },
            load::{
                GroupedContinuationCapabilities, GroupedExecutionContext, GroupedPaginationWindow,
                ResolvedExecutionKeyStream,
            },
            plan_metrics::GroupedPlanMetricsStrategy,
        },
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedDistinctExecutionStrategy,
            PlannedProjectionLayout,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

///
/// PageCursor
///
/// Internal continuation cursor enum for scalar and grouped pagination.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PageCursor {
    Scalar(ContinuationToken),
    Grouped(GroupedContinuationToken),
}

impl PageCursor {
    /// Borrow scalar continuation token when this cursor is scalar-shaped.
    #[must_use]
    pub(in crate::db) const fn as_scalar(&self) -> Option<&ContinuationToken> {
        match self {
            Self::Scalar(token) => Some(token),
            Self::Grouped(_) => None,
        }
    }

    /// Borrow grouped continuation token when this cursor is grouped-shaped.
    #[must_use]
    pub(in crate::db) const fn as_grouped(&self) -> Option<&GroupedContinuationToken> {
        match self {
            Self::Scalar(_) => None,
            Self::Grouped(token) => Some(token),
        }
    }
}

impl From<ContinuationToken> for PageCursor {
    fn from(value: ContinuationToken) -> Self {
        Self::Scalar(value)
    }
}

impl From<GroupedContinuationToken> for PageCursor {
    fn from(value: GroupedContinuationToken) -> Self {
        Self::Grouped(value)
    }
}

///
/// CursorPage
///
/// Internal load page result with continuation cursor payload.
/// Returned by paged executor entrypoints.
///

#[derive(Debug)]
pub(crate) struct CursorPage<E: EntityKind> {
    pub(crate) items: EntityResponse<E>,
    pub(crate) next_cursor: Option<PageCursor>,
}

///
/// GroupedCursorPage
///
/// Internal grouped page result with grouped rows and continuation cursor payload.
///

#[derive(Debug)]
pub(in crate::db) struct GroupedCursorPage {
    pub(in crate::db) rows: Vec<GroupedRow>,
    pub(in crate::db) next_cursor: Option<PageCursor>,
}

/// Resolve key-stream comparator contract from runtime direction.
pub(in crate::db::executor) const fn key_stream_comparator_from_direction(
    direction: Direction,
) -> KeyOrderComparator {
    KeyOrderComparator::from_direction(direction)
}

///
/// FastPathKeyResult
///
/// Internal fast-path access result.
/// Carries ordered keys plus observability metadata for shared execution phases.
///

pub(in crate::db::executor) struct FastPathKeyResult {
    pub(in crate::db::executor) ordered_key_stream: OrderedKeyStreamBox,
    pub(in crate::db::executor) rows_scanned: usize,
    pub(in crate::db::executor) optimization: ExecutionOptimization,
}

///
/// LoadExecutor
///
/// Load-plan executor with canonical post-access semantics.
/// Coordinates fast paths, trace hooks, and pagination cursors.
///

#[derive(Clone)]
pub(crate) struct LoadExecutor<E: EntityKind> {
    pub(in crate::db::executor::load) db: Db<E::Canister>,
    pub(in crate::db::executor::load) debug: bool,
}

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

///
/// GroupedStreamStage
///
/// Stream-construction stage payload for grouped execution.
/// Owns recovered context, execution preparation, and resolved grouped key
/// stream for fold-phase consumption.
///

pub(in crate::db::executor::load) struct GroupedStreamStage<'a, E: EntityKind + EntityValue> {
    ctx: Context<'a, E>,
    execution_preparation: ExecutionPreparation,
    resolved: ResolvedExecutionKeyStream,
}

impl<'a, E> GroupedStreamStage<'a, E>
where
    E: EntityKind + EntityValue,
{
    // Build one grouped stream stage from recovered context, execution preparation,
    // and resolved grouped key stream payload.
    pub(in crate::db::executor::load) const fn new(
        ctx: Context<'a, E>,
        execution_preparation: ExecutionPreparation,
        resolved: ResolvedExecutionKeyStream,
    ) -> Self {
        Self {
            ctx,
            execution_preparation,
            resolved,
        }
    }

    // Borrow grouped runtime context, execution preparation, and mutable resolved
    // key stream together so callers can combine immutable/mutable borrows safely.
    pub(in crate::db::executor::load) const fn parts_mut(
        &mut self,
    ) -> (
        &Context<'a, E>,
        &ExecutionPreparation,
        &mut ResolvedExecutionKeyStream,
    ) {
        (&self.ctx, &self.execution_preparation, &mut self.resolved)
    }

    // Derive grouped path `rows_scanned` from resolved stream metadata or runtime fallback.
    pub(in crate::db::executor::load) fn rows_scanned(&self, fallback: usize) -> usize {
        self.resolved.rows_scanned_override().unwrap_or(fallback)
    }

    // Borrow grouped path optimization outcome metadata.
    pub(in crate::db::executor::load) const fn optimization(
        &self,
    ) -> Option<ExecutionOptimization> {
        self.resolved.optimization()
    }

    // Borrow grouped path index-predicate observability metadata.
    pub(in crate::db::executor::load) const fn index_predicate_applied(&self) -> bool {
        self.resolved.index_predicate_applied()
    }

    // Borrow grouped path index-predicate rejection counter.
    pub(in crate::db::executor::load) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.resolved.index_predicate_keys_rejected()
    }

    // Borrow grouped path DISTINCT-key dedupe counter.
    pub(in crate::db::executor::load) fn distinct_keys_deduped(&self) -> u64 {
        self.resolved.distinct_keys_deduped()
    }
}

///
/// GroupedFoldStage
///
/// Fold-phase output payload for grouped execution.
/// Owns grouped page materialization plus observability counters consumed by
/// the final output stage.
///

pub(in crate::db::executor::load) struct GroupedFoldStage {
    page: GroupedCursorPage,
    filtered_rows: usize,
    check_filtered_rows_upper_bound: bool,
    rows_scanned: usize,
    optimization: Option<ExecutionOptimization>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped: u64,
}

impl GroupedFoldStage {
    // Build one grouped fold-stage payload from grouped page output plus stream
    // observability metadata captured after grouped fold execution.
    pub(in crate::db::executor::load) fn from_grouped_stream<E>(
        page: GroupedCursorPage,
        filtered_rows: usize,
        check_filtered_rows_upper_bound: bool,
        stream: &GroupedStreamStage<'_, E>,
        scanned_rows_fallback: usize,
    ) -> Self
    where
        E: EntityKind + EntityValue,
    {
        Self {
            page,
            filtered_rows,
            check_filtered_rows_upper_bound,
            rows_scanned: stream.rows_scanned(scanned_rows_fallback),
            optimization: stream.optimization(),
            index_predicate_applied: stream.index_predicate_applied(),
            index_predicate_keys_rejected: stream.index_predicate_keys_rejected(),
            distinct_keys_deduped: stream.distinct_keys_deduped(),
        }
    }

    // Return grouped output row count for observability.
    pub(in crate::db::executor::load) const fn rows_returned(&self) -> usize {
        self.page.rows.len()
    }

    // Borrow grouped path optimization outcome metadata.
    pub(in crate::db::executor::load) const fn optimization(
        &self,
    ) -> Option<ExecutionOptimization> {
        self.optimization
    }

    // Borrow grouped path rows-scanned observability metric.
    pub(in crate::db::executor::load) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    // Borrow grouped path index-predicate observability metadata.
    pub(in crate::db::executor::load) const fn index_predicate_applied(&self) -> bool {
        self.index_predicate_applied
    }

    // Borrow grouped path index-predicate rejection counter.
    pub(in crate::db::executor::load) const fn index_predicate_keys_rejected(&self) -> u64 {
        self.index_predicate_keys_rejected
    }

    // Borrow grouped path DISTINCT-key dedupe counter.
    pub(in crate::db::executor::load) const fn distinct_keys_deduped(&self) -> u64 {
        self.distinct_keys_deduped
    }

    // Return whether grouped finalization should assert filtered-row upper bound.
    pub(in crate::db::executor::load) const fn should_check_filtered_rows_upper_bound(
        &self,
    ) -> bool {
        self.check_filtered_rows_upper_bound
    }

    // Borrow grouped filtered-row count for pagination sanity checks.
    pub(in crate::db::executor::load) const fn filtered_rows(&self) -> usize {
        self.filtered_rows
    }

    // Consume folded stage and return final grouped page payload.
    pub(in crate::db::executor::load) fn into_page(self) -> GroupedCursorPage {
        self.page
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one load executor bound to a database handle and debug mode.
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self { db, debug }
    }

    /// Recover one canonical read context for kernel-owned execution setup.
    pub(in crate::db::executor) fn recovered_context(
        &self,
    ) -> Result<crate::db::Context<'_, E>, InternalError> {
        self.db.recovered_context::<E>()
    }

    // Resolve one aggregate target field into a stable slot with canonical
    // field-error taxonomy mapping.
    pub(in crate::db::executor) fn resolve_any_field_slot(
        target_field: &str,
    ) -> Result<FieldSlot, InternalError> {
        resolve_any_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Resolve one numeric aggregate target field into a stable slot with
    // canonical field-error taxonomy mapping.
    pub(in crate::db::executor) fn resolve_numeric_field_slot(
        target_field: &str,
    ) -> Result<FieldSlot, InternalError> {
        resolve_numeric_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)
    }
}
