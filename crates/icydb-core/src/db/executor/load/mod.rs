//! Module: executor::load
//! Responsibility: load-path execution orchestration, pagination, and trace contracts.
//! Does not own: logical planning semantics or relation/commit mutation policy.
//! Boundary: consumes executable load plans and delegates post-access semantics to kernel.
#![deny(unreachable_patterns)]

mod entrypoints;
mod execute;
mod fast_stream;
mod grouped_distinct;
mod grouped_fold;
mod grouped_having;
mod grouped_output;
mod grouped_route;
mod index_range_limit;
mod page;
mod pk_stream;
mod projection;
mod secondary_index;
mod terminal;

use crate::{
    db::{
        Context, Db, GroupedRow,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, GroupedContinuationToken,
            decode_pk_cursor_boundary,
        },
        direction::Direction,
        executor::{
            ContinuationEngine, ExecutionOptimization, ExecutionPreparation, ExecutionTrace,
            KeyOrderComparator, OrderedKeyStreamBox,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot,
            },
            plan_metrics::GroupedPlanMetricsStrategy,
        },
        query::plan::{
            AccessPlannedQuery, GroupHavingSpec, GroupedContinuationWindow,
            GroupedDistinctExecutionStrategy, PlannedProjectionLayout,
        },
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

#[cfg(test)]
pub(in crate::db::executor) use self::entrypoints::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, ExecutionInputsProjection, MaterializedExecutionAttempt,
    ResolvedExecutionKeyStream,
};
pub(in crate::db::executor) use self::page::PageMaterializationRequest;

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
    db: Db<E::Canister>,
    debug: bool,
}

///
/// GroupedRouteStage
///
/// Route-planning stage payload for grouped execution.
/// Owns grouped handoff extraction, grouped route contracts, and grouped
/// execution metadata before runtime stream resolution starts.
///

///
/// GroupedPlannerPayload
///
/// Planner-owned grouped execution payload consumed by grouped runtime stages.
/// Keeps logical grouped plan artifacts (projection layout, grouped fields,
/// grouped terminals, and grouped DISTINCT/HAVING policy outputs) under one
/// ownership boundary.
///

struct GroupedPlannerPayload<E: EntityKind + EntityValue> {
    plan: AccessPlannedQuery<E::Key>,
    grouped_execution: crate::db::query::plan::GroupedExecutionConfig,
    group_fields: Vec<crate::db::query::plan::FieldSlot>,
    grouped_aggregate_exprs: Vec<crate::db::query::builder::AggregateExpr>,
    projection_layout: PlannedProjectionLayout,
    grouped_having: Option<GroupHavingSpec>,
    grouped_distinct_execution_strategy: GroupedDistinctExecutionStrategy,
}

///
/// GroupedRoutePayload
///
/// Route-owned grouped execution payload produced after grouped planner handoff.
/// Keeps route-plan artifacts scoped to grouped routing and stream resolution.
///

struct GroupedRoutePayload {
    grouped_route_plan: crate::db::executor::ExecutionPlan,
}

///
/// IndexSpecBundle
///
/// Grouped execution lowered index-spec bundle used by grouped stream
/// resolution. Keeps prefix/range specs grouped to avoid parallel vector drift.
///

struct IndexSpecBundle {
    index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
}

///
/// GroupedPaginationWindow
///
/// Runtime grouped pagination projection consumed by grouped fold/page stages.
/// Separates grouped paging primitives from route/fold call signatures so grouped
/// continuation window semantics flow through one runtime boundary object.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) struct GroupedPaginationWindow {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedPaginationWindow {
    /// Build runtime grouped pagination projection from planner continuation window contract.
    #[must_use]
    pub(in crate::db::executor::load) fn from_contract(window: GroupedContinuationWindow) -> Self {
        let (
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ) = window.into_parts();

        Self {
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    /// Return grouped page limit for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Return grouped page-initial offset for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn initial_offset_for_page(&self) -> usize {
        self.initial_offset_for_page
    }

    /// Return bounded grouped candidate selection cap (`offset + limit + 1`) when active.
    #[must_use]
    pub(in crate::db::executor::load) const fn selection_bound(&self) -> Option<usize> {
        self.selection_bound
    }

    /// Return resume offset encoded into grouped continuation tokens.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_initial_offset(&self) -> u32 {
        self.resume_initial_offset
    }

    /// Borrow optional grouped resume boundary value for continuation filtering.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_boundary(&self) -> Option<&Value> {
        self.resume_boundary.as_ref()
    }
}

///
/// GroupedContinuationContext
///
/// Runtime grouped continuation context derived from immutable continuation
/// contracts. Carries grouped continuation signature, boundary arity, and one
/// grouped pagination projection bundle consumed by grouped runtime stages.
///

struct GroupedContinuationContext {
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_pagination_window: GroupedPaginationWindow,
}

impl GroupedContinuationContext {
    /// Construct grouped continuation runtime context from grouped contract values.
    #[must_use]
    const fn new(
        continuation_signature: ContinuationSignature,
        continuation_boundary_arity: usize,
        grouped_pagination_window: GroupedPaginationWindow,
    ) -> Self {
        Self {
            continuation_signature,
            continuation_boundary_arity,
            grouped_pagination_window,
        }
    }

    /// Borrow grouped runtime pagination projection.
    #[must_use]
    const fn grouped_pagination_window(&self) -> &GroupedPaginationWindow {
        &self.grouped_pagination_window
    }

    /// Build one grouped next cursor after validating grouped boundary arity.
    fn grouped_next_cursor(&self, last_group_key: Vec<Value>) -> Result<PageCursor, InternalError> {
        if last_group_key.len() != self.continuation_boundary_arity {
            return Err(invariant(format!(
                "grouped continuation boundary arity mismatch: expected {}, found {}",
                self.continuation_boundary_arity,
                last_group_key.len()
            )));
        }

        Ok(PageCursor::Grouped(
            ContinuationEngine::grouped_next_cursor_token(
                self.continuation_signature,
                last_group_key,
                self.grouped_pagination_window.resume_initial_offset(),
            ),
        ))
    }
}

///
/// GroupedRuntimeProjection
///
/// Runtime grouped execution projection shared across grouped stream/fold/output
/// stages. Keeps routed direction, grouped plan-metrics strategy, and optional
/// execution trace under one runtime-boundary object.
///

struct GroupedRuntimeProjection {
    direction: Direction,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    execution_trace: Option<ExecutionTrace>,
}

impl GroupedRuntimeProjection {
    /// Construct grouped runtime projection from routed direction/metrics/trace.
    #[must_use]
    const fn new(
        direction: Direction,
        grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
        execution_trace: Option<ExecutionTrace>,
    ) -> Self {
        Self {
            direction,
            grouped_plan_metrics_strategy,
            execution_trace,
        }
    }

    /// Return routed grouped stream direction.
    #[must_use]
    const fn direction(&self) -> Direction {
        self.direction
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    #[must_use]
    const fn grouped_plan_metrics_strategy(&self) -> GroupedPlanMetricsStrategy {
        self.grouped_plan_metrics_strategy
    }

    /// Borrow optional grouped execution trace for observability mutation.
    const fn execution_trace_mut(&mut self) -> &mut Option<ExecutionTrace> {
        &mut self.execution_trace
    }

    /// Consume projection and return final grouped execution trace payload.
    const fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.execution_trace
    }
}

///
/// GroupedExecutionContext
///
/// Grouped runtime execution context artifacts derived at grouped route stage.
/// Keeps cursor/runtime direction, continuation signature, trace, and grouped
/// metrics strategy together for grouped stream/fold/output stages.
///

struct GroupedExecutionContext {
    continuation: GroupedContinuationContext,
    runtime: GroupedRuntimeProjection,
}

impl GroupedExecutionContext {
    /// Construct grouped execution context from continuation + runtime projection.
    #[must_use]
    const fn new(
        continuation: GroupedContinuationContext,
        runtime: GroupedRuntimeProjection,
    ) -> Self {
        Self {
            continuation,
            runtime,
        }
    }

    /// Return routed grouped stream direction.
    #[must_use]
    const fn direction(&self) -> Direction {
        self.runtime.direction()
    }

    /// Return grouped plan-metrics strategy for grouped stream observability.
    #[must_use]
    const fn grouped_plan_metrics_strategy(&self) -> GroupedPlanMetricsStrategy {
        self.runtime.grouped_plan_metrics_strategy()
    }

    /// Borrow grouped continuation context.
    #[must_use]
    const fn continuation(&self) -> &GroupedContinuationContext {
        &self.continuation
    }

    /// Borrow optional grouped execution trace for observability mutation.
    const fn execution_trace_mut(&mut self) -> &mut Option<ExecutionTrace> {
        self.runtime.execution_trace_mut()
    }

    /// Consume grouped execution context and return final grouped execution trace payload.
    fn into_execution_trace(self) -> Option<ExecutionTrace> {
        self.runtime.into_execution_trace()
    }
}

struct GroupedRouteStage<E: EntityKind + EntityValue> {
    planner_payload: GroupedPlannerPayload<E>,
    route_payload: GroupedRoutePayload,
    index_specs: IndexSpecBundle,
    execution_context: GroupedExecutionContext,
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

struct GroupedStreamStage<'a, E: EntityKind + EntityValue> {
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

struct GroupedFoldStage {
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

    // Preserve PK fast-path cursor-boundary error classification at the executor boundary.
    pub(in crate::db::executor) fn validate_pk_fast_path_boundary_if_applicable(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if !Self::pk_order_stream_fast_path_shape_supported(plan) {
            return Ok(());
        }
        let _ = decode_pk_cursor_boundary::<E>(cursor_boundary)?;

        Ok(())
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
