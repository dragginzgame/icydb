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
            ExecutionOptimization, ExecutionPreparation, ExecutionTrace, KeyOrderComparator,
            OrderedKeyStreamBox,
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
};

#[cfg(test)]
pub(in crate::db::executor) use self::entrypoints::{
    load_execute_stage_order_guard, load_pipeline_state_optional_slot_count_guard,
};
pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
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
/// GroupedExecutionContext
///
/// Grouped runtime execution context artifacts derived at grouped route stage.
/// Keeps cursor/runtime direction, continuation signature, trace, and grouped
/// metrics strategy together for grouped stream/fold/output stages.
///

struct GroupedExecutionContext {
    direction: Direction,
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_continuation_window: GroupedContinuationWindow,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    execution_trace: Option<ExecutionTrace>,
}

struct GroupedRouteStage<E: EntityKind + EntityValue> {
    planner_payload: GroupedPlannerPayload<E>,
    route_payload: GroupedRoutePayload,
    index_specs: IndexSpecBundle,
    execution_context: GroupedExecutionContext,
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
