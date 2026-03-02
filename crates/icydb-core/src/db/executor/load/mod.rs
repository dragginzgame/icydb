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
            GroupedPlannedCursor, decode_pk_cursor_boundary,
        },
        direction::Direction,
        executor::{
            ExecutionOptimization, ExecutionPreparation, ExecutionTrace, KeyOrderComparator,
            OrderedKeyStreamBox,
            aggregate::AggregateKind,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, resolve_any_aggregate_target_slot,
                resolve_numeric_aggregate_target_slot,
            },
            plan_metrics::GroupedPlanMetricsStrategy,
        },
        query::plan::{AccessPlannedQuery, GroupHavingSpec, PlannedProjectionLayout},
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;

pub(in crate::db::executor) use self::execute::{
    ExecutionInputs, MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
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
    pub(crate) items: Response<E>,
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
    _marker: PhantomData<E>,
}

///
/// GroupedRouteStage
///
/// Route-planning stage payload for grouped execution.
/// Owns grouped handoff extraction, grouped route contracts, and grouped
/// execution metadata before runtime stream resolution starts.
///

struct GroupedRouteStage<E: EntityKind + EntityValue> {
    plan: AccessPlannedQuery<E::Key>,
    cursor: GroupedPlannedCursor,
    direction: Direction,
    continuation_signature: ContinuationSignature,
    index_prefix_specs: Vec<crate::db::access::LoweredIndexPrefixSpec>,
    index_range_specs: Vec<crate::db::access::LoweredIndexRangeSpec>,
    grouped_execution: crate::db::query::plan::GroupedExecutionConfig,
    group_fields: Vec<crate::db::query::plan::FieldSlot>,
    grouped_aggregate_exprs: Vec<crate::db::query::builder::AggregateExpr>,
    projection_layout: PlannedProjectionLayout,
    grouped_having: Option<GroupHavingSpec>,
    grouped_route_plan: crate::db::executor::ExecutionPlan,
    grouped_plan_metrics_strategy: GroupedPlanMetricsStrategy,
    global_distinct_field_aggregate: Option<(AggregateKind, String)>,
    execution_trace: Option<ExecutionTrace>,
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
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
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
