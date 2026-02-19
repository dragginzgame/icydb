mod execute;
mod index_range_limit;
mod page;
mod pk_stream;
mod route;
mod secondary_index;

use crate::{
    db::{
        Db,
        executor::OrderedKeyStreamBox,
        executor::plan::{record_plan_metrics, record_rows_scanned},
        index::RawIndexKey,
        query::plan::{
            AccessPlan, AccessPlanProjection, CursorBoundary, Direction, ExecutablePlan,
            LogicalPlan, OrderDirection, PlannedCursor, compute_page_window,
            decode_pk_cursor_boundary, project_access_plan, validate::validate_executor_plan,
        },
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::{marker::PhantomData, ops::Bound};

///
/// CursorPage
///
/// Internal load page result with continuation cursor payload.
/// Returned by paged executor entrypoints.
///

#[derive(Debug)]
pub(crate) struct CursorPage<E: EntityKind> {
    pub(crate) items: Response<E>,

    pub(crate) next_cursor: Option<Vec<u8>>,
}

///
/// ExecutionAccessPathVariant
///
/// Coarse access path shape used by the load execution trace surface.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionAccessPathVariant {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexRange,
    FullScan,
    Union,
    Intersection,
}

///
/// ExecutionOptimization
///
/// Canonical load optimization selected by execution, if any.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOptimization {
    PrimaryKey,
    SecondaryOrderPushdown,
    IndexRangeLimitPushdown,
}

///
/// ExecutionTrace
///
/// Structured, opt-in load execution introspection snapshot.
/// Captures plan-shape and execution decisions without changing semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub access_path_variant: ExecutionAccessPathVariant,
    pub direction: OrderDirection,
    pub optimization: Option<ExecutionOptimization>,
    pub keys_scanned: u64,
    pub rows_returned: u64,
    pub continuation_applied: bool,
}

impl ExecutionTrace {
    fn new<K>(access: &AccessPlan<K>, direction: Direction, continuation_applied: bool) -> Self {
        Self {
            access_path_variant: access_path_variant(access),
            direction: execution_order_direction(direction),
            optimization: None,
            keys_scanned: 0,
            rows_returned: 0,
            continuation_applied,
        }
    }

    fn set_path_outcome(
        &mut self,
        optimization: Option<ExecutionOptimization>,
        keys_scanned: usize,
        rows_returned: usize,
    ) {
        self.optimization = optimization;
        self.keys_scanned = u64::try_from(keys_scanned).unwrap_or(u64::MAX);
        self.rows_returned = u64::try_from(rows_returned).unwrap_or(u64::MAX);
    }
}

struct ExecutionAccessProjection;

impl<K> AccessPlanProjection<K> for ExecutionAccessProjection {
    type Output = ExecutionAccessPathVariant;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        ExecutionAccessPathVariant::ByKey
    }

    fn by_keys(&mut self, _keys: &[K]) -> Self::Output {
        ExecutionAccessPathVariant::ByKeys
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        ExecutionAccessPathVariant::KeyRange
    }

    fn index_prefix(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        ExecutionAccessPathVariant::IndexPrefix
    }

    fn index_range(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        ExecutionAccessPathVariant::IndexRange
    }

    fn full_scan(&mut self) -> Self::Output {
        ExecutionAccessPathVariant::FullScan
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        ExecutionAccessPathVariant::Union
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        ExecutionAccessPathVariant::Intersection
    }
}

fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    let mut projection = ExecutionAccessProjection;
    project_access_plan(access, &mut projection)
}

const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}

///
/// FastPathKeyResult
///
/// Internal fast-path access result.
/// Carries ordered keys plus observability metadata for shared execution phases.
///
struct FastPathKeyResult {
    ordered_key_stream: OrderedKeyStreamBox,
    rows_scanned: usize,
    optimization: ExecutionOptimization,
}

///
/// IndexRangeLimitSpec
///
/// Canonical executor decision payload for index-range limit pushdown.
/// Encodes the bounded fetch size after all eligibility gates pass.
///

struct IndexRangeLimitSpec {
    fetch: usize,
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

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    pub(crate) fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        self.execute_paged_with_cursor(plan, PlannedCursor::none())
            .map(|page| page.items)
    }

    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        self.execute_paged_with_cursor_traced(plan, cursor)
            .map(|(page, _)| page)
    }

    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let cursor: PlannedCursor = plan.revalidate_planned_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_anchor = cursor.index_range_anchor().cloned();

        if !plan.mode().is_load() {
            return Err(InternalError::query_invariant(
                "executor invariant violated: load executor requires load plans",
            ));
        }

        let direction = plan.direction();
        let continuation_signature = plan.continuation_signature();
        let continuation_applied = cursor_boundary.is_some() || index_range_anchor.is_some();
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            let plan = plan.into_inner();

            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;

            record_plan_metrics(&plan.access);
            // Plan fast-path routing decisions once, then execute in canonical order.
            let fast_path_plan = Self::build_fast_path_plan(
                &plan,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
            )?;

            if let Some(page) = Self::try_execute_fast_path_plan(
                &ctx,
                &plan,
                &fast_path_plan,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
                direction,
                continuation_signature,
                &mut span,
                &mut execution_trace,
            )? {
                return Ok(page);
            }

            let page = Self::execute_fallback_path(
                &ctx,
                &plan,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
                direction,
                continuation_signature,
                &mut span,
                &mut execution_trace,
            )?;
            Ok(page)
        })();

        result.map(|page| (page, execution_trace))
    }

    // Record shared observability outcome for any execution path.
    fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        rows_returned: usize,
    ) {
        record_rows_scanned::<E>(rows_scanned);
        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(optimization, rows_scanned, rows_returned);
            debug_assert_eq!(
                execution_trace.keys_scanned,
                u64::try_from(rows_scanned).unwrap_or(u64::MAX),
                "execution trace keys_scanned must match rows_scanned metrics input",
            );
        }
    }

    // Preserve PK fast-path cursor-boundary error classification at the executor boundary.
    fn validate_pk_fast_path_boundary_if_applicable(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if !Self::is_pk_order_stream_eligible(plan) {
            return Ok(());
        }
        let _ = decode_pk_cursor_boundary::<E>(cursor_boundary)?;

        Ok(())
    }

    fn assess_index_range_limit_pushdown(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
    ) -> Option<IndexRangeLimitSpec> {
        if !Self::is_index_range_limit_pushdown_shape_eligible(plan) {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = compute_page_window(page.offset, limit, true).fetch_count;

        Some(IndexRangeLimitSpec { fetch })
    }
}
