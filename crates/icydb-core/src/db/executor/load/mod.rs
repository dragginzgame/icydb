mod index_range_limit;
mod pk_stream;
mod secondary_index;

use crate::{
    db::{
        Context, Db,
        executor::plan::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
        executor::{OrderedKeyStream, OrderedKeyStreamBox},
        index::{IndexKey, RawIndexKey},
        query::plan::{
            AccessPlan, AccessPlanProjection, ContinuationSignature, ContinuationToken,
            CursorBoundary, Direction, ExecutablePlan, IndexRangeCursorAnchor, LogicalPlan,
            OrderDirection, PlannedCursor, decode_pk_cursor_boundary,
            logical::PostAccessStats,
            project_access_plan,
            validate::{
                PushdownApplicability, assess_secondary_order_pushdown_if_applicable_validated,
                validate_executor_plan,
            },
        },
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    types::Id,
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
/// ExecutionPushdownType
///
/// Pushdown optimization kind applied by load execution, if any.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPushdownType {
    SecondaryOrder,
    IndexRangeLimit,
}

///
/// ExecutionFastPath
///
/// Fast-path branch selected by load execution, if any.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionFastPath {
    PrimaryKey,
    SecondaryIndex,
    IndexRange,
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
    pub pushdown_used: bool,
    pub pushdown_type: Option<ExecutionPushdownType>,
    pub fast_path_used: Option<ExecutionFastPath>,
    pub keys_scanned: u64,
    pub rows_returned: u64,
    pub continuation_applied: bool,
}

impl ExecutionTrace {
    fn new<K>(access: &AccessPlan<K>, direction: Direction, continuation_applied: bool) -> Self {
        Self {
            access_path_variant: access_path_variant(access),
            direction: execution_order_direction(direction),
            pushdown_used: false,
            pushdown_type: None,
            fast_path_used: None,
            keys_scanned: 0,
            rows_returned: 0,
            continuation_applied,
        }
    }

    fn set_path_outcome(
        &mut self,
        fast_path_used: Option<ExecutionFastPath>,
        pushdown_type: Option<ExecutionPushdownType>,
        keys_scanned: usize,
        rows_returned: usize,
    ) {
        self.fast_path_used = fast_path_used;
        self.pushdown_type = pushdown_type;
        self.pushdown_used = pushdown_type.is_some();
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
    fast_path_used: ExecutionFastPath,
    pushdown_type: Option<ExecutionPushdownType>,
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
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
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
            // Compute secondary ORDER BY pushdown eligibility once, then share the
            // derived decision across trace and fast-path gating.
            let secondary_pushdown_applicability =
                Self::assess_secondary_order_pushdown_applicability(&plan);

            if let Some(page) = Self::try_execute_fast_paths(
                &ctx,
                &plan,
                &secondary_pushdown_applicability,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
                direction,
                continuation_signature,
                &mut span,
                &mut execution_trace,
            )? {
                return Ok(page);
            }

            let mut key_stream = ctx.ordered_key_stream_from_access_plan_with_index_range_anchor(
                &plan.access,
                index_range_anchor.as_ref(),
                direction,
            )?;
            let (page, keys_scanned, post_access_rows) = Self::materialize_key_stream_into_page(
                &ctx,
                &plan,
                key_stream.as_mut(),
                cursor_boundary.as_ref(),
                direction,
                continuation_signature,
            )?;
            Self::finalize_path_outcome(
                &mut execution_trace,
                None,
                None,
                keys_scanned,
                post_access_rows,
            );

            set_rows_from_len(&mut span, page.items.0.len());
            Ok(page)
        })();

        result.map(|page| (page, execution_trace))
    }

    // Record shared observability outcome for any execution path.
    fn finalize_path_outcome(
        execution_trace: &mut Option<ExecutionTrace>,
        fast_path_used: Option<ExecutionFastPath>,
        pushdown_type: Option<ExecutionPushdownType>,
        rows_scanned: usize,
        rows_returned: usize,
    ) {
        record_rows_scanned::<E>(rows_scanned);
        if let Some(execution_trace) = execution_trace.as_mut() {
            execution_trace.set_path_outcome(
                fast_path_used,
                pushdown_type,
                rows_scanned,
                rows_returned,
            );
            debug_assert_eq!(
                execution_trace.keys_scanned,
                u64::try_from(rows_scanned).unwrap_or(u64::MAX),
                "execution trace keys_scanned must match rows_scanned metrics input",
            );
        }
    }

    // Run the shared load phases for an already-produced ordered key stream.
    fn materialize_key_stream_into_page(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        key_stream: &mut dyn OrderedKeyStream,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<(CursorPage<E>, usize, usize), InternalError> {
        let data_rows = ctx.rows_from_ordered_key_stream(key_stream, plan.consistency)?;
        let rows_scanned = data_rows.len();
        let mut rows = Context::deserialize_rows(data_rows)?;
        let page = Self::finalize_rows_into_page(
            plan,
            &mut rows,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        let post_access_rows = page.items.0.len();

        Ok((page, rows_scanned, post_access_rows))
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

    fn index_range_limit_pushdown_fetch(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
    ) -> Option<usize> {
        if !Self::is_index_range_limit_pushdown_shape_eligible(plan) {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(0);
        }

        let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let page_end = offset.saturating_add(limit);
        let needs_extra_row = true;

        Some(page_end.saturating_add(usize::from(needs_extra_row)))
    }

    // Try each fast-path strategy in canonical order and return the first hit.
    #[expect(
        clippy::too_many_arguments,
        reason = "fast-path dispatch keeps execution inputs explicit at one call site"
    )]
    fn try_execute_fast_paths(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        secondary_pushdown_applicability: &PushdownApplicability,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<Option<CursorPage<E>>, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        if let Some(mut fast) = Self::try_execute_pk_order_stream(ctx, plan)? {
            let (page, _, post_access_rows) = Self::materialize_key_stream_into_page(
                ctx,
                plan,
                fast.ordered_key_stream.as_mut(),
                cursor_boundary,
                direction,
                continuation_signature,
            )?;
            Self::finalize_path_outcome(
                execution_trace,
                Some(fast.fast_path_used),
                fast.pushdown_type,
                fast.rows_scanned,
                post_access_rows,
            );
            set_rows_from_len(span, page.items.0.len());
            return Ok(Some(page));
        }

        if let Some(mut fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            plan,
            secondary_pushdown_applicability,
        )? {
            let (page, _, post_access_rows) = Self::materialize_key_stream_into_page(
                ctx,
                plan,
                fast.ordered_key_stream.as_mut(),
                cursor_boundary,
                direction,
                continuation_signature,
            )?;
            Self::finalize_path_outcome(
                execution_trace,
                Some(fast.fast_path_used),
                fast.pushdown_type,
                fast.rows_scanned,
                post_access_rows,
            );
            set_rows_from_len(span, page.items.0.len());
            return Ok(Some(page));
        }

        let index_range_limit_fetch =
            Self::index_range_limit_pushdown_fetch(plan, cursor_boundary, index_range_anchor);
        if let Some(mut fast) = Self::try_execute_index_range_limit_pushdown_stream(
            ctx,
            plan,
            index_range_anchor,
            direction,
            index_range_limit_fetch,
        )? {
            let (page, _, post_access_rows) = Self::materialize_key_stream_into_page(
                ctx,
                plan,
                fast.ordered_key_stream.as_mut(),
                cursor_boundary,
                direction,
                continuation_signature,
            )?;
            Self::finalize_path_outcome(
                execution_trace,
                Some(fast.fast_path_used),
                fast.pushdown_type,
                fast.rows_scanned,
                post_access_rows,
            );
            set_rows_from_len(span, page.items.0.len());
            return Ok(Some(page));
        }

        Ok(None)
    }

    // Apply canonical post-access phases to scanned rows and assemble the cursor page.
    fn finalize_rows_into_page(
        plan: &LogicalPlan<E::Key>,
        rows: &mut Vec<(Id<E>, E)>,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<CursorPage<E>, InternalError> {
        let stats = plan.apply_post_access_with_cursor::<E, _>(rows, cursor_boundary)?;
        let next_cursor =
            Self::build_next_cursor(plan, rows, &stats, direction, continuation_signature)?;
        let items = Response(std::mem::take(rows));

        Ok(CursorPage { items, next_cursor })
    }

    // Assess secondary-index ORDER BY pushdown once for this execution and
    // map matrix outcomes to executor decisions.
    fn assess_secondary_order_pushdown_applicability(
        plan: &LogicalPlan<E::Key>,
    ) -> PushdownApplicability {
        assess_secondary_order_pushdown_if_applicable_validated(E::MODEL, plan)
    }

    fn build_next_cursor(
        plan: &LogicalPlan<E::Key>,
        rows: &[(Id<E>, E)],
        stats: &PostAccessStats,
        direction: Direction,
        signature: ContinuationSignature,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        let Some(page) = plan.page.as_ref() else {
            return Ok(None);
        };
        let Some(limit) = page.limit else {
            return Ok(None);
        };
        if rows.is_empty() {
            return Ok(None);
        }

        // NOTE: post-access execution materializes full in-memory rows for Phase 1.
        let page_end = (page.offset as usize).saturating_add(limit as usize);
        if stats.rows_after_cursor <= page_end {
            return Ok(None);
        }

        let Some((_, last_entity)) = rows.last() else {
            return Ok(None);
        };
        Self::encode_next_cursor_for_last_entity(plan, last_entity, direction, signature).map(Some)
    }

    // Encode the continuation token from the last returned entity.
    fn encode_next_cursor_for_last_entity(
        plan: &LogicalPlan<E::Key>,
        last_entity: &E,
        direction: Direction,
        signature: ContinuationSignature,
    ) -> Result<Vec<u8>, InternalError> {
        let boundary = plan.cursor_boundary_from_entity(last_entity)?;
        let token = if plan.access.cursor_support().supports_index_range_anchor() {
            let (index, _, _, _) =
                plan.access.as_index_range_path().ok_or_else(|| {
                    InternalError::new(
                        ErrorClass::InvariantViolation,
                        ErrorOrigin::Query,
                        "executor invariant violated: index-range cursor support missing concrete index-range path",
                    )
                })?;
            let index_key = IndexKey::new(last_entity, index)?.ok_or_else(|| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: cursor row is not indexable for planned index-range access",
                )
            })?;

            ContinuationToken::new_index_range_with_direction(
                signature,
                boundary,
                IndexRangeCursorAnchor::new(index_key.to_raw()),
                direction,
            )
        } else {
            ContinuationToken::new_with_direction(signature, boundary, direction)
        };
        token.encode().map_err(|err| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Serialize,
                format!("failed to encode continuation cursor: {err}"),
            )
        })
    }
}
