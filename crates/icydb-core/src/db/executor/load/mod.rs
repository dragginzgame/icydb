mod index_range_limit;
mod pk_stream;

use crate::{
    db::{
        Context, Db,
        executor::{
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
            trace::{
                QueryTraceSink, TraceExecutorKind, TracePushdownDecision, TraceScope,
                emit_access_post_access_phases, finish_trace_from_result, start_plan_trace,
            },
        },
        index::IndexKey,
        query::plan::{
            AccessPath, ContinuationSignature, ContinuationToken, CursorBoundary, ExecutablePlan,
            IndexRangeCursorAnchor, LogicalPlan, PlannedCursor,
            logical::PostAccessStats,
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
};
use std::marker::PhantomData;

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
/// FastLoadResult
///
/// Internal fast-path load execution result.
/// Bundles the page payload and row accounting.
///

struct FastLoadResult<E: EntityKind> {
    page: CursorPage<E>,
    rows_scanned: usize,
    post_access_rows: usize,
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
    trace: Option<&'static dyn QueryTraceSink>,
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
            trace: None,
            _marker: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) const fn with_trace(mut self, trace: &'static dyn QueryTraceSink) -> Self {
        self.trace = Some(trace);
        self
    }

    fn debug_log(&self, s: impl AsRef<str>) {
        if self.debug {
            println!("[debug] {}", s.as_ref());
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
        let cursor: PlannedCursor = cursor.into();
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_anchor = cursor.index_range_anchor().cloned();

        if !plan.mode().is_load() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "load executor requires load plans",
            ));
        }

        let continuation_signature = plan.continuation_signature();
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Load, &plan);

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            let plan = plan.into_inner();

            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;
            self.debug_log_load_plan(&plan, cursor_boundary.as_ref());

            record_plan_metrics(&plan.access);
            // Compute secondary ORDER BY pushdown eligibility once, then share the
            // derived decision across trace and fast-path gating.
            let secondary_pushdown_applicability =
                Self::assess_secondary_order_pushdown_applicability(&plan);
            Self::emit_secondary_order_pushdown_trace(
                trace.as_ref(),
                &secondary_pushdown_applicability,
            );

            if let Some(fast) = Self::try_execute_pk_order_stream(
                &ctx,
                &plan,
                cursor_boundary.as_ref(),
                continuation_signature,
            )? {
                return Ok(Self::finish_fast_path(&mut span, trace.as_ref(), fast));
            }

            if let Some(fast) = Self::try_execute_secondary_index_order_stream(
                &ctx,
                &plan,
                &secondary_pushdown_applicability,
                cursor_boundary.as_ref(),
                continuation_signature,
            )? {
                return Ok(Self::finish_fast_path(&mut span, trace.as_ref(), fast));
            }

            if let Some(fast) = Self::try_execute_index_range_limit_pushdown_stream(
                &ctx,
                &plan,
                cursor_boundary.as_ref(),
                index_range_anchor.as_ref(),
                continuation_signature,
            )? {
                return Ok(Self::finish_fast_path(&mut span, trace.as_ref(), fast));
            }

            let data_rows = ctx.rows_from_access_plan_with_index_range_anchor(
                &plan.access,
                plan.consistency,
                index_range_anchor.as_ref(),
            )?;
            record_rows_scanned::<E>(data_rows.len());

            let mut rows = Context::deserialize_rows(data_rows)?;
            let access_rows = rows.len();
            let page = Self::finalize_rows_into_page(
                &plan,
                &mut rows,
                cursor_boundary.as_ref(),
                continuation_signature,
            )?;
            let post_access_rows = page.items.0.len();

            emit_access_post_access_phases(trace.as_ref(), access_rows, post_access_rows);

            set_rows_from_len(&mut span, page.items.0.len());
            Ok(page)
        })();

        finish_trace_from_result(trace, &result, |page| page.items.0.len());

        result
    }

    // Emit a compact debug summary for one load execution plan.
    fn debug_log_load_plan(
        &self,
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) {
        if !self.debug {
            return;
        }

        self.debug_log(format!(
            "Executing load plan on {} (consistency={:?})",
            E::PATH,
            plan.consistency
        ));
        self.debug_log(format!("Access: {}", access_summary(&plan.access)));

        let ordered = plan
            .order
            .as_ref()
            .is_some_and(|order| !order.fields.is_empty());
        let page = match plan.page.as_ref() {
            Some(p) => format!("limit={:?}, offset={}", p.limit, p.offset),
            None => "none".to_string(),
        };
        self.debug_log(format!(
            "Post-access: filter={}, order={}, page={}",
            yes_no(plan.predicate.is_some()),
            yes_no(ordered),
            page
        ));
        self.debug_log(format!(
            "Cursor provided: {}",
            yes_no(cursor_boundary.is_some())
        ));
    }

    // Emit a deterministic trace marker for secondary ORDER BY pushdown decisions.
    fn emit_secondary_order_pushdown_trace(
        trace: Option<&TraceScope>,
        applicability: &PushdownApplicability,
    ) {
        let Some(trace) = trace else {
            return;
        };
        let Some(surface) = applicability.surface_eligibility() else {
            return;
        };
        let trace_decision = TracePushdownDecision::from(surface);

        trace.pushdown(trace_decision);
    }

    // Apply shared metrics/trace/span completion for any fast-path branch.
    fn finish_fast_path(
        span: &mut Span<E>,
        trace: Option<&TraceScope>,
        fast: FastLoadResult<E>,
    ) -> CursorPage<E> {
        record_rows_scanned::<E>(fast.rows_scanned);
        emit_access_post_access_phases(trace, fast.rows_scanned, fast.post_access_rows);
        set_rows_from_len(span, fast.page.items.0.len());

        fast.page
    }

    // Apply canonical post-access phases to scanned rows and assemble the cursor page.
    fn finalize_rows_into_page(
        plan: &LogicalPlan<E::Key>,
        rows: &mut Vec<(Id<E>, E)>,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
    ) -> Result<CursorPage<E>, InternalError> {
        let stats = plan.apply_post_access_with_cursor::<E, _>(rows, cursor_boundary)?;
        let next_cursor = Self::build_next_cursor(plan, rows, &stats, continuation_signature)?;
        let items = Response(std::mem::take(rows));

        Ok(CursorPage { items, next_cursor })
    }

    // Fast path for secondary-index traversal when planner pushdown eligibility
    // proves canonical ORDER BY parity with raw index-key order.
    fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        secondary_pushdown_applicability: &PushdownApplicability,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        if !secondary_pushdown_applicability.is_eligible() {
            return Ok(None);
        }

        let Some(AccessPath::IndexPrefix { index, values }) = plan.access.as_path() else {
            return Ok(None);
        };

        // Phase 1: resolve candidate keys using canonical index traversal order.
        let ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index.store).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values::<E>(index, values.as_slice())
                })
            })
        })?;
        let rows_scanned = ordered_keys.len();

        // Phase 2: load rows while preserving traversal order.
        let data_rows = ctx.rows_from_ordered_data_keys(&ordered_keys, plan.consistency)?;
        let mut rows = Context::deserialize_rows(data_rows)?;

        // Phase 3: apply canonical post-access semantics (predicate/cursor/page) and continuation.
        let page = Self::finalize_rows_into_page(
            plan,
            &mut rows,
            cursor_boundary,
            continuation_signature,
        )?;

        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned,
        }))
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
        Self::encode_next_cursor_for_last_entity(plan, last_entity, signature).map(Some)
    }

    // Encode the continuation token from the last returned entity.
    fn encode_next_cursor_for_last_entity(
        plan: &LogicalPlan<E::Key>,
        last_entity: &E,
        signature: ContinuationSignature,
    ) -> Result<Vec<u8>, InternalError> {
        let boundary = plan.cursor_boundary_from_entity(last_entity)?;
        let token = match plan.access.as_path() {
            Some(AccessPath::IndexRange { index, .. }) => {
                let index_key =
                    IndexKey::new(last_entity, index)?.ok_or_else(|| {
                        InternalError::new(
                            ErrorClass::InvariantViolation,
                            ErrorOrigin::Query,
                            "executor invariant violated: cursor row is not indexable for planned index-range access",
                        )
                    })?;
                ContinuationToken::new_index_range(
                    signature,
                    boundary,
                    IndexRangeCursorAnchor::new(index_key.to_raw()),
                )
            }
            _ => ContinuationToken::new(signature, boundary),
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
