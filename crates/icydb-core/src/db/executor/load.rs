use crate::{
    db::{
        Context, Db,
        decode::decode_entity_with_expected_key,
        executor::{
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
            trace::{
                QueryTraceSink, TraceExecutorKind, TracePushdownDecision, TraceScope,
                emit_access_post_access_phases, finish_trace_from_result, start_plan_trace,
            },
        },
        query::plan::{
            AccessPath, AccessPlan, ContinuationSignature, ContinuationToken, CursorBoundary,
            ExecutablePlan, LogicalPlan, OrderDirection, decode_pk_cursor_boundary,
            validate::{
                SecondaryOrderPushdownEligibility, assess_secondary_order_pushdown,
                validate_executor_plan,
            },
        },
        response::Response,
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::{marker::PhantomData, ops::Bound};

///
/// CursorPage
/// Internal load page result with continuation cursor payload.
///

#[derive(Debug)]
pub struct CursorPage<E: EntityKind> {
    pub(crate) items: Response<E>,

    pub(crate) next_cursor: Option<Vec<u8>>,
}

// Internal result for fast-path load execution branches.
struct FastLoadResult<E: EntityKind> {
    page: CursorPage<E>,
    rows_scanned: usize,
    post_access_rows: usize,
}

// Fast-path scan configuration derived from access-path bounds.
struct PkStreamScanConfig<K> {
    range_start_key: Option<K>,
    range_end_key: Option<K>,
}

// Fast-path access scan output before canonical post-access semantics.
struct PkStreamScanResult<E: EntityKind> {
    rows: Vec<(Id<E>, E)>,
    rows_scanned: usize,
}

///
/// LoadExecutor
///

#[derive(Clone)]
pub struct LoadExecutor<E: EntityKind> {
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
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
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

    pub fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        self.execute_paged(plan, None).map(|page| page.items)
    }

    pub(crate) fn execute_paged(
        &self,
        plan: ExecutablePlan<E>,
        cursor_boundary: Option<CursorBoundary>,
    ) -> Result<CursorPage<E>, InternalError> {
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

            if self.debug {
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

            record_plan_metrics(&plan.access);
            let secondary_pushdown_eligibility = Self::secondary_order_pushdown_eligibility(&plan);
            Self::emit_secondary_order_pushdown_trace(
                trace.as_ref(),
                secondary_pushdown_eligibility.as_ref(),
            );

            if let Some(fast) = Self::try_execute_pk_order_stream(
                &ctx,
                &plan,
                cursor_boundary.as_ref(),
                continuation_signature,
            )? {
                record_rows_scanned::<E>(fast.rows_scanned);
                emit_access_post_access_phases(
                    trace.as_ref(),
                    fast.rows_scanned,
                    fast.post_access_rows,
                );

                set_rows_from_len(&mut span, fast.page.items.0.len());
                return Ok(fast.page);
            }

            if let Some(fast) = Self::try_execute_secondary_index_order_stream(
                &ctx,
                &plan,
                secondary_pushdown_eligibility.as_ref(),
                cursor_boundary.as_ref(),
                continuation_signature,
            )? {
                record_rows_scanned::<E>(fast.rows_scanned);
                emit_access_post_access_phases(
                    trace.as_ref(),
                    fast.rows_scanned,
                    fast.post_access_rows,
                );

                set_rows_from_len(&mut span, fast.page.items.0.len());
                return Ok(fast.page);
            }

            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            record_rows_scanned::<E>(data_rows.len());

            let mut rows = Context::deserialize_rows(data_rows)?;
            let access_rows = rows.len();

            let stats =
                plan.apply_post_access_with_cursor::<E, _>(&mut rows, cursor_boundary.as_ref())?;
            let post_access_rows = rows.len();
            let next_cursor =
                Self::build_next_cursor(&plan, &rows, &stats, continuation_signature)?;

            emit_access_post_access_phases(trace.as_ref(), access_rows, post_access_rows);

            set_rows_from_len(&mut span, rows.len());
            Ok(CursorPage {
                items: Response(rows),
                next_cursor,
            })
        })();

        finish_trace_from_result(trace, &result, |page| page.items.0.len());

        result
    }

    // Evaluate secondary-index ORDER BY pushdown once per plan execution.
    fn secondary_order_pushdown_eligibility(
        plan: &LogicalPlan<E::Key>,
    ) -> Option<SecondaryOrderPushdownEligibility> {
        if !matches!(
            plan.access,
            AccessPlan::Path(AccessPath::IndexPrefix { .. })
        ) {
            return None;
        }
        if plan
            .order
            .as_ref()
            .is_none_or(|order| order.fields.is_empty())
        {
            return None;
        }

        Some(assess_secondary_order_pushdown(E::MODEL, plan))
    }

    // Emit a deterministic trace marker for secondary ORDER BY pushdown decisions.
    fn emit_secondary_order_pushdown_trace(
        trace: Option<&TraceScope>,
        eligibility: Option<&SecondaryOrderPushdownEligibility>,
    ) {
        let Some(trace) = trace else {
            return;
        };
        let Some(eligibility) = eligibility else {
            return;
        };

        let decision = match eligibility {
            SecondaryOrderPushdownEligibility::Eligible { .. } => {
                TracePushdownDecision::AcceptedSecondaryIndexOrder
            }
            SecondaryOrderPushdownEligibility::Rejected(_) => {
                TracePushdownDecision::RejectedSecondaryIndexOrder
            }
        };
        trace.pushdown(decision);
    }

    // Fast path for canonical primary-key ordering over full scans.
    fn try_execute_pk_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        // Phase 1: derive a fast-path scan config from the canonical plan + cursor.
        let Some(config) = Self::build_pk_stream_scan_config(plan, cursor_boundary)? else {
            return Ok(None);
        };
        if Self::pk_scan_range_is_empty(config.range_start_key, config.range_end_key) {
            return Ok(Some(FastLoadResult {
                page: CursorPage {
                    items: Response(Vec::new()),
                    next_cursor: None,
                },
                rows_scanned: 0,
                post_access_rows: 0,
            }));
        }

        // Phase 2: stream rows directly from the store in primary-key order.
        let mut scan = Self::scan_pk_stream_rows(ctx, &config)?;

        // Phase 3: apply canonical post-access semantics and derive continuation.
        let stats = plan.apply_post_access_with_cursor::<E, _>(&mut scan.rows, cursor_boundary)?;
        let next_cursor =
            Self::build_next_cursor(plan, &scan.rows, &stats, continuation_signature)?;
        let page = CursorPage {
            items: Response(scan.rows),
            next_cursor,
        };
        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned: scan.rows_scanned,
        }))
    }

    // Fast path for secondary-index traversal when planner pushdown eligibility
    // proves canonical ORDER BY parity with raw index-key order.
    fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        secondary_pushdown_eligibility: Option<&SecondaryOrderPushdownEligibility>,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<FastLoadResult<E>>, InternalError> {
        let Some(SecondaryOrderPushdownEligibility::Eligible { .. }) =
            secondary_pushdown_eligibility
        else {
            return Ok(None);
        };

        let AccessPlan::Path(AccessPath::IndexPrefix { index, values }) = &plan.access else {
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
        let stats = plan.apply_post_access_with_cursor::<E, _>(&mut rows, cursor_boundary)?;
        let next_cursor = Self::build_next_cursor(plan, &rows, &stats, continuation_signature)?;
        let page = CursorPage {
            items: Response(rows),
            next_cursor,
        };

        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned,
        }))
    }

    // Build the fast-path scan config for canonical PK-ordered streaming.
    fn build_pk_stream_scan_config(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<Option<PkStreamScanConfig<E::Key>>, InternalError> {
        if !Self::is_pk_order_stream_eligible(plan) {
            return Ok(None);
        }

        // Keep malformed boundary classification stable on PK fast-path execution.
        let _cursor_key = decode_pk_cursor_boundary::<E>(cursor_boundary)?;
        let (range_start_key, range_end_key) = match &plan.access {
            AccessPlan::Path(AccessPath::FullScan) => (None, None),
            AccessPlan::Path(AccessPath::KeyRange { start, end }) => (Some(*start), Some(*end)),
            _ => return Ok(None),
        };

        Ok(Some(PkStreamScanConfig {
            range_start_key,
            range_end_key,
        }))
    }

    // Execute the store-range streaming phase for the PK fast path.
    fn scan_pk_stream_rows(
        ctx: &Context<'_, E>,
        config: &PkStreamScanConfig<E::Key>,
    ) -> Result<PkStreamScanResult<E>, InternalError> {
        ctx.with_store(|store| {
            let lower_raw = match config.range_start_key {
                Some(start) => DataKey::try_new::<E>(start)?.to_raw()?,
                None => DataKey::lower_bound::<E>().to_raw()?,
            };
            let lower_bound = Bound::Included(lower_raw);
            let upper_raw = match config.range_end_key {
                Some(end) => DataKey::try_new::<E>(end)?.to_raw()?,
                None => DataKey::upper_bound::<E>().to_raw()?,
            };

            let mut rows_scanned = 0usize;
            let mut rows = Vec::new();

            for entry in store.range((lower_bound, Bound::Included(upper_raw))) {
                rows_scanned = rows_scanned.saturating_add(1);

                let data_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
                    InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Store,
                        format!("ordered scan encountered corrupted data key: {err}"),
                    )
                })?;
                let expected_key = data_key.try_key::<E>()?;
                let entity = decode_entity_with_expected_key::<E, _, _, _, _>(
                    expected_key,
                    || entry.value().try_decode::<E>(),
                    |err| {
                        InternalError::new(
                            ErrorClass::Corruption,
                            ErrorOrigin::Serialize,
                            format!("ordered scan failed to decode row for {data_key}: {err}"),
                        )
                    },
                    |expected_key, actual_key| {
                        let expected = DataKey::try_new::<E>(expected_key)?;
                        let found = DataKey::try_new::<E>(actual_key)?;
                        Ok(InternalError::new(
                            ErrorClass::Corruption,
                            ErrorOrigin::Store,
                            format!("row key mismatch: expected {expected}, found {found}"),
                        ))
                    },
                )?;

                rows.push((Id::from_key(expected_key), entity));
            }

            Ok(PkStreamScanResult { rows, rows_scanned })
        })?
    }

    fn is_pk_order_stream_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        if !plan.mode.is_load() {
            return false;
        }

        let supports_pk_stream_access = matches!(
            &plan.access,
            AccessPlan::Path(AccessPath::FullScan | AccessPath::KeyRange { .. })
        );
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = plan.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1
            && order.fields[0].0 == E::MODEL.primary_key.name
            && matches!(order.fields[0].1, OrderDirection::Asc)
    }

    fn pk_scan_range_is_empty(
        range_start_key: Option<E::Key>,
        range_end_key: Option<E::Key>,
    ) -> bool {
        let Some(start) = range_start_key else {
            return false;
        };
        let Some(end) = range_end_key else {
            return false;
        };

        start > end
    }

    fn build_next_cursor(
        plan: &LogicalPlan<E::Key>,
        rows: &[(Id<E>, E)],
        stats: &crate::db::query::plan::logical::PostAccessStats,
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
        ContinuationToken::new(signature, boundary)
            .encode()
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Serialize,
                    format!("failed to encode continuation cursor: {err}"),
                )
            })
    }
}
