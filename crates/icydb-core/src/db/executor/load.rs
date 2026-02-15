use crate::{
    db::{
        Context, Db,
        decode::decode_entity_with_expected_key,
        executor::{
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::{
            AccessPath, AccessPlan, ContinuationSignature, ContinuationToken, CursorBoundary,
            ExecutablePlan, LogicalPlan, OrderDirection, PrimaryKeyCursorSlotDecodeError,
            decode_primary_key_cursor_slot, validate::validate_executor_plan,
        },
        response::Response,
        store::DataKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) next_cursor: Option<Vec<u8>>,
}

// Internal result for fast-path load execution branches.
struct FastLoadResult<E: EntityKind> {
    page: CursorPage<E>,
    rows_scanned: usize,
    post_access_rows: usize,
}

enum PkScanLowerBound<K> {
    Min,
    Included(K),
    Excluded(K),
}

// Fast-path scan configuration derived from the logical plan + cursor boundary.
struct PkStreamScanConfig<K> {
    scan_lower: PkScanLowerBound<K>,
    range_end_key: Option<K>,
    limit: Option<usize>,
    collect_cap: Option<usize>,
    offset: usize,
}

// Fast-path scan output before page/cursor finalization.
struct PkStreamScanResult<E: EntityKind> {
    rows: Vec<(Id<E>, E)>,
    rows_scanned: usize,
    has_more: bool,
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

            if let Some(fast) = Self::try_execute_pk_order_stream(
                &ctx,
                &plan,
                cursor_boundary.as_ref(),
                continuation_signature,
            )? {
                sink::record(MetricsEvent::RowsScanned {
                    entity_path: E::PATH,
                    rows_scanned: u64::try_from(fast.rows_scanned).unwrap_or(u64::MAX),
                });

                if let Some(trace) = trace.as_ref() {
                    // NOTE: Trace metrics saturate on overflow; diagnostics only.
                    let to_u64 = |n| u64::try_from(n).unwrap_or(u64::MAX);
                    trace.phase(TracePhase::Access, to_u64(fast.rows_scanned));
                    trace.phase(TracePhase::PostAccess, to_u64(fast.post_access_rows));
                }

                set_rows_from_len(&mut span, fast.page.items.0.len());
                return Ok(fast.page);
            }

            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            let mut rows = Context::deserialize_rows(data_rows)?;
            let access_rows = rows.len();

            let stats =
                plan.apply_post_access_with_cursor::<E, _>(&mut rows, cursor_boundary.as_ref())?;
            let post_access_rows = rows.len();
            let next_cursor =
                Self::build_next_cursor(&plan, &rows, &stats, continuation_signature)?;

            if let Some(trace) = trace.as_ref() {
                // NOTE: Trace metrics saturate on overflow; diagnostics only.
                let to_u64 = |n| u64::try_from(n).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
                trace.phase(TracePhase::PostAccess, to_u64(post_access_rows));
            }

            set_rows_from_len(&mut span, rows.len());
            Ok(CursorPage {
                items: Response(rows),
                next_cursor,
            })
        })();

        if let Some(trace) = trace {
            match &result {
                Ok(page) => trace.finish(page.items.0.len() as u64),
                Err(err) => trace.error(err),
            }
        }

        result
    }

    // Fast path for canonical primary-key ordering over full scans.
    // Secondary index traversal cannot satisfy ORDER BY semantics today because
    // index key ordering is fingerprint-based, not canonical value order.
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
        if Self::pk_scan_range_is_empty(&config.scan_lower, config.range_end_key) {
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
        let mut scan = Self::scan_pk_stream_rows(ctx, plan, &config)?;

        // Phase 3: finalize page rows and continuation token.
        let Some(page) = Self::finalize_pk_stream_page(
            plan,
            &mut scan.rows,
            scan.has_more,
            config.limit,
            continuation_signature,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(FastLoadResult {
            post_access_rows: page.items.0.len(),
            page,
            rows_scanned: scan.rows_scanned,
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

        let cursor_key = Self::decode_pk_cursor_key(cursor_boundary)?;
        let (range_start_key, range_end_key) = match &plan.access {
            AccessPlan::Path(AccessPath::FullScan) => (None, None),
            AccessPlan::Path(AccessPath::KeyRange { start, end }) => (Some(*start), Some(*end)),
            _ => return Ok(None),
        };
        let limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));
        let collect_cap = limit.map(|limit| limit.saturating_add(1));
        let offset = plan.page.as_ref().map_or(0usize, |page| {
            usize::try_from(page.offset).unwrap_or(usize::MAX)
        });

        Ok(Some(PkStreamScanConfig {
            scan_lower: Self::select_pk_scan_lower_bound(range_start_key, cursor_key),
            range_end_key,
            limit,
            collect_cap,
            offset,
        }))
    }

    // Execute the store-range streaming phase for the PK fast path.
    fn scan_pk_stream_rows(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        config: &PkStreamScanConfig<E::Key>,
    ) -> Result<PkStreamScanResult<E>, InternalError> {
        ctx.with_store(|store| {
            let lower_raw = match config.scan_lower {
                PkScanLowerBound::Min => DataKey::lower_bound::<E>().to_raw()?,
                PkScanLowerBound::Included(key) | PkScanLowerBound::Excluded(key) => {
                    DataKey::try_new::<E>(key)?.to_raw()?
                }
            };
            let lower_bound = match config.scan_lower {
                PkScanLowerBound::Min | PkScanLowerBound::Included(_) => Bound::Included(lower_raw),
                PkScanLowerBound::Excluded(_) => Bound::Excluded(lower_raw),
            };
            let upper_raw = match config.range_end_key {
                Some(end) => DataKey::try_new::<E>(end)?.to_raw()?,
                None => DataKey::upper_bound::<E>().to_raw()?,
            };

            let mut rows_scanned = 0usize;
            let mut rows = Vec::new();
            let mut has_more = false;
            let mut offset_remaining = config.offset;

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

                if let Some(predicate) = plan.predicate.as_ref()
                    && !crate::db::query::predicate::eval(&entity, predicate)
                {
                    continue;
                }

                if offset_remaining > 0 {
                    offset_remaining = offset_remaining.saturating_sub(1);
                    continue;
                }

                rows.push((Id::from_key(expected_key), entity));

                if let Some(cap) = config.collect_cap
                    && rows.len() >= cap
                {
                    has_more = true;
                    break;
                }
            }

            Ok(PkStreamScanResult {
                rows,
                rows_scanned,
                has_more,
            })
        })?
    }

    // Finalize fast-path rows into a page result and optional continuation token.
    fn finalize_pk_stream_page(
        plan: &LogicalPlan<E::Key>,
        rows: &mut Vec<(Id<E>, E)>,
        has_more: bool,
        limit: Option<usize>,
        continuation_signature: ContinuationSignature,
    ) -> Result<Option<CursorPage<E>>, InternalError> {
        if let Some(limit_rows) = limit
            && rows.len() > limit_rows
        {
            rows.truncate(limit_rows);
        }

        let next_cursor = if has_more {
            debug_assert!(
                !rows.is_empty(),
                "pk stream invariant violated: has_more requires at least one row",
            );
            let Some((_, last_entity)) = rows.last() else {
                return Ok(None);
            };
            Some(Self::encode_next_cursor_for_last_entity(
                plan,
                last_entity,
                continuation_signature,
            )?)
        } else {
            None
        };

        Ok(Some(CursorPage {
            items: Response(std::mem::take(rows)),
            next_cursor,
        }))
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

    fn decode_pk_cursor_key(
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Result<Option<E::Key>, InternalError> {
        let Some(boundary) = cursor_boundary else {
            return Ok(None);
        };

        if boundary.slots.len() != 1 {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                format!(
                    "executor invariant violated: pk-ordered continuation boundary must contain exactly 1 slot, found {}",
                    boundary.slots.len()
                ),
            ));
        }

        decode_primary_key_cursor_slot::<E::Key>(&boundary.slots[0])
            .map(Some)
            .map_err(|err| match err {
                PrimaryKeyCursorSlotDecodeError::Missing => InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: pk cursor slot must be present",
                ),
                PrimaryKeyCursorSlotDecodeError::TypeMismatch { .. } => InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Query,
                    "executor invariant violated: pk cursor slot type mismatch",
                ),
            })
    }

    fn select_pk_scan_lower_bound(
        range_start_key: Option<E::Key>,
        cursor_key: Option<E::Key>,
    ) -> PkScanLowerBound<E::Key> {
        match (range_start_key, cursor_key) {
            (None, None) => PkScanLowerBound::Min,
            (Some(start), None) => PkScanLowerBound::Included(start),
            (None, Some(cursor)) => PkScanLowerBound::Excluded(cursor),
            (Some(start), Some(cursor)) => {
                if cursor < start {
                    PkScanLowerBound::Included(start)
                } else {
                    PkScanLowerBound::Excluded(cursor)
                }
            }
        }
    }

    fn pk_scan_range_is_empty(
        lower_bound: &PkScanLowerBound<E::Key>,
        range_end_key: Option<E::Key>,
    ) -> bool {
        let Some(end) = range_end_key else {
            return false;
        };

        match lower_bound {
            PkScanLowerBound::Min => false,
            PkScanLowerBound::Included(start) => *start > end,
            PkScanLowerBound::Excluded(start) => *start >= end,
        }
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
