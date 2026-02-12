use crate::{
    db::{
        Context, Db,
        executor::{
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::{
            ContinuationSignature, ContinuationToken, CursorBoundary, ExecutablePlan, LogicalPlan,
            validate::validate_executor_plan,
        },
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::marker::PhantomData;

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
        let boundary = plan.cursor_boundary_from_entity(last_entity)?;

        ContinuationToken::new(signature, boundary)
            .encode()
            .map(Some)
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Serialize,
                    format!("failed to encode continuation cursor: {err}"),
                )
            })
    }
}
