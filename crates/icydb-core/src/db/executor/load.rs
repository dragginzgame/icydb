use crate::{
    db::{
        Db,
        executor::{
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::{AccessPath, AccessPlan, ExecutablePlan},
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    traits::EntityKind,
};
use std::marker::PhantomData;

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

impl<E: EntityKind> LoadExecutor<E> {
    // ======================================================================
    // Construction & diagnostics
    // ======================================================================

    // Debug is session-scoped via DbSession and propagated into executors;
    // executors do not expose independent debug control.
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            trace: None,
            _marker: PhantomData,
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn with_trace_sink(
        mut self,
        sink: Option<&'static dyn QueryTraceSink>,
    ) -> Self {
        self.trace = sink;
        self
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("[debug] {}", s.into());
        }
    }

    // ======================================================================
    // Execution
    // ======================================================================

    /// Execute an executor-ready plan directly (no planner inference).
    pub fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "load executor requires load plans".to_string(),
            ));
        }
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Load, &plan);
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            let plan = plan.into_inner();

            if self.debug {
                let access = access_summary(&plan.access);
                let ordered = plan
                    .order
                    .as_ref()
                    .is_some_and(|order| !order.fields.is_empty());
                let page = match plan.page.as_ref() {
                    Some(page) => format!("limit={:?}, offset={}", page.limit, page.offset),
                    None => "none".to_string(),
                };

                self.debug_log(format!(
                    "Executing load plan on {} (consistency={:?})",
                    E::PATH,
                    plan.consistency
                ));
                self.debug_log(format!("Access: {access}"));
                self.debug_log(format!(
                    "Post-access: filter={}, order={}, page={}",
                    yes_no(plan.predicate.is_some()),
                    yes_no(ordered),
                    page
                ));
            }

            let ctx = self.db.context::<E>();
            record_plan_metrics(&plan.access);

            // Access phase: resolve candidate rows from the store.
            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            self.debug_log(format!(
                "Scanned {} data rows before deserialization",
                data_rows.len()
            ));

            // Decode rows into entities before post-access filtering.
            let mut rows = ctx.deserialize_rows(data_rows)?;
            let access_rows = rows.len();
            self.debug_log(format!(
                "Deserialized {} entities before filtering",
                rows.len()
            ));

            // Post-access phase: filter, order, and paginate.
            let stats = plan.apply_post_access::<E, _>(&mut rows)?;
            if stats.filtered {
                self.debug_log(format!(
                    "Applied predicate -> {} entities remaining",
                    rows.len()
                ));
            }
            if stats.ordered {
                self.debug_log("Applied order spec");
            }
            if stats.paged {
                self.debug_log(format!("Applied pagination -> {} entities", rows.len()));
            }

            // Emit per-phase counts after the pipeline completes successfully.
            if let Some(trace) = trace.as_ref() {
                let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
                trace.phase(TracePhase::Filter, to_u64(stats.rows_after_filter));
                trace.phase(TracePhase::Order, to_u64(stats.rows_after_order));
                trace.phase(TracePhase::Page, to_u64(stats.rows_after_page));
            }

            set_rows_from_len(&mut span, rows.len());
            self.debug_log(format!("Query complete -> {} final rows", rows.len()));

            Ok(Response(rows))
        })();

        if let Some(trace) = trace {
            match &result {
                Ok(resp) => trace.finish(u64::try_from(resp.0.len()).unwrap_or(u64::MAX)),
                Err(err) => trace.error(err),
            }
        }

        result
    }
}

/// Return a human-readable summary of the access plan.
fn access_summary(access: &AccessPlan) -> String {
    match access {
        AccessPlan::Path(path) => access_path_summary(path),
        AccessPlan::Union(children) => format!("union of {} access paths", children.len()),
        AccessPlan::Intersection(children) => {
            format!("intersection of {} access paths", children.len())
        }
    }
}

/// Render a compact description for a concrete access path.
fn access_path_summary(path: &AccessPath) -> String {
    match path {
        AccessPath::ByKey(_) => "primary key lookup".to_string(),
        AccessPath::ByKeys(keys) => format!("primary key lookup ({} keys)", keys.len()),
        AccessPath::KeyRange { .. } => "primary key range scan".to_string(),
        AccessPath::IndexPrefix { index, values } => format!(
            "index prefix scan ({}, prefix_len={})",
            index.name,
            values.len()
        ),
        AccessPath::FullScan => "full scan".to_string(),
    }
}

/// Convert a boolean to a concise yes/no label for debug summaries.
const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
