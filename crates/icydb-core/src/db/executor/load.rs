use crate::{
    db::{
        Db,
        executor::{
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::ExecutablePlan,
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
            println!("{}", s.into());
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

            self.debug_log(format!("🧭 Executing plan on {}", E::PATH));

            let ctx = self.db.context::<E>();
            record_plan_metrics(&plan.access);

            // Access phase: resolve candidate rows from the store.
            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            self.debug_log(format!(
                "📦 Scanned {} data rows before deserialization",
                data_rows.len()
            ));

            // Decode rows into entities before post-access filtering.
            let mut rows = ctx.deserialize_rows(data_rows)?;
            let access_rows = rows.len();
            self.debug_log(format!(
                "🧩 Deserialized {} entities before filtering",
                rows.len()
            ));

            // Post-access phase: filter, order, and paginate.
            let stats = plan.apply_post_access::<E, _>(&mut rows)?;
            if stats.filtered {
                self.debug_log(format!(
                    "🔎 Applied predicate -> {} entities remaining",
                    rows.len()
                ));
            }
            if stats.ordered {
                self.debug_log("↕️ Applied order spec");
            }
            if stats.paged {
                self.debug_log(format!("📏 Applied pagination -> {} entities", rows.len()));
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
            self.debug_log(format!("✅ query complete -> {} final rows", rows.len()));

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
