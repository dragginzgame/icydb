use crate::{
    db::{
        Db,
        executor::{
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, start_plan_trace},
        },
        query::plan::ExecutablePlan,
        response::Response,
    },
    error::InternalError,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    traits::EntityKind,
};
use std::{collections::HashMap, hash::Hash, marker::PhantomData};

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
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Load, &plan);
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            let plan = plan.into_inner();

            self.debug_log(format!("🧭 Executing plan on {}", E::PATH));

            let ctx = self.db.context::<E>();
            record_plan_metrics(&plan.access);

            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            self.debug_log(format!(
                "📦 Scanned {} data rows before deserialization",
                data_rows.len()
            ));

            let mut rows = ctx.deserialize_rows(data_rows)?;
            self.debug_log(format!(
                "🧩 Deserialized {} entities before filtering",
                rows.len()
            ));

            let stats = plan.apply_post_access::<E, _>(&mut rows);
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

    /// Execute a plan and require exactly one row.
    pub fn require_one(&self, plan: ExecutablePlan<E>) -> Result<(), InternalError> {
        self.execute(plan)?.require_one()
    }

    /// Count rows matching a plan.
    pub fn count(&self, plan: ExecutablePlan<E>) -> Result<u32, InternalError> {
        Ok(self.execute(plan)?.count())
    }

    // ======================================================================
    // Aggregations
    // ======================================================================

    /// Group rows matching a plan and count them by a derived key.
    ///
    /// This is intentionally implemented on the executor (not Response)
    /// so it can later avoid full deserialization.
    pub fn group_count_by<K, F>(
        &self,
        plan: ExecutablePlan<E>,
        key_fn: F,
    ) -> Result<HashMap<K, u32>, InternalError>
    where
        K: Eq + Hash,
        F: Fn(&E) -> K,
    {
        let entities = self.execute(plan)?.entities();

        let mut counts = HashMap::new();
        for e in entities {
            *counts.entry(key_fn(&e)).or_insert(0) += 1;
        }

        Ok(counts)
    }
}
