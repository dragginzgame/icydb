use crate::{
    db::{
        Context, Db,
        executor::{
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::{ExecutablePlan, validate::validate_executor_plan},
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    traits::{EntityKind, EntityValue},
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
    #[must_use]
    #[expect(dead_code)]
    pub(crate) const fn with_trace_sink(
        mut self,
        sink: Option<&'static dyn QueryTraceSink>,
    ) -> Self {
        self.trace = sink;
        self
    }

    fn debug_log(&self, s: impl AsRef<str>) {
        if self.debug {
            println!("[debug] {}", s.as_ref());
        }
    }

    pub fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        if !plan.mode().is_load() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "load executor requires load plans",
            ));
        }

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
            }

            record_plan_metrics(&plan.access);

            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            let mut rows = Context::deserialize_rows(data_rows)?;
            let access_rows = rows.len();

            let stats = plan.apply_post_access::<E, _>(&mut rows)?;

            if let Some(trace) = trace.as_ref() {
                // NOTE: Trace metrics saturate on overflow; diagnostics only.
                let to_u64 = |n| u64::try_from(n).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
                trace.phase(TracePhase::Filter, to_u64(stats.rows_after_filter));
                trace.phase(TracePhase::Order, to_u64(stats.rows_after_order));
                trace.phase(TracePhase::Page, to_u64(stats.rows_after_page));
            }

            set_rows_from_len(&mut span, rows.len());
            Ok(Response(rows))
        })();

        if let Some(trace) = trace {
            match &result {
                Ok(resp) => trace.finish(resp.0.len() as u64),
                Err(err) => trace.error(err),
            }
        }

        result
    }
}
