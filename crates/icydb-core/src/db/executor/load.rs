use crate::{
    db::{
        Db,
        executor::{
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, start_plan_trace},
        },
        query::{
            plan::{LogicalPlan, OrderDirection, OrderSpec, validate_plan_with_model},
            predicate::{eval as eval_predicate, normalize as normalize_predicate},
        },
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    traits::EntityKind,
};
use std::{cmp::Ordering, collections::HashMap, hash::Hash, marker::PhantomData};

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

    /// Execute a logical plan directly (no planner inference).
    pub fn execute(&self, plan: LogicalPlan) -> Result<Response<E>, InternalError> {
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Load, &plan);
        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);
            validate_plan_with_model(&plan, E::MODEL).map_err(|err| {
                InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            })?;
            plan.debug_validate_with_model(E::MODEL);

            self.debug_log(format!("🧭 Executing plan on {}", E::PATH));

            let ctx = self.db.context::<E>();
            record_plan_metrics(&plan.access);

            let data_rows = ctx.rows_from_access(&plan.access)?;
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

            // Predicate (always post-fetch for this planner)
            let normalized = plan.predicate.as_ref().map(normalize_predicate);
            let filtered = if let Some(predicate) = normalized.as_ref() {
                rows.retain(|(_, entity)| eval_predicate(entity, predicate));
                self.debug_log(format!(
                    "🔎 Applied predicate -> {} entities remaining",
                    rows.len()
                ));
                true
            } else {
                false
            };

            // Ordering
            let ordered = if let Some(order) = &plan.order
                && rows.len() > 1
                && !order.fields.is_empty()
            {
                debug_assert!(
                    plan.predicate.is_none() || filtered,
                    "executor invariant violated: ordering must run after filtering"
                );
                apply_order_spec(&mut rows, order);
                self.debug_log("↕️ Applied order spec");
                true
            } else {
                false
            };

            // Pagination
            if let Some(page) = &plan.page {
                debug_assert!(
                    plan.order.is_none() || ordered,
                    "executor invariant violated: pagination must run after ordering"
                );
                apply_pagination(&mut rows, page.offset, page.limit);
                self.debug_log(format!(
                    "📏 Applied pagination (offset={}, limit={:?}) -> {} entities",
                    page.offset,
                    page.limit,
                    rows.len()
                ));
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
    pub fn require_one(&self, plan: LogicalPlan) -> Result<(), InternalError> {
        self.execute(plan)?.require_one()
    }

    /// Count rows matching a plan.
    pub fn count(&self, plan: LogicalPlan) -> Result<u32, InternalError> {
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
        plan: LogicalPlan,
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

fn apply_order_spec<E: EntityKind>(rows: &mut [(Key, E)], order: &OrderSpec) {
    rows.sort_by(|(_, ea), (_, eb)| {
        for (field, direction) in &order.fields {
            let va = ea.get_value(field);
            let vb = eb.get_value(field);

            let ordering = match (va, vb) {
                (None, None) => continue,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(va), Some(vb)) => match va.partial_cmp(&vb) {
                    Some(ord) => ord,
                    None => continue,
                },
            };

            let ordering = match direction {
                OrderDirection::Asc => ordering,
                OrderDirection::Desc => ordering.reverse(),
            };

            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        Ordering::Equal
    });
}

/// Apply offset/limit pagination to an in-memory vector, in-place.
fn apply_pagination<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total = rows.len();
    let start = usize::min(offset as usize, total);
    let end = limit.map_or(total, |l| usize::min(start + l as usize, total));

    if start >= end {
        rows.clear();
    } else {
        rows.drain(..start);
        rows.truncate(end - start);
    }
}
