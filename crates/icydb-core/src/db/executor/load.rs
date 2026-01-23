use crate::{
    db::{
        Db,
        executor::plan::{record_plan_metrics, set_rows_from_len},
        query::v2::{
            plan::{LogicalPlan, OrderDirection, OrderSpec, validate_plan},
            predicate::{eval as eval_v2, normalize as normalize_v2},
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
            _marker: PhantomData,
        }
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("{}", s.into());
        }
    }

    // ======================================================================
    // Execution
    // ======================================================================

    /// Execute a v2 logical plan directly (no planner inference).
    pub fn execute(&self, plan: LogicalPlan) -> Result<Response<E>, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Load);
        validate_plan::<E>(&plan).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;

        self.debug_log(format!("🧭 Executing v2 plan on {}", E::PATH));

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

        // Predicate (always post-fetch for v2)
        let normalized = plan.predicate.as_ref().map(normalize_v2);
        if let Some(predicate) = normalized.as_ref() {
            rows.retain(|(_, entity)| eval_v2(entity, predicate));
            self.debug_log(format!(
                "🔎 Applied v2 predicate -> {} entities remaining",
                rows.len()
            ));
        }

        // Ordering
        if let Some(order) = &plan.order
            && rows.len() > 1
            && !order.fields.is_empty()
        {
            apply_order_spec(&mut rows, order);
            self.debug_log("↕️ Applied v2 order spec");
        }

        // Pagination
        if let Some(page) = &plan.page {
            apply_pagination(&mut rows, page.offset, page.limit);
            self.debug_log(format!(
                "📏 Applied v2 pagination (offset={}, limit={:?}) -> {} entities",
                page.offset,
                page.limit,
                rows.len()
            ));
        }

        set_rows_from_len(&mut span, rows.len());
        self.debug_log(format!("✅ v2 query complete -> {} final rows", rows.len()));

        Ok(Response(rows))
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
