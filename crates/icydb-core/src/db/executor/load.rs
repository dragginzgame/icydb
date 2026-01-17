use crate::{
    db::{
        Db,
        executor::{
            FilterEvaluator,
            plan::{plan_for, record_plan_metrics, scan_strict, set_rows_from_len},
        },
        primitives::{FilterDsl, FilterExpr, FilterExt, IntoFilterExpr, Order, SortExpr},
        query::{LoadQuery, QueryPlan, QueryValidate},
        response::{Response, ResponseError},
        store::DataRow,
    },
    error::InternalError,
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    traits::{EntityKind, FieldValue},
};
use std::{cmp::Ordering, collections::HashMap, hash::Hash, marker::PhantomData, ops::ControlFlow};

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
    // Query builders (execute and return Response)
    // ======================================================================

    /// Execute a query for a single primary key.
    pub fn one(&self, value: impl FieldValue) -> Result<Response<E>, InternalError> {
        self.execute(LoadQuery::new().one::<E>(value))
    }

    /// Execute a query for the unit primary key.
    pub fn only(&self) -> Result<Response<E>, InternalError> {
        self.execute(LoadQuery::new().one::<E>(()))
    }

    /// Execute a query matching multiple primary keys.
    pub fn many<I, V>(&self, values: I) -> Result<Response<E>, InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let query = LoadQuery::new().many_by_field(E::PRIMARY_KEY, values);
        self.execute(query)
    }

    /// Execute an unfiltered query for all rows.
    pub fn all(&self) -> Result<Response<E>, InternalError> {
        self.execute(LoadQuery::new())
    }

    /// Execute a query built from a filter.
    pub fn filter<F, I>(&self, f: F) -> Result<Response<E>, InternalError>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        self.execute(LoadQuery::new().filter(f))
    }

    // ======================================================================
    // Cardinality guards (delegated to Response)
    // ======================================================================

    /// Execute a query and require exactly one row.
    pub fn require_one(&self, query: LoadQuery) -> Result<(), InternalError> {
        self.execute(query)?.require_one()
    }

    /// Require exactly one row by primary key.
    pub fn require_one_pk(&self, value: impl FieldValue) -> Result<(), InternalError> {
        self.require_one(LoadQuery::new().one::<E>(value))
    }

    /// Require exactly one row from a filter.
    pub fn require_one_filter<F, I>(&self, f: F) -> Result<(), InternalError>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        self.require_one(LoadQuery::new().filter(f))
    }

    // ======================================================================
    // Existence checks (≥1 semantics, intentionally weaker)
    // ======================================================================

    /// Check whether at least one row matches the query.
    ///
    /// Note: existence checks are strict. Missing or malformed rows surface
    /// as corruption errors instead of returning false.
    ///
    /// Respects offset/limit when provided (limit=0 returns false).
    pub fn exists(&self, query: LoadQuery) -> Result<bool, InternalError> {
        QueryValidate::<E>::validate(&query)?;
        sink::record(MetricsEvent::ExistsCall {
            entity_path: E::PATH,
        });

        let plan = plan_for::<E>(query.filter.as_ref());
        let filter = query.filter.map(FilterExpr::simplify);
        let offset = query.limit.as_ref().map_or(0, |lim| lim.offset);
        let limit = query.limit.as_ref().and_then(|lim| lim.limit);
        if limit == Some(0) {
            return Ok(false);
        }
        let mut seen = 0u32;
        let mut scanned = 0u64;
        let mut found = false;

        scan_strict::<E, _>(&self.db, plan, |_, entity| {
            scanned = scanned.saturating_add(1);
            let matches = filter
                .as_ref()
                .is_none_or(|f| FilterEvaluator::new(&entity).eval(f));

            if matches {
                if seen < offset {
                    seen += 1;
                    ControlFlow::Continue(())
                } else {
                    found = true;
                    ControlFlow::Break(())
                }
            } else {
                ControlFlow::Continue(())
            }
        })?;

        sink::record(MetricsEvent::RowsScanned {
            entity_path: E::PATH,
            rows_scanned: scanned,
        });

        Ok(found)
    }

    /// Check existence by primary key.
    pub fn exists_one(&self, value: impl FieldValue) -> Result<bool, InternalError> {
        self.exists(LoadQuery::new().one::<E>(value))
    }

    /// Check existence with a filter.
    pub fn exists_filter<F, I>(&self, f: F) -> Result<bool, InternalError>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        self.exists(LoadQuery::new().filter(f))
    }

    /// Check whether the table contains any rows.
    pub fn exists_any(&self) -> Result<bool, InternalError> {
        self.exists(LoadQuery::new())
    }

    // ======================================================================
    // Existence checks with not-found errors (fast path, no deserialization)
    // ======================================================================

    /// Require at least one row by primary key.
    pub fn ensure_exists_one(&self, value: impl FieldValue) -> Result<(), InternalError> {
        if self.exists_one(value)? {
            Ok(())
        } else {
            Err(ResponseError::NotFound { entity: E::PATH }.into())
        }
    }

    /// Require that all provided primary keys exist.
    #[allow(clippy::cast_possible_truncation)]
    pub fn ensure_exists_many<I, V>(&self, values: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let pks: Vec<_> = values.into_iter().collect();

        let expected = pks.len() as u32;
        if expected == 0 {
            return Ok(());
        }

        let res = self.many(pks)?;
        res.require_len(expected)?;

        Ok(())
    }

    /// Require at least one row from a filter.
    pub fn ensure_exists_filter<F, I>(&self, f: F) -> Result<(), InternalError>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        if self.exists_filter(f)? {
            Ok(())
        } else {
            Err(ResponseError::NotFound { entity: E::PATH }.into())
        }
    }

    // ======================================================================
    // Execution & planning
    // ======================================================================

    /// Validate and return the query plan without executing.
    pub fn explain(self, query: LoadQuery) -> Result<QueryPlan, InternalError> {
        QueryValidate::<E>::validate(&query)?;

        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    fn execute_raw(
        &self,
        plan: QueryPlan,
        query: &LoadQuery,
    ) -> Result<Vec<DataRow>, InternalError> {
        let ctx = self.db.context::<E>();

        if let Some(lim) = &query.limit {
            Ok(ctx.rows_from_plan_with_pagination(plan, lim.offset, lim.limit)?)
        } else {
            Ok(ctx.rows_from_plan(plan)?)
        }
    }

    /// Execute a full query and return a collection of entities.
    ///
    /// Note: index-backed loads are best-effort. If index entries point to missing
    /// or malformed rows, those candidates are skipped. Use explicit strict APIs
    /// when corruption must surface as an error.
    pub fn execute(&self, query: LoadQuery) -> Result<Response<E>, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Load);
        QueryValidate::<E>::validate(&query)?;

        self.debug_log(format!("🧭 Executing query: {:?} on {}", query, E::PATH));

        let ctx = self.db.context::<E>();
        let plan = plan_for::<E>(query.filter.as_ref());

        self.debug_log(format!("📄 Query plan: {plan:?}"));
        record_plan_metrics(&plan);

        // Fast path: pre-pagination
        let pre_paginated = query.filter.is_none() && query.sort.is_none() && query.limit.is_some();
        let mut rows: Vec<(Key, E)> = if pre_paginated {
            let data_rows = self.execute_raw(plan, &query)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            self.debug_log(format!(
                "📦 Scanned {} data rows before deserialization",
                data_rows.len()
            ));

            let rows = ctx.deserialize_rows(data_rows)?;
            self.debug_log(format!(
                "🧩 Deserialized {} entities before filtering",
                rows.len()
            ));
            rows
        } else {
            let data_rows = ctx.rows_from_plan(plan)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });
            self.debug_log(format!(
                "📦 Scanned {} data rows before deserialization",
                data_rows.len()
            ));

            let rows = ctx.deserialize_rows(data_rows)?;
            self.debug_log(format!(
                "🧩 Deserialized {} entities before filtering",
                rows.len()
            ));

            rows
        };

        // Filtering
        if let Some(f) = &query.filter {
            let simplified = f.clone().simplify();
            Self::apply_filter(&mut rows, &simplified);

            self.debug_log(format!(
                "🔎 Applied filter -> {} entities remaining",
                rows.len()
            ));
        }

        // Sorting
        if let Some(sort) = &query.sort
            && rows.len() > 1
        {
            Self::apply_sort(&mut rows, sort);
            self.debug_log("↕️ Applied sort expression");
        }

        // Pagination
        if let Some(lim) = &query.limit
            && !pre_paginated
        {
            apply_pagination(&mut rows, lim.offset, lim.limit);
            self.debug_log(format!(
                "📏 Applied pagination (offset={}, limit={:?}) -> {} entities",
                lim.offset,
                lim.limit,
                rows.len()
            ));
        }

        set_rows_from_len(&mut span, rows.len());
        self.debug_log(format!("✅ Query complete -> {} final rows", rows.len()));

        Ok(Response(rows))
    }

    /// Count rows matching a query.
    pub fn count(&self, query: LoadQuery) -> Result<u32, InternalError> {
        Ok(self.execute(query)?.count())
    }

    pub fn count_all(&self) -> Result<u32, InternalError> {
        self.count(LoadQuery::new())
    }

    // ======================================================================
    // Aggregations
    // ======================================================================

    /// Group rows matching a query and count them by a derived key.
    ///
    /// This is intentionally implemented on the executor (not Response)
    /// so it can later avoid full deserialization.
    pub fn group_count_by<K, F>(
        &self,
        query: LoadQuery,
        key_fn: F,
    ) -> Result<HashMap<K, u32>, InternalError>
    where
        K: Eq + Hash,
        F: Fn(&E) -> K,
    {
        let entities = self.execute(query)?.entities();

        let mut counts = HashMap::new();
        for e in entities {
            *counts.entry(key_fn(&e)).or_insert(0) += 1;
        }

        Ok(counts)
    }

    // ======================================================================
    // Private Helpers
    // ======================================================================

    // apply_filter
    fn apply_filter(rows: &mut Vec<(Key, E)>, filter: &FilterExpr) {
        rows.retain(|(_, e)| FilterEvaluator::new(e).eval(filter));
    }

    // apply_sort
    fn apply_sort(rows: &mut [(Key, E)], sort_expr: &SortExpr) {
        rows.sort_by(|(_, ea), (_, eb)| {
            for (field, direction) in sort_expr.iter() {
                let va = ea.get_value(field);
                let vb = eb.get_value(field);

                // Define how to handle missing values (None)
                let ordering = match (va, vb) {
                    (None, None) => continue,             // both missing → move to next field
                    (None, Some(_)) => Ordering::Less,    // None sorts before Some(_)
                    (Some(_), None) => Ordering::Greater, // Some(_) sorts after None
                    (Some(va), Some(vb)) => match va.partial_cmp(&vb) {
                        Some(ord) => ord,
                        None => continue, // incomparable values → move to next field
                    },
                };

                // Apply direction (Asc/Desc)
                let ordering = match direction {
                    Order::Asc => ordering,
                    Order::Desc => ordering.reverse(),
                };

                if ordering != Ordering::Equal {
                    return ordering;
                }
            }

            // all fields equal
            Ordering::Equal
        });
    }
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
