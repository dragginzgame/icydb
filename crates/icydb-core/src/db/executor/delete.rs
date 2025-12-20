use crate::{
    Error, Key,
    db::{
        Db,
        executor::{
            FilterEvaluator,
            plan::{plan_for, scan_plan, set_rows_from_len},
        },
        primitives::{FilterDsl, FilterExpr, FilterExt, IntoFilterExpr},
        query::{DeleteQuery, QueryPlan, QueryValidate},
        response::Response,
        store::DataKey,
    },
    obs::metrics,
    traits::{EntityKind, FieldValue},
};
use std::{marker::PhantomData, ops::ControlFlow};

///
/// DeleteAccumulator
///
/// collects matched rows for deletion while applying filter + offset/limit during iteration
/// stops scanning once the window is satisfied.
///

struct DeleteAccumulator<'f, E> {
    filter: Option<&'f FilterExpr>,
    offset: usize,
    skipped: usize,
    limit: Option<usize>,
    matches: Vec<(DataKey, E)>,
}

impl<'f, E: EntityKind> DeleteAccumulator<'f, E> {
    fn new(filter: Option<&'f FilterExpr>, offset: usize, limit: Option<usize>) -> Self {
        Self {
            filter,
            offset,
            skipped: 0,
            limit,
            matches: Vec::with_capacity(limit.unwrap_or(0)),
        }
    }

    fn limit_reached(&self) -> bool {
        self.limit.is_some_and(|lim| self.matches.len() >= lim)
    }

    /// Returns true when the limit has been reached and iteration should stop.
    fn should_stop(&mut self, dk: DataKey, entity: E) -> bool {
        if let Some(f) = self.filter
            && !FilterEvaluator::new(&entity).eval(f)
        {
            return false;
        }

        if self.skipped < self.offset {
            self.skipped += 1;
            return false;
        }

        if self.limit_reached() {
            return true;
        }

        self.matches.push((dk, entity));
        false
    }
}

///
/// DeleteExecutor
///

#[derive(Clone, Copy)]
pub struct DeleteExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> DeleteExecutor<E> {
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
    }

    // debug
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    ///
    /// HELPER METHODS
    ///

    /// Delete a single matching row.
    pub fn one(self, value: impl FieldValue) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().one::<E>(value);
        self.execute(query)
    }

    /// Delete the unit-key row.
    pub fn only(self) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().one::<E>(());
        self.execute(query)
    }

    /// Delete multiple rows by primary keys.
    pub fn many(
        self,
        values: impl IntoIterator<Item = impl FieldValue>,
    ) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().many::<E>(values);
        self.execute(query)
    }

    /// Delete all rows.
    pub fn all(self) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new();
        self.execute(query)
    }

    /// Apply a filter builder and delete matches.
    pub fn filter<F, I>(self, f: F) -> Result<Response<E>, Error>
    where
        F: FnOnce(FilterDsl) -> I,
        I: IntoFilterExpr,
    {
        let query = DeleteQuery::new().filter(f);
        self.execute(query)
    }

    ///
    /// EXECUTION METHODS
    ///

    pub fn ensure_delete_one(self, pk: impl FieldValue) -> Result<(), Error> {
        self.one(pk)?.require_one()?;

        Ok(())
    }

    pub fn ensure_delete_any(
        self,
        pks: impl IntoIterator<Item = impl FieldValue>,
    ) -> Result<(), Error> {
        self.many(pks)?.require_some()?;

        Ok(())
    }

    /// Validate and return the query plan without executing.
    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, Error> {
        QueryValidate::<E>::validate(&query)?;

        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    /// Execute a delete query and return the removed rows.
    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, Error> {
        QueryValidate::<E>::validate(&query)?;
        let mut span = metrics::Span::<E>::new(metrics::ExecKind::Delete);

        let plan = plan_for::<E>(query.filter.as_ref());

        // query prep
        let limit = query
            .limit
            .as_ref()
            .and_then(|l| l.limit)
            .map(|l| l as usize);
        let offset = query.limit.as_ref().map_or(0_usize, |l| l.offset as usize);
        let filter_simplified = query.filter.as_ref().map(|f| f.clone().simplify());

        let mut acc = DeleteAccumulator::new(filter_simplified.as_ref(), offset, limit);

        scan_plan::<E, _>(&self.db, plan, |dk, entity| {
            if acc.should_stop(dk, entity) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;

        // Apply deletions + index teardown
        let mut res: Vec<(Key, E)> = Vec::with_capacity(acc.matches.len());
        self.db.context::<E>().with_store_mut(|s| {
            for (dk, entity) in acc.matches {
                s.remove(&dk);
                if !E::INDEXES.is_empty() {
                    self.remove_indexes(&entity)?;
                }
                res.push((dk.key(), entity));
            }

            Ok::<_, Error>(())
        })??;

        set_rows_from_len(&mut span, res.len());

        Ok(Response(res))
    }

    // remove_indexes
    fn remove_indexes(&self, entity: &E) -> Result<(), Error> {
        for index in E::INDEXES {
            let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;

            store.with_borrow_mut(|this| {
                this.remove_index_entry(entity, index);
            });
        }

        Ok(())
    }
}
