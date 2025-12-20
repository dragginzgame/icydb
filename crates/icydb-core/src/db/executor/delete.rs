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

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    // ─────────────────────────────────────────────
    // PK-BASED HELPERS
    // ─────────────────────────────────────────────

    /// Delete a single row by primary key.
    pub fn one(self, pk: impl FieldValue) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().one::<E>(pk);
        self.execute(query)
    }

    /// Delete the unit-key row.
    pub fn only(self) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().one::<E>(());
        self.execute(query)
    }

    /// Delete multiple rows by primary keys.
    pub fn many<I, V>(self, values: I) -> Result<Response<E>, Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let query = DeleteQuery::new().many_by_field(E::PRIMARY_KEY, values);

        self.execute(query)
    }

    // ─────────────────────────────────────────────
    // GENERIC FIELD-BASED DELETE
    // ─────────────────────────────────────────────

    /// Delete a single row by an arbitrary field value.
    pub fn one_by_field(
        self,
        field: impl AsRef<str>,
        value: impl FieldValue,
    ) -> Result<Response<E>, Error> {
        let query = DeleteQuery::new().one_by_field(field, value);
        self.execute(query)
    }

    /// Delete multiple rows by an arbitrary field.
    pub fn many_by_field<I, V>(
        self,
        field: impl AsRef<str>,
        values: I,
    ) -> Result<Response<E>, Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let query = DeleteQuery::new().many_by_field(field, values);
        self.execute(query)
    }

    /// Delete all rows.
    pub fn all(self) -> Result<Response<E>, Error> {
        self.execute(DeleteQuery::new())
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

    // ─────────────────────────────────────────────
    // ENSURE HELPERS
    // ─────────────────────────────────────────────

    pub fn ensure_delete_one(self, pk: impl FieldValue) -> Result<(), Error> {
        self.one(pk)?.require_one()?;
        Ok(())
    }

    pub fn ensure_delete_any_by_pk<I, V>(self, pks: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.many(pks)?.require_some()?;

        Ok(())
    }

    pub fn ensure_delete_any<I, V>(self, values: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.ensure_delete_any_by_pk(values)
    }

    // ─────────────────────────────────────────────
    // EXECUTION
    // ─────────────────────────────────────────────

    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, Error> {
        QueryValidate::<E>::validate(&query)?;
        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, Error> {
        QueryValidate::<E>::validate(&query)?;
        let mut span = metrics::Span::<E>::new(metrics::ExecKind::Delete);

        let plan = plan_for::<E>(query.filter.as_ref());

        let limit = query
            .limit
            .as_ref()
            .and_then(|l| l.limit)
            .map(|l| l as usize);

        let offset = query.limit.as_ref().map_or(0, |l| l.offset as usize);
        let filter_simplified = query.filter.as_ref().map(|f| f.clone().simplify());

        let mut acc = DeleteAccumulator::new(filter_simplified.as_ref(), offset, limit);

        scan_plan::<E, _>(&self.db, plan, |dk, entity| {
            if acc.should_stop(dk, entity) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;

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
