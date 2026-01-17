use crate::{
    db::{
        Db,
        executor::{
            ExecutorError, FilterEvaluator, UniqueIndexHandle, WriteUnit,
            plan::{plan_for, record_plan_metrics, scan_strict, set_rows_from_len},
            resolve_unique_pk,
        },
        primitives::{FilterDsl, FilterExpr, FilterExt, IntoFilterExpr},
        query::{DeleteQuery, QueryPlan, QueryValidate},
        response::Response,
        store::{DataKey, IndexRemoveOutcome},
    },
    error::{ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    sanitize::sanitize,
    serialize::deserialize,
    traits::{EntityKind, FieldValue, FromKey},
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
    pub fn one(self, pk: impl FieldValue) -> Result<Response<E>, InternalError> {
        let query = DeleteQuery::new().one::<E>(pk);
        self.execute(query)
    }

    /// Delete the unit-key row.
    pub fn only(self) -> Result<Response<E>, InternalError> {
        let query = DeleteQuery::new().one::<E>(());
        self.execute(query)
    }

    /// Delete multiple rows by primary keys.
    pub fn many<I, V>(self, values: I) -> Result<Response<E>, InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let query = DeleteQuery::new().many_by_field(E::PRIMARY_KEY, values);
        self.execute(query)
    }

    // ─────────────────────────────────────────────
    // UNIQUE INDEX DELETE
    // ─────────────────────────────────────────────

    /// Delete a single row using a unique index handle.
    pub fn by_unique_index(
        self,
        index: UniqueIndexHandle,
        entity: E,
    ) -> Result<Response<E>, InternalError>
    where
        E::PrimaryKey: FromKey,
    {
        let mut span = Span::<E>::new(ExecKind::Delete);
        let index = index.index();
        let mut lookup = entity;
        sanitize(&mut lookup)?;

        let Some(pk) = resolve_unique_pk::<E>(&self.db, index, &lookup)? else {
            set_rows_from_len(&mut span, 0);

            return Ok(Response(Vec::new()));
        };

        let (dk, stored) = self.load_existing(pk)?;

        self.db.context::<E>().with_store_mut(|s| {
            // Non-atomic delete: data removal happens before index removal.
            // If index removal fails, orphaned index entries may remain.
            let _unit = WriteUnit::new("delete_unique_row_non_atomic");
            s.remove(&dk);
            if !E::INDEXES.is_empty() {
                self.remove_indexes(&stored)?;
            }

            Ok::<_, InternalError>(())
        })??;

        set_rows_from_len(&mut span, 1);

        Ok(Response(vec![(dk.key(), stored)]))
    }

    // ─────────────────────────────────────────────
    // GENERIC FIELD-BASED DELETE
    // ─────────────────────────────────────────────

    /// Delete a single row by an arbitrary field value.
    pub fn one_by_field(
        self,
        field: impl AsRef<str>,
        value: impl FieldValue,
    ) -> Result<Response<E>, InternalError> {
        let query = DeleteQuery::new().one_by_field(field, value);
        self.execute(query)
    }

    /// Delete multiple rows by an arbitrary field.
    pub fn many_by_field<I, V>(
        self,
        field: impl AsRef<str>,
        values: I,
    ) -> Result<Response<E>, InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        let query = DeleteQuery::new().many_by_field(field, values);
        self.execute(query)
    }

    /// Delete all rows.
    pub fn all(self) -> Result<Response<E>, InternalError> {
        self.execute(DeleteQuery::new())
    }

    /// Apply a filter builder and delete matches.
    pub fn filter<F, I>(self, f: F) -> Result<Response<E>, InternalError>
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

    pub fn ensure_delete_one(self, pk: impl FieldValue) -> Result<(), InternalError> {
        self.one(pk)?.require_one()?;

        Ok(())
    }

    pub fn ensure_delete_any_by_pk<I, V>(self, pks: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.many(pks)?.require_some()?;

        Ok(())
    }

    pub fn ensure_delete_any<I, V>(self, values: I) -> Result<(), InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.ensure_delete_any_by_pk(values)
    }

    // ─────────────────────────────────────────────
    // EXECUTION
    // ─────────────────────────────────────────────

    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, InternalError> {
        QueryValidate::<E>::validate(&query)?;
        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    /// Execute a planner-based delete query.
    ///
    /// NOTE:
    /// - Planner-based deletes are strict on row integrity (missing/malformed rows
    ///   surface corruption).
    /// - Planner-based deletes DO NOT enforce unique-index invariants.
    ///   Use `by_unique_index` for strict unique-index semantics.
    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, InternalError> {
        QueryValidate::<E>::validate(&query)?;
        let mut span = Span::<E>::new(ExecKind::Delete);

        let plan = plan_for::<E>(query.filter.as_ref());
        record_plan_metrics(&plan);

        let limit = query
            .limit
            .as_ref()
            .and_then(|l| l.limit)
            .map(|l| l as usize);

        let offset = query.limit.as_ref().map_or(0, |l| l.offset as usize);
        let filter_simplified = query.filter.as_ref().map(|f| f.clone().simplify());

        let mut acc = DeleteAccumulator::new(filter_simplified.as_ref(), offset, limit);

        let mut scanned = 0u64;
        scan_strict::<E, _>(&self.db, plan, |dk, entity| {
            scanned = scanned.saturating_add(1);
            if acc.should_stop(dk, entity) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;

        // rows_scanned counts evaluated rows, not deleted rows
        sink::record(MetricsEvent::RowsScanned {
            entity_path: E::PATH,
            rows_scanned: scanned,
        });

        let mut res: Vec<(Key, E)> = Vec::with_capacity(acc.matches.len());
        self.db.context::<E>().with_store_mut(|s| {
            // Non-atomic delete loop: partial deletions may persist on failure.
            for (dk, entity) in acc.matches {
                let _unit = WriteUnit::new("delete_row_non_atomic");
                s.remove(&dk);
                if !E::INDEXES.is_empty() {
                    self.remove_indexes(&entity)?;
                }
                res.push((dk.key(), entity));
            }

            Ok::<_, InternalError>(())
        })??;

        set_rows_from_len(&mut span, res.len());

        Ok(Response(res))
    }

    fn remove_indexes(&self, entity: &E) -> Result<(), InternalError> {
        for index in E::INDEXES {
            let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
            let removed = store.with_borrow_mut(|this| this.remove_index_entry(entity, index));
            if removed == IndexRemoveOutcome::Removed {
                sink::record(MetricsEvent::IndexRemove {
                    entity_path: E::PATH,
                });
            }
        }

        Ok(())
    }

    fn load_existing(&self, pk: E::PrimaryKey) -> Result<(DataKey, E), InternalError> {
        let data_key = DataKey::new::<E>(pk.into());
        let bytes = self.db.context::<E>().read_strict(&data_key)?;
        let entity = deserialize::<E>(&bytes).map_err(|_| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {data_key}"),
            )
        })?;

        Ok((data_key, entity))
    }
}
