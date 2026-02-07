mod commit;
pub(crate) mod executor;
pub mod identity;
pub mod index;
pub mod query;
pub mod response;
pub mod store;

pub(crate) use commit::*;

use crate::{
    db::{
        executor::{Context, DeleteExecutor, LoadExecutor, SaveExecutor},
        index::IndexStoreRegistry,
        query::{
            Query, QueryError, QueryMode, ReadConsistency, SessionDeleteQuery, SessionLoadQuery,
        },
        response::{Response, WriteBatchResponse, WriteResponse},
        store::DataStoreRegistry,
    },
    error::InternalError,
    obs::sink::{self, MetricsSink},
    traits::{CanisterKind, EntityKind, EntityValue},
};
use std::{marker::PhantomData, thread::LocalKey};

#[cfg(test)]
use crate::db::{index::IndexStore, store::DataStore};

///
/// Db
///
/// A handle to the set of stores registered for a specific canister domain.
///
pub struct Db<C: CanisterKind> {
    data: &'static LocalKey<DataStoreRegistry>,
    index: &'static LocalKey<IndexStoreRegistry>,
    _marker: PhantomData<C>,
}

impl<C: CanisterKind> Db<C> {
    #[must_use]
    pub const fn new(
        data: &'static LocalKey<DataStoreRegistry>,
        index: &'static LocalKey<IndexStoreRegistry>,
    ) -> Self {
        Self {
            data,
            index,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub(crate) const fn context<E>(&self) -> Context<'_, E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Context::new(self)
    }

    /// Return a recovery-guarded context for read paths (startup recovery only).
    pub(crate) fn recovered_context<E>(&self) -> Result<Context<'_, E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        ensure_recovered(self)?;

        Ok(Context::new(self))
    }

    /// TEST ONLY: Mutate a data store directly, bypassing atomicity and executors.
    ///
    /// This is intended for corruption injection and diagnostic testing only.
    #[cfg(test)]
    pub fn with_data_store_mut_for_test<R>(
        &self,
        path: &'static str,
        f: impl FnOnce(&mut DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.with_data(|reg| reg.with_store_mut(path, f))
    }

    pub(crate) fn with_data<R>(&self, f: impl FnOnce(&DataStoreRegistry) -> R) -> R {
        self.data.with(|reg| f(reg))
    }

    pub(crate) fn with_index<R>(&self, f: impl FnOnce(&IndexStoreRegistry) -> R) -> R {
        self.index.with(|reg| f(reg))
    }
}

impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}

///
/// DbSession
///
/// Session-scoped database handle with policy (debug, metrics) and execution routing.
///
pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
    metrics: Option<&'static dyn MetricsSink>,
}

impl<C: CanisterKind> DbSession<C> {
    #[must_use]
    pub const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.metrics = Some(sink);
        self
    }

    fn with_metrics<T>(&self, f: impl FnOnce() -> T) -> T {
        if let Some(sink) = self.metrics {
            sink::with_metrics_sink(sink, f)
        } else {
            f()
        }
    }

    // ---------------------------------------------------------------------
    // Query entry points (public, fluent)
    // ---------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery::new(self, Query::new(ReadConsistency::MissingOk))
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery::new(self, Query::new(consistency))
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(ReadConsistency::MissingOk).delete())
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(consistency).delete())
    }

    // ---------------------------------------------------------------------
    // Low-level executors (crate-internal; execution primitives)
    // ---------------------------------------------------------------------

    #[must_use]
    pub(crate) const fn load_executor<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn delete_executor<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    #[must_use]
    pub(crate) const fn save_executor<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    // ---------------------------------------------------------------------
    // Query diagnostics / execution (internal routing)
    // ---------------------------------------------------------------------

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, QueryError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let plan = query.plan()?;

        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::Execute)
    }

    // ---------------------------------------------------------------------
    // High-level write API (public, intent-level)
    // ---------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().insert(entity))
            .map(WriteResponse::new)
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let entities =
            self.with_metrics(|| self.save_executor::<E>().insert_many_non_atomic(entities))?;

        Ok(WriteBatchResponse::new(entities))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().replace(entity))
            .map(WriteResponse::new)
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let entities =
            self.with_metrics(|| self.save_executor::<E>().replace_many_non_atomic(entities))?;

        Ok(WriteBatchResponse::new(entities))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().update(entity))
            .map(WriteResponse::new)
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let entities =
            self.with_metrics(|| self.save_executor::<E>().update_many_non_atomic(entities))?;

        Ok(WriteBatchResponse::new(entities))
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().insert_view(view))
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().replace_view(view))
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        self.with_metrics(|| self.save_executor::<E>().update_view(view))
    }

    /// TEST ONLY: clear all registered data and index stores for this database.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn clear_stores_for_tests(&self) {
        // Data stores.
        self.db.with_data(|reg| {
            for (path, _) in reg.iter() {
                let _ = reg.with_store_mut(path, DataStore::clear);
            }
        });

        // Index stores.
        self.db.with_index(|reg| {
            for (path, _) in reg.iter() {
                let _ = reg.with_store_mut(path, IndexStore::clear);
            }
        });
    }
}
