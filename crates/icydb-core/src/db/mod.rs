mod commit;
pub mod executor;
pub mod identity;
pub mod index;
pub mod query;
pub mod response;
pub mod store;
pub mod traits;
pub mod types;
mod write;

pub(crate) use commit::*;
pub(crate) use write::WriteUnit;

use crate::{
    db::{
        executor::{Context, DeleteExecutor, LoadExecutor, SaveExecutor, UpsertExecutor},
        index::IndexStoreRegistry,
        query::{
            Query, QueryError, QueryMode, ReadConsistency, SessionDeleteQuery, SessionLoadQuery,
            diagnostics::{
                QueryDiagnostics, QueryExecutionDiagnostics, QueryTraceExecutorKind, finish_event,
                start_event, trace_access_from_plan,
            },
        },
        response::Response,
        store::DataStoreRegistry,
        traits::FromKey,
    },
    error::InternalError,
    obs::sink::{self, MetricsSink},
    traits::{CanisterKind, EntityKind},
};
use std::{marker::PhantomData, thread::LocalKey};

///
/// Db
///
/// A handle to the set of stores registered for a specific canister domain.
///
/// - `C` is the [`CanisterKind`] (schema canister marker).
///
/// The `Db` acts as the entry point for querying, saving, and deleting entities
/// within a single canister's store registry.
///
/// Schema/model identity must be validated before use. Derive-generated models
/// uphold identity invariants; manual models should validate once (e.g. via
/// `SchemaInfo::from_entity_model`) prior to executor use.
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
    pub const fn context<E>(&self) -> Context<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        Context::new(self)
    }

    /// Run a closure with read access to the data store registry.
    pub fn with_data<R>(&self, f: impl FnOnce(&DataStoreRegistry) -> R) -> R {
        self.data.with(|reg| f(reg))
    }

    /// Run a closure with read access to the index store registry.
    pub fn with_index<R>(&self, f: impl FnOnce(&IndexStoreRegistry) -> R) -> R {
        self.index.with(|reg| f(reg))
    }
}

// Manual Copy + Clone implementations.
// Safe because Db only contains &'static LocalKey<_> handles,
// duplicating them does not duplicate the contents.
impl<C: CanisterKind> Copy for Db<C> {}

impl<C: CanisterKind> Clone for Db<C> {
    fn clone(&self) -> Self {
        *self
    }
}

///
/// DbSession
/// Database handle plus a debug flag that controls executor verbosity.
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
    metrics: Option<&'static dyn MetricsSink>,
}

impl<C: CanisterKind> DbSession<C> {
    #[must_use]
    /// Create a new session scoped to the provided database.
    pub const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    #[must_use]
    /// Enable debug logging for subsequent queries in this session.
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    #[must_use]
    /// Override the metrics sink for operations executed through this session.
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

    //
    // Query entry points
    //

    ///
    /// Load Query
    /// Create a fluent, session-bound load query with default consistency.
    ///
    #[must_use]
    pub const fn load<E>(&self) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery::new(self, Query::new(ReadConsistency::MissingOk))
    }

    ///
    /// Load Query With Consistency
    /// Create a fluent, session-bound load query with explicit consistency.
    ///
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

    ///
    /// Delete Query
    /// Create a fluent, session-bound delete query with default consistency.
    ///
    #[must_use]
    pub const fn delete<E>(&self) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(ReadConsistency::MissingOk).delete())
    }

    ///
    /// Delete Query With Consistency
    /// Create a fluent, session-bound delete query with explicit consistency.
    ///
    #[must_use]
    pub const fn delete_with_consistency<E>(
        &self,
        consistency: ReadConsistency,
    ) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery::new(self, Query::new(consistency).delete())
    }

    //
    // Low-level executors
    //

    /// Get a [`LoadExecutor`] for executing planned load queries.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn load_executor<E>(&self) -> LoadExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        LoadExecutor::new(self.db, self.debug)
    }

    /// Get a [`SaveExecutor`] for inserting or updating entities.
    ///
    /// Normally you will use the higher-level `insert/replace/update` shortcuts instead.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn save<E>(&self) -> SaveExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        SaveExecutor::new(self.db, self.debug)
    }

    /// Get an [`UpsertExecutor`] for inserting or updating by a unique index.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn upsert<E>(&self) -> UpsertExecutor<E>
    where
        E: EntityKind<Canister = C>,
        E::PrimaryKey: FromKey,
    {
        UpsertExecutor::new(self.db, self.debug)
    }

    /// Get a [`DeleteExecutor`] for deleting entities by key or query.
    /// Note: executor methods do not apply the session metrics override.
    #[must_use]
    pub const fn delete_executor<E>(&self) -> DeleteExecutor<E>
    where
        E: EntityKind<Canister = C>,
    {
        DeleteExecutor::new(self.db, self.debug)
    }

    //
    // Query diagnostics
    //

    /// Plan and return diagnostics for a query without executing it.
    pub fn diagnose_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<QueryDiagnostics, QueryError> {
        let explain = query.explain()?;

        Ok(QueryDiagnostics::from(explain))
    }

    /// Execute a query intent using session policy and executor routing.
    pub fn execute_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<Response<E>, QueryError> {
        let plan = query.plan()?;
        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };

        result.map_err(QueryError::Execute)
    }

    /// Execute a query and return per-execution diagnostics.
    pub fn execute_with_diagnostics<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<(Response<E>, QueryExecutionDiagnostics), QueryError> {
        let plan = query.plan()?;
        let fingerprint = plan.fingerprint();
        let access = Some(trace_access_from_plan(plan.access()));
        let executor = match query.mode() {
            QueryMode::Load(_) => QueryTraceExecutorKind::Load,
            QueryMode::Delete(_) => QueryTraceExecutorKind::Delete,
        };
        let start = start_event(fingerprint, access, executor);

        let result = match query.mode() {
            QueryMode::Load(_) => self.with_metrics(|| self.load_executor::<E>().execute(plan)),
            QueryMode::Delete(_) => self.with_metrics(|| self.delete_executor::<E>().execute(plan)),
        };
        match result {
            Ok(response) => {
                let rows = u64::try_from(response.0.len()).unwrap_or(u64::MAX);
                let finish = finish_event(fingerprint, access, executor, rows);
                let diagnostics = QueryExecutionDiagnostics {
                    fingerprint,
                    events: vec![start, finish],
                };

                Ok((response, diagnostics))
            }

            Err(err) => Err(QueryError::Execute(err)),
        }
    }

    //
    // High-level save shortcuts
    //

    /// Insert a new entity, returning the stored value.
    pub fn insert<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().insert(entity))
    }

    /// Insert multiple entities, returning stored values.
    pub fn insert_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().insert_many(entities))
    }

    /// Replace an existing entity or insert it if it does not yet exist.
    pub fn replace<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().replace(entity))
    }

    /// Replace multiple entities, inserting if missing.
    pub fn replace_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().replace_many(entities))
    }

    /// Partially update an existing entity.
    pub fn update<E>(&self, entity: E) -> Result<E, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().update(entity))
    }

    /// Partially update multiple existing entities.
    pub fn update_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().update_many(entities))
    }

    /// Insert a new view value for an entity.
    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().insert_view(view))
    }

    /// Replace an existing view or insert it if it does not yet exist.
    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().replace_view(view))
    }

    /// Partially update an existing view.
    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, InternalError>
    where
        E: EntityKind<Canister = C>,
    {
        self.with_metrics(|| self.save::<E>().update_view(view))
    }
}
