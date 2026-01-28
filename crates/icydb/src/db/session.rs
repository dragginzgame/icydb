use crate::{
    db::query::{
        Query, QueryDiagnostics, QueryExecutionDiagnostics, ReadConsistency, predicate::Predicate,
    },
    db::response::Response,
    error::Error,
    traits::{CanisterKind, EntityKind},
};
use icydb_core as core;

///
/// DbSession
/// Public facade session wrapper for query execution and policy.
/// Converts core errors into `icydb::Error`.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    /// Create a new facade session scoped to the provided database.
    #[must_use]
    pub const fn new(db: core::db::Db<C>) -> Self {
        Self {
            inner: core::db::DbSession::new(db),
        }
    }

    /// Enable debug logging for subsequent queries in this session.
    ///
    /// Debug contract:
    /// - Debug is session-scoped only; executors do not expose independent toggles.
    /// - Load debug narrates the full access/decode/filter/order/page pipeline.
    /// - Save/delete debug narrate query intent plus commit/rollback outcomes.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    /// Override the metrics sink for operations executed through this session.
    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn core::obs::sink::MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
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
        SessionLoadQuery {
            inner: self.inner.load::<E>(),
        }
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
        SessionLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
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
        SessionDeleteQuery {
            inner: self.inner.delete::<E>(),
        }
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
        SessionDeleteQuery {
            inner: self.inner.delete_with_consistency::<E>(consistency),
        }
    }

    //
    // Query diagnostics
    //

    /// Plan and return diagnostics for a query without executing it.
    pub fn diagnose_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<QueryDiagnostics, Error> {
        Ok(self.inner.diagnose_query(query)?)
    }

    /// Execute a query using session policy and executor routing.
    pub fn execute_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<Response<E>, Error> {
        Ok(self.inner.execute_query(query)?)
    }

    /// Execute a query and return per-execution diagnostics.
    pub fn execute_with_diagnostics<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<(Response<E>, QueryExecutionDiagnostics), Error> {
        Ok(self.inner.execute_with_diagnostics(query)?)
    }

    //
    // High-level write shortcuts
    //

    /// Insert a new entity, returning the stored value.
    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert(entity)?)
    }

    /// Insert multiple entities, returning stored values.
    pub fn insert_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert_many(entities)?)
    }

    /// Replace an existing entity or insert it if it does not yet exist.
    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace(entity)?)
    }

    /// Replace multiple entities, inserting if missing.
    pub fn replace_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace_many(entities)?)
    }

    /// Partially update an existing entity.
    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update(entity)?)
    }

    /// Partially update multiple existing entities.
    pub fn update_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update_many(entities)?)
    }

    /// Insert a new view value for an entity.
    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert_view::<E>(view)?)
    }

    /// Replace an existing view or insert it if it does not yet exist.
    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace_view::<E>(view)?)
    }

    /// Partially update an existing view.
    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update_view::<E>(view)?)
    }
}

///
/// SessionLoadQuery
/// Facade wrapper for session-bound load queries.
/// Converts core errors into `icydb::Error`.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    inner: core::db::query::SessionLoadQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'_, C, E> {
    /// Return a reference to the underlying query.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    /// Apply a load limit to bound result size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Apply a load offset.
    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(self.inner.execute()?)
    }

    /// Execute a load query and return all entities.
    pub fn all(&self) -> Result<Vec<E>, Error> {
        Ok(self.inner.all()?)
    }

    /// Execute a load query and require exactly one entity.
    pub fn one(&self) -> Result<E, Error> {
        Ok(self.inner.one()?)
    }

    /// Execute a load query and return zero or one entity.
    pub fn one_opt(&self) -> Result<Option<E>, Error> {
        Ok(self.inner.one_opt()?)
    }
}

///
/// SessionDeleteQuery
/// Facade wrapper for session-bound delete queries.
/// Converts core errors into `icydb::Error`.
///

pub struct SessionDeleteQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    inner: core::db::query::SessionDeleteQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionDeleteQuery<'_, C, E> {
    /// Return a reference to the underlying query.
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    /// Add a predicate, implicitly AND-ing with any existing predicate.
    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    /// Append an ascending sort key.
    #[must_use]
    pub fn order_by(mut self, field: &'static str) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    /// Append a descending sort key.
    #[must_use]
    pub fn order_by_desc(mut self, field: &'static str) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    /// Apply a delete limit to bound mutation size.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Explain this query without executing it.
    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<Response<E>, Error> {
        Ok(self.inner.execute()?)
    }

    /// Execute a delete query and return the deleted rows.
    pub fn delete_rows(&self) -> Result<Response<E>, Error> {
        Ok(self.inner.delete_rows()?)
    }
}
