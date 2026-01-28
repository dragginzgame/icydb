use crate::{
    db::query::{
        Query, QueryDiagnostics, QueryExecutionDiagnostics, ReadConsistency, predicate::Predicate,
    },
    error::Error,
    key::Key,
    traits::{CanisterKind, EntityKind},
    view::View,
};
use icydb_core as core;

///
/// DbSession
///
/// Public facade for session-scoped query execution and policy.
/// Wraps the core session and converts core errors into `icydb::Error`.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    // ------------------------------------------------------------------
    // Session configuration
    // ------------------------------------------------------------------

    /// Create a new session scoped to the provided database.
    #[must_use]
    pub const fn new(db: core::db::Db<C>) -> Self {
        Self {
            inner: core::db::DbSession::new(db),
        }
    }

    /// Enable debug logging for queries executed in this session.
    ///
    /// Debug is session-scoped and affects all subsequent operations.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    /// Override the metrics sink for queries executed in this session.
    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn core::obs::sink::MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    // ------------------------------------------------------------------
    // Query entry points
    // ------------------------------------------------------------------

    /// Create a session-bound load query with default consistency.
    #[must_use]
    pub const fn load<E>(&self) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery {
            inner: self.inner.load::<E>(),
        }
    }

    /// Create a session-bound load query with explicit consistency.
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

    /// Create a session-bound delete query with default consistency.
    #[must_use]
    pub const fn delete<E>(&self) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete::<E>(),
        }
    }

    /// Create a session-bound delete query with explicit consistency.
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

    // ------------------------------------------------------------------
    // Query diagnostics / execution
    // ------------------------------------------------------------------

    /// Plan and return diagnostics without executing the query.
    pub fn diagnose_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<QueryDiagnostics, Error> {
        Ok(self.inner.diagnose_query(query)?)
    }

    /// Execute a query using session policy.
    pub fn execute_query<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<core::db::response::Response<E>, Error> {
        Ok(self.inner.execute_query(query)?)
    }

    /// Execute a query and return execution diagnostics.
    pub fn execute_with_diagnostics<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<(core::db::response::Response<E>, QueryExecutionDiagnostics), Error> {
        Ok(self.inner.execute_with_diagnostics(query)?)
    }

    // ------------------------------------------------------------------
    // High-level write helpers
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert(entity)?)
    }

    pub fn insert_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert_many(entities)?)
    }

    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace(entity)?)
    }

    pub fn replace_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace_many(entities)?)
    }

    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update(entity)?)
    }

    pub fn update_many<E>(&self, entities: impl IntoIterator<Item = E>) -> Result<Vec<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update_many(entities)?)
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.insert_view::<E>(view)?)
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.replace_view::<E>(view)?)
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.update_view::<E>(view)?)
    }
}

///
/// SessionLoadQuery
///
/// Session-bound fluent wrapper for load queries.
///

pub struct SessionLoadQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    inner: core::db::query::SessionLoadQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionLoadQuery<'_, C, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access
    // ------------------------------------------------------------------

    #[must_use]
    pub fn key(mut self, key: impl Into<Key>) -> Self {
        self.inner = self.inner.key(key.into());
        self
    }

    /// Load multiple entities by primary key.
    ///
    /// Uses key-based access only (no predicate lowering).
    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::PrimaryKey>,
    {
        self.inner = self.inner.many(keys);
        self
    }

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    #[must_use]
    pub fn offset(mut self, offset: u64) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    // ------------------------------------------------------------------
    // Execution terminals
    // ------------------------------------------------------------------

    pub fn exists(&self) -> Result<bool, Error> {
        Ok(self.inner.exists()?)
    }

    pub fn count(&self) -> Result<u64, Error> {
        Ok(self.inner.count()?)
    }

    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    pub fn execute(&self) -> Result<core::db::response::Response<E>, Error> {
        Ok(self.inner.execute()?)
    }

    pub fn all(&self) -> Result<Vec<E>, Error> {
        Ok(self.inner.all()?)
    }

    pub fn views(&self) -> Result<Vec<View<E>>, Error> {
        Ok(self.inner.views()?)
    }

    pub fn one(&self) -> Result<E, Error> {
        Ok(self.inner.one()?)
    }

    pub fn view(&self) -> Result<View<E>, Error> {
        Ok(self.inner.view()?)
    }

    pub fn one_opt(&self) -> Result<Option<E>, Error> {
        Ok(self.inner.one_opt()?)
    }

    pub fn view_opt(&self) -> Result<Option<View<E>>, Error> {
        Ok(self.inner.view_opt()?)
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = ()>> SessionLoadQuery<'_, C, E> {
    /// Load the singleton entity identified by the unit primary key `()`.
    ///
    /// Semantics:
    /// - Equivalent to `WHERE pk = ()`
    /// - Uses key-based access (ByKey)
    /// - Does not allow predicates
    /// - MissingOk mode returns empty
    /// - Strict mode treats missing row as corruption
    #[must_use]
    pub fn only(mut self) -> Self {
        self.inner = self.inner.only();
        self
    }
}

///
/// SessionDeleteQuery
///
/// Session-bound fluent wrapper for delete queries.
///

pub struct SessionDeleteQuery<'a, C: CanisterKind, E: EntityKind<Canister = C>> {
    inner: core::db::query::SessionDeleteQuery<'a, C, E>,
}

impl<C: CanisterKind, E: EntityKind<Canister = C>> SessionDeleteQuery<'_, C, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access
    // ------------------------------------------------------------------

    #[must_use]
    pub fn key(mut self, key: impl Into<Key>) -> Self {
        self.inner = self.inner.key(key.into());
        self
    }

    /// Delete multiple entities by primary key.
    ///
    /// Deletions are key-only and idempotent in MissingOk mode.
    #[must_use]
    pub fn many<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = E::PrimaryKey>,
    {
        self.inner = self.inner.many(keys);
        self
    }

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    #[must_use]
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.inner = self.inner.filter(predicate);
        self
    }

    #[must_use]
    pub fn order_by(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by(field);
        self
    }

    #[must_use]
    pub fn order_by_desc(mut self, field: impl AsRef<str>) -> Self {
        self.inner = self.inner.order_by_desc(field);
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    pub fn explain(&self) -> Result<core::db::query::plan::ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    pub fn execute(&self) -> Result<core::db::response::Response<E>, Error> {
        Ok(self.inner.execute()?)
    }

    pub fn delete_rows(&self) -> Result<core::db::response::Response<E>, Error> {
        Ok(self.inner.delete_rows()?)
    }
}

impl<C: CanisterKind, E: EntityKind<Canister = C, PrimaryKey = ()>> SessionDeleteQuery<'_, C, E> {
    /// Delete the singleton entity identified by the unit primary key `()`.
    ///
    /// Semantics:
    /// - Equivalent to `DELETE … WHERE pk = ()`
    /// - Uses key-based access (ByKey)
    /// - MissingOk mode is idempotent
    /// - Strict mode treats missing row as corruption
    #[must_use]
    pub fn only(mut self) -> Self {
        self.inner = self.inner.only();
        self
    }
}
