pub mod delete;
pub mod load;

use crate::{
    db::{
        query::{Query, QueryDiagnostics, QueryExecutionDiagnostics, ReadConsistency},
        response::Response,
    },
    error::Error,
    traits::{CanisterKind, EntityKind},
};
use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::SessionLoadQuery;

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
    ) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute_query(query)?))
    }

    /// Execute a query and return execution diagnostics.
    pub fn execute_with_diagnostics<E: EntityKind<Canister = C>>(
        &self,
        query: &Query<E>,
    ) -> Result<(Response<E>, QueryExecutionDiagnostics), Error> {
        let (response, diagnostics) = self.inner.execute_with_diagnostics(query)?;

        Ok((Response::from_core(response), diagnostics))
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
