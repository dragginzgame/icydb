pub mod delete;
pub mod load;

use crate::{
    db::{
        query::{Query, QueryDiagnostics, QueryExecutionDiagnostics, ReadConsistency},
        response::{Response, WriteBatchResponse, WriteResponse},
    },
    error::Error,
    traits::{CanisterKind, EntityKind, EntityValue},
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

    #[must_use]
    pub const fn new(db: core::db::Db<C>) -> Self {
        Self {
            inner: core::db::DbSession::new(db),
        }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn core::obs::sink::MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    // ------------------------------------------------------------------
    // Query entry points
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> SessionLoadQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionLoadQuery {
            inner: self.inner.load::<E>(),
        }
    }

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

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, C, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete::<E>(),
        }
    }

    #[must_use]
    pub fn delete_with_consistency<E>(
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

    pub fn diagnose_query<E>(&self, query: &Query<E>) -> Result<QueryDiagnostics, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.diagnose_query(query)?)
    }

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(Response::from_core(self.inner.execute_query(query)?))
    }

    pub fn execute_with_diagnostics<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(Response<E>, QueryExecutionDiagnostics), Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let (response, diagnostics) = self.inner.execute_with_diagnostics(query)?;

        Ok((Response::from_core(response), diagnostics))
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::from_core(self.inner.insert(entity)?))
    }

    pub fn insert_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.insert_many(entities)?,
        ))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::from_core(self.inner.replace(entity)?))
    }

    pub fn replace_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.replace_many(entities)?,
        ))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::from_core(self.inner.update(entity)?))
    }

    pub fn update_many<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.update_many(entities)?,
        ))
    }

    pub fn insert_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.insert_view::<E>(view)?)
    }

    pub fn replace_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.replace_view::<E>(view)?)
    }

    pub fn update_view<E>(&self, view: E::ViewType) -> Result<E::ViewType, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.update_view::<E>(view)?)
    }
}
