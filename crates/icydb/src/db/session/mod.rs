pub mod delete;
pub mod load;
mod macros;

use crate::{
    db::{
        query::{Query, ReadConsistency},
        response::{Response, WriteBatchResponse, WriteResponse},
    },
    error::Error,
    obs::MetricsSink,
    traits::{CanisterKind, EntityKind, EntityValue, Update, UpdateView},
    types::Id,
};
use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::{PagedLoadQuery, SessionLoadQuery};

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
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
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
    // Execution
    // ------------------------------------------------------------------

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<Response<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(Response::from_core(self.inner.execute_query(query)?))
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

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.insert_many_atomic(entities)?,
        ))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.insert_many_non_atomic(entities)?,
        ))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::from_core(self.inner.replace(entity)?))
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.replace_many_atomic(entities)?,
        ))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.replace_many_non_atomic(entities)?,
        ))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::from_core(self.inner.update(entity)?))
    }

    /// Load one entity by id, apply a merge patch, and persist it.
    ///
    /// Patch semantics are handled at this facade boundary so callers do not
    /// need to interact with core patch errors directly.
    pub fn patch_by_id<E>(&self, id: Id<E>, patch: Update<E>) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue + UpdateView,
    {
        let mut entity = self.load::<E>().by_id(id).entity()?;

        UpdateView::merge(&mut entity, patch)?;

        self.update(entity)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.update_many_atomic(entities)?,
        ))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteBatchResponse::from_core(
            self.inner.update_many_non_atomic(entities)?,
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
