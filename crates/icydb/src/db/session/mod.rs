pub mod delete;
pub mod load;
mod macros;

use crate::{
    db::{
        EntitySchemaDescription, StorageReport,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::{
            PagedGroupedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
        },
    },
    error::Error,
    metrics::MetricsSink,
    traits::{CanisterKind, EntityKind, EntityValue, Update, UpdateView},
    types::Id,
};
use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::{FluentLoadQuery, PagedLoadQuery};

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
    pub const fn new(session: core::db::DbSession<C>) -> Self {
        Self { inner: session }
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
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery {
            inner: self.inner.load::<E>(),
        }
    }

    #[must_use]
    pub const fn load_with_consistency<E>(
        &self,
        consistency: MissingRowPolicy,
    ) -> FluentLoadQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        FluentLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
    }

    /// Build one typed query intent from one reduced SQL statement.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.query_from_sql::<E>(sql)?)
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<Response<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(Response::from_core(self.inner.execute_sql::<E>(sql)?))
    }

    /// Execute one reduced SQL `SELECT` statement and return projection-shaped rows.
    pub fn execute_sql_projection<E>(&self, sql: &str) -> Result<ProjectionResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(ProjectionResponse::from_core(
            self.inner.execute_sql_projection::<E>(sql)?,
        ))
    }

    /// Explain one reduced SQL statement.
    pub fn explain_sql<E>(&self, sql: &str) -> Result<String, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(self.inner.explain_sql::<E>(sql)?)
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, E>
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
        consistency: MissingRowPolicy,
    ) -> SessionDeleteQuery<'_, E>
    where
        E: EntityKind<Canister = C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete_with_consistency::<E>(consistency),
        }
    }

    /// Return one stable, human-readable index listing for the entity schema.
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.show_indexes::<E>()
    }

    /// Return one structured schema description for the entity.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.describe_entity::<E>()
    }

    /// Build one point-in-time storage report for observability endpoints.
    pub fn storage_report(
        &self,
        name_to_path: &[(&'static str, &'static str)],
    ) -> Result<StorageReport, Error> {
        Ok(self.inner.storage_report(name_to_path)?)
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

    /// Build one trace payload for a query without executing it.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.trace_query(query)?)
    }

    /// Execute one grouped query page with optional continuation cursor.
    pub fn execute_grouped<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedResponse, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        let execution = self.inner.execute_grouped(query, cursor_token)?;
        let next_cursor = execution.continuation_cursor().map(core::db::encode_cursor);

        Ok(PagedGroupedResponse::new(
            execution.rows().to_vec(),
            next_cursor,
            execution.execution_trace().copied(),
        ))
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: EntityKind<Canister = C> + EntityValue,
    {
        Ok(WriteResponse::new(self.inner.insert(entity)?))
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
        Ok(WriteResponse::new(self.inner.replace(entity)?))
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
        Ok(WriteResponse::new(self.inner.update(entity)?))
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
