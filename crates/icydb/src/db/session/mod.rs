//! Module: db::session
//!
//! Responsibility: public session and fluent query facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

pub mod delete;
pub(crate) mod generated;
pub mod load;
mod macros;
#[cfg(feature = "sql")]
mod sql;

#[cfg(feature = "diagnostics")]
use crate::db::response::QueryResponse;
#[cfg(feature = "diagnostics")]
use crate::traits::Entity;
use crate::{
    ErrorCode,
    db::{
        EntityCatalogDescription, EntityFieldDescription, EntitySchemaDescription,
        MemoryCatalogDescription, StorageReport, StoreCatalogDescription,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::{ProjectionRows, RowProjectionOutput},
    },
    diagnostic::RuntimeBoundaryCode,
    error::{Error, ErrorOrigin},
    metrics::MetricsSink,
    traits::CanisterKind,
    value::{InputValue, OutputValue},
};

use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::{FluentLoadQuery, PartialWindowLoadQuery};
#[cfg(feature = "sql")]
pub use sql::{
    SqlExecutionPerfAttribution, SqlPureCoveringPerfAttribution, SqlQueryPerfAttribution,
};

///
/// MutationMode
///
/// Public write-mode contract for structural session mutations.
/// This keeps insert, update, and replace under one API surface instead of
/// freezing separate partial helpers with divergent semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationMode {
    Insert,
    Replace,
    Update,
}

impl MutationMode {
    const fn into_core(self) -> core::db::MutationMode {
        match self {
            Self::Insert => core::db::MutationMode::Insert,
            Self::Replace => core::db::MutationMode::Replace,
            Self::Update => core::db::MutationMode::Update,
        }
    }
}

///
/// StructuralPatch
///
/// Public structural mutation patch wrapper.
/// Public callers should construct field-bearing patches through
/// `DbSession::structural_patch(...)` so field lookup follows the accepted
/// persisted schema instead of generated model field order.
/// Empty patches remain representable for callers that need to explicitly
/// exercise sparse mutation behavior.
///

#[derive(Default)]
pub struct StructuralPatch {
    inner: core::db::StructuralPatch,
}

impl StructuralPatch {
    /// Build one empty structural patch.
    ///
    /// Use `DbSession::structural_patch(...)` for patches with field updates.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: core::db::StructuralPatch::new(),
        }
    }

    const fn from_core(inner: core::db::StructuralPatch) -> Self {
        Self { inner }
    }
}

///
/// DbSession
///
/// Public facade for session-scoped query execution, typed SQL lowering, and
/// structural mutation policy.
/// Wraps the core session and converts core results and errors into the
/// outward-facing `icydb` response surface.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    #[cfg(feature = "diagnostics")]
    fn query_response_from_core<E>(inner: core::db::LoadQueryResult<E>) -> QueryResponse<E>
    where
        E: Entity,
    {
        QueryResponse::from_core(inner)
    }

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
    pub fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    // ------------------------------------------------------------------
    // Query entry points
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn load<E>(&self) -> FluentLoadQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
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
        E: crate::traits::EntityFor<C>,
    {
        FluentLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
    }

    /// Execute one typed/fluent query while reporting the compile/execute
    /// split at the shared query boundary.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_query_result_with_attribution<E>(
        &self,
        query: &Query<E>,
    ) -> Result<(QueryResponse<E>, crate::db::QueryExecutionAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, attribution) = self.inner.execute_query_result_with_attribution(query)?;

        Ok((Self::query_response_from_core(result), attribution))
    }

    fn projection_selection<E>(
        selected_fields: Option<&[String]>,
    ) -> Result<(Vec<String>, Vec<usize>), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        match selected_fields {
            None => Ok((
                E::MODEL
                    .fields()
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect(),
                (0..E::MODEL.fields().len()).collect(),
            )),
            Some(fields) => {
                let mut indices = Vec::with_capacity(fields.len());

                for field in fields {
                    let index = E::MODEL
                        .fields()
                        .iter()
                        .position(|candidate| candidate.name() == field.as_str())
                        .ok_or_else(|| {
                            Error::from_runtime_boundary(
                                RuntimeBoundaryCode::RowProjectionFieldNotConfigured,
                                ErrorOrigin::Query,
                            )
                        })?;
                    indices.push(index);
                }

                Ok((fields.to_vec(), indices))
            }
        }
    }

    pub(crate) fn row_projection_output_from_entities<E>(
        entity_name: String,
        entities: Vec<E>,
        selected_fields: Option<&[String]>,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        // Phase 1: resolve the explicit outward projection contract before
        // rendering any row data so every row-producing typed write helper
        // shares one field-selection rule.
        let (columns, indices) = Self::projection_selection::<E>(selected_fields)?;
        let mut rows = Vec::with_capacity(entities.len());

        // Phase 2: move selected entity slots into the typed output payload so
        // row-producing write surfaces do not pre-render blob fields as text.
        for entity in entities {
            let mut row = Vec::with_capacity(indices.len());
            for index in &indices {
                let value = entity.get_value_by_index(*index).ok_or_else(|| {
                    Error::from_error_code(ErrorCode::RUNTIME_INTERNAL, ErrorOrigin::Query)
                })?;
                row.push(OutputValue::from(value));
            }
            rows.push(row);
        }

        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(RowProjectionOutput::from_projection(
            entity_name,
            ProjectionRows::new(columns, rows, row_count),
        ))
    }

    fn returning_fields<I, S>(fields: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        fields
            .into_iter()
            .map(|field| field.as_ref().to_string())
            .collect()
    }

    fn row_projection_output_from_entity<E>(
        entity: E,
        selected_fields: Option<&[String]>,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Self::row_projection_output_from_entities::<E>(
            E::PATH.to_string(),
            vec![entity],
            selected_fields,
        )
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, E>
    where
        E: crate::traits::EntityFor<C>,
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
        E: crate::traits::EntityFor<C>,
    {
        SessionDeleteQuery {
            inner: self.inner.delete_with_consistency::<E>(consistency),
        }
    }

    /// Return one stable, human-readable index listing for the entity schema.
    #[must_use]
    pub fn show_indexes<E>(&self) -> Vec<String>
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.show_indexes::<E>()
    }

    /// Return one stable list of field descriptors for the entity schema.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.show_columns::<E>()
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    #[must_use]
    pub fn show_entities(&self) -> Vec<EntityCatalogDescription> {
        self.inner.show_entities()
    }

    /// Return one stable list of runtime-registered entity catalog entries.
    pub fn try_show_entities(
        &self,
    ) -> Result<Vec<EntityCatalogDescription>, core::error::InternalError> {
        self.inner.try_show_entities()
    }

    /// Return one stable list of runtime-registered store catalog entries.
    #[must_use]
    pub fn show_stores(&self) -> Vec<StoreCatalogDescription> {
        self.inner.show_stores()
    }

    /// Return one stable list of runtime-registered stable-memory allocations.
    #[must_use]
    pub fn show_memory(&self) -> Vec<MemoryCatalogDescription> {
        self.inner.show_memory()
    }

    /// Return one structured schema description for the entity.
    #[must_use]
    pub fn describe_entity<E>(&self) -> EntitySchemaDescription
    where
        E: crate::traits::EntityFor<C>,
    {
        self.inner.describe_entity::<E>()
    }

    /// Return one accepted live-schema description for the entity.
    ///
    /// Generated schema endpoints use this accepted-schema path so DDL-published
    /// index metadata and recovered schema authority are reflected in tooling
    /// payloads instead of only the compiled model proposal.
    pub fn try_describe_entity<E>(&self) -> Result<EntitySchemaDescription, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.try_describe_entity::<E>()?)
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

    /// Build one trace payload for a query without executing it.
    #[doc(hidden)]
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.trace_query(query)?)
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert(entity)?)
    }

    /// Insert one full entity and return every persisted field.
    pub fn insert_returning_all<E>(&self, entity: E) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.insert(entity)?;

        Self::row_projection_output_from_entity::<E>(entity, None)
    }

    /// Insert one full entity and return one explicit field list.
    pub fn insert_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.insert(entity)?;
        let fields = Self::returning_fields(fields);

        Self::row_projection_output_from_entity::<E>(entity, Some(fields.as_slice()))
    }

    /// Create one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<I::Entity, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.create(input)?)
    }

    /// Create one authored typed input and return every persisted field.
    pub fn create_returning_all<I>(&self, input: I) -> Result<RowProjectionOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.create(input)?;

        Self::row_projection_output_from_entity::<I::Entity>(entity, None)
    }

    /// Create one authored typed input and return one explicit field list.
    pub fn create_returning<I, F, S>(
        &self,
        input: I,
        fields: F,
    ) -> Result<RowProjectionOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
        F: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.create(input)?;
        let fields = Self::returning_fields(fields);

        Self::row_projection_output_from_entity::<I::Entity>(entity, Some(fields.as_slice()))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert_many_atomic(entities)?.entities())
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::insert_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.insert_many_non_atomic(entities)?.entities())
    }

    pub fn replace<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace(entity)?)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace_many_atomic(entities)?.entities())
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::replace_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.replace_many_non_atomic(entities)?.entities())
    }

    pub fn update<E>(&self, entity: E) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update(entity)?)
    }

    /// Update one full entity and return every persisted field.
    pub fn update_returning_all<E>(&self, entity: E) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.update(entity)?;

        Self::row_projection_output_from_entity::<E>(entity, None)
    }

    /// Update one full entity and return one explicit field list.
    pub fn update_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<RowProjectionOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.update(entity)?;
        let fields = Self::returning_fields(fields);

        Self::row_projection_output_from_entity::<E>(entity, Some(fields.as_slice()))
    }

    /// Apply one structural mutation under one explicit write-mode contract.
    ///
    /// This is a dynamic, field-name-driven write ingress, not a weaker write
    /// path: the same entity validation and commit rules still apply before
    /// the write can succeed.
    ///
    /// `mode` semantics are explicit:
    /// - `Insert`: sparse patches are allowed; missing fields must materialize
    ///   through explicit defaults or managed-field preflight, and the write
    ///   still fails if the row already exists.
    /// - `Update`: patch applies over the existing row; fails if the row is missing.
    /// - `Replace`: sparse patches are allowed, but omitted fields are not inherited
    ///   from the previous value; they must materialize through explicit defaults
    ///   or managed-field preflight, and the row is inserted if it is missing.
    pub fn mutate_structural<E>(
        &self,
        key: E::Key,
        patch: StructuralPatch,
        mode: MutationMode,
    ) -> Result<E, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self
            .inner
            .mutate_structural::<E>(key, patch.inner, mode.into_core())?)
    }

    /// Build one structural mutation patch through the active accepted schema.
    ///
    /// This session-owned constructor resolves field names through persisted
    /// schema metadata before returning the patch to the caller.
    pub fn structural_patch<E, I, S>(&self, fields: I) -> Result<StructuralPatch, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = (S, InputValue)>,
        S: AsRef<str>,
    {
        let fields = fields
            .into_iter()
            .map(|(field, value)| (field, value.into()));
        let patch = self.inner.structural_patch::<E, _, _>(fields)?;

        Ok(StructuralPatch::from_core(patch))
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    /// Prefer this helper when the caller needs all-or-nothing behavior for a
    /// same-entity batch.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update_many_atomic(entities)?.entities())
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an
    /// error, and returning that error from the surrounding canister update does
    /// not roll back the committed prefix. Use [`Self::update_many_atomic`] when
    /// partial batch persistence is not acceptable.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(self.inner.update_many_non_atomic(entities)?.entities())
    }
}
