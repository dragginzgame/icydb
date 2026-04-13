pub mod delete;
pub(crate) mod generated;
pub mod load;
mod macros;

#[cfg(feature = "sql")]
use crate::db::sql::{SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput, render_value_text};
use crate::{
    db::{
        EntityFieldDescription, EntitySchemaDescription, PersistedRow, StorageReport,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::{MutationResult, QueryResponse},
    },
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
    metrics::MetricsSink,
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
};
use icydb_core as core;

// re-exports
pub use delete::SessionDeleteQuery;
pub use load::{FluentLoadQuery, PagedLoadQuery};

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
/// UpdatePatch
///
/// Public structural mutation patch builder.
/// Callers address fields by model field name and provide runtime `Value`
/// payloads; validation remains model-owned and occurs both at patch
/// construction and again during session mutation execution.
///

#[derive(Default)]
pub struct UpdatePatch {
    inner: core::db::UpdatePatch,
}

impl UpdatePatch {
    /// Build one empty structural patch.
    ///
    /// Callers then append field updates through `set_field(...)` so model
    /// field-name validation stays at the patch boundary.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: core::db::UpdatePatch::new(),
        }
    }

    /// Resolve one model field name and append its structural field update.
    ///
    /// This keeps the public patch surface field-name-driven while still
    /// validating field existence before mutation execution begins.
    pub fn set_field(
        mut self,
        model: &'static EntityModel,
        field_name: &str,
        value: Value,
    ) -> Result<Self, Error> {
        self.inner = self.inner.set_field(model, field_name, value)?;

        Ok(self)
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
    fn query_response_from_core<E>(inner: core::db::LoadQueryResult<E>) -> QueryResponse<E>
    where
        E: EntityKind,
    {
        QueryResponse::from_core(inner)
    }

    const fn mutation_entity<E>(entity: E) -> MutationResult<E>
    where
        E: EntityKind,
    {
        MutationResult::from_entity(entity)
    }

    fn mutation_entities<E>(inner: icydb_core::db::WriteBatchResponse<E>) -> MutationResult<E>
    where
        E: EntityKind,
    {
        MutationResult::from_core_batch(inner)
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
        E: PersistedRow<Canister = C>,
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
        E: PersistedRow<Canister = C>,
    {
        FluentLoadQuery {
            inner: self.inner.load_with_consistency::<E>(consistency),
        }
    }

    /// Execute one reduced SQL query against one concrete entity type.
    #[cfg(feature = "sql")]
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(crate::db::sql::sql_query_result_from_statement(
            self.inner.execute_sql_query::<E>(sql)?,
            E::MODEL.name().to_string(),
        ))
    }

    /// Execute one reduced SQL mutation statement against one concrete entity type.
    #[cfg(feature = "sql")]
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(crate::db::sql::sql_query_result_from_statement(
            self.inner.execute_sql_update::<E>(sql)?,
            E::MODEL.name().to_string(),
        ))
    }

    #[cfg(feature = "sql")]
    fn projection_selection<E>(
        selected_fields: Option<&[String]>,
    ) -> Result<(Vec<String>, Vec<usize>), Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
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
                            Error::new(
                                ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
                                ErrorOrigin::Query,
                                format!(
                                    "RETURNING field '{field}' does not exist on the target entity '{}'",
                                    E::PATH
                                ),
                            )
                        })?;
                    indices.push(index);
                }

                Ok((fields.to_vec(), indices))
            }
        }
    }

    #[cfg(feature = "sql")]
    pub(crate) fn sql_query_rows_output_from_entities<E>(
        entity_name: String,
        entities: Vec<E>,
        selected_fields: Option<&[String]>,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: resolve the explicit outward projection contract before
        // rendering any row data so every row-producing typed write helper
        // shares one field-selection rule.
        let (columns, indices) = Self::projection_selection::<E>(selected_fields)?;
        let mut rows = Vec::with_capacity(entities.len());

        // Phase 2: render the selected entity slots into stable SQL-style text
        // rows so every row-producing write surface converges on the same
        // outward payload family.
        for entity in entities {
            let mut rendered = Vec::with_capacity(indices.len());
            for index in &indices {
                let value = entity.get_value_by_index(*index).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Runtime(RuntimeErrorKind::Internal),
                        ErrorOrigin::Query,
                        format!(
                            "RETURNING projection row must align with declared columns: entity='{}' index={index}",
                            E::PATH
                        ),
                    )
                })?;
                rendered.push(render_value_text(&value));
            }
            rows.push(rendered);
        }

        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(SqlQueryRowsOutput::from_projection(
            entity_name,
            SqlProjectionRows::new(columns, rows, row_count),
        ))
    }

    #[must_use]
    pub fn delete<E>(&self) -> SessionDeleteQuery<'_, E>
    where
        E: PersistedRow<Canister = C>,
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
        E: PersistedRow<Canister = C>,
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

    /// Return one stable list of field descriptors for the entity schema.
    #[must_use]
    pub fn show_columns<E>(&self) -> Vec<EntityFieldDescription>
    where
        E: EntityKind<Canister = C>,
    {
        self.inner.show_columns::<E>()
    }

    /// Return one stable list of runtime-registered entity names.
    #[must_use]
    pub fn show_entities(&self) -> Vec<String> {
        self.inner.show_entities()
    }

    /// Return one stable list of runtime-registered entity names.
    ///
    /// This is the typed alias of SQL `SHOW TABLES`, which itself aliases
    /// `SHOW ENTITIES`.
    #[must_use]
    pub fn show_tables(&self) -> Vec<String> {
        self.inner.show_tables()
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

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<QueryResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::query_response_from_core(
            self.inner.execute_query_result(query)?,
        ))
    }

    /// Build one trace payload for a query without executing it.
    pub fn trace_query<E>(&self, query: &Query<E>) -> Result<QueryTracePlan, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.trace_query(query)?)
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entity(self.inner.insert(entity)?))
    }

    /// Insert one full entity and return every persisted field.
    #[cfg(feature = "sql")]
    pub fn insert_returning_all<E>(&self, entity: E) -> Result<SqlQueryRowsOutput, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let entity = self.inner.insert(entity)?;

        Self::sql_query_rows_output_from_entities::<E>(E::PATH.to_string(), vec![entity], None)
    }

    /// Insert one full entity and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn insert_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.insert(entity)?;
        let fields = fields
            .into_iter()
            .map(|field| field.as_ref().to_string())
            .collect::<Vec<_>>();

        Self::sql_query_rows_output_from_entities::<E>(
            E::PATH.to_string(),
            vec![entity],
            Some(fields.as_slice()),
        )
    }

    /// Create one authored typed input.
    pub fn create<I>(&self, input: I) -> Result<MutationResult<I::Entity>, Error>
    where
        I: crate::traits::EntityCreateInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entity(self.inner.create(input)?))
    }

    /// Create one authored typed input and return every persisted field.
    #[cfg(feature = "sql")]
    pub fn create_returning_all<I>(&self, input: I) -> Result<SqlQueryRowsOutput, Error>
    where
        I: crate::traits::EntityCreateInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
    {
        let entity = self.inner.create(input)?;

        Self::sql_query_rows_output_from_entities::<I::Entity>(
            I::Entity::PATH.to_string(),
            vec![entity],
            None,
        )
    }

    /// Create one authored typed input and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn create_returning<I, F, S>(
        &self,
        input: I,
        fields: F,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        I: crate::traits::EntityCreateInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
        F: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.create(input)?;
        let fields = fields
            .into_iter()
            .map(|field| field.as_ref().to_string())
            .collect::<Vec<_>>();

        Self::sql_query_rows_output_from_entities::<I::Entity>(
            I::Entity::PATH.to_string(),
            vec![entity],
            Some(fields.as_slice()),
        )
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn insert_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.insert_many_atomic(entities)?,
        ))
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub fn insert_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.insert_many_non_atomic(entities)?,
        ))
    }

    pub fn replace<E>(&self, entity: E) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entity(self.inner.replace(entity)?))
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn replace_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.replace_many_atomic(entities)?,
        ))
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub fn replace_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.replace_many_non_atomic(entities)?,
        ))
    }

    pub fn update<E>(&self, entity: E) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entity(self.inner.update(entity)?))
    }

    /// Update one full entity and return every persisted field.
    #[cfg(feature = "sql")]
    pub fn update_returning_all<E>(&self, entity: E) -> Result<SqlQueryRowsOutput, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let entity = self.inner.update(entity)?;

        Self::sql_query_rows_output_from_entities::<E>(E::PATH.to_string(), vec![entity], None)
    }

    /// Update one full entity and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn update_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.update(entity)?;
        let fields = fields
            .into_iter()
            .map(|field| field.as_ref().to_string())
            .collect::<Vec<_>>();

        Self::sql_query_rows_output_from_entities::<E>(
            E::PATH.to_string(),
            vec![entity],
            Some(fields.as_slice()),
        )
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
        patch: UpdatePatch,
        mode: MutationMode,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entity(self.inner.mutate_structural::<E>(
            key,
            patch.inner,
            mode.into_core(),
        )?))
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
    ///
    /// This API is not a multi-entity transaction surface.
    pub fn update_many_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.update_many_atomic(entities)?,
        ))
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub fn update_many_non_atomic<E>(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<MutationResult<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::mutation_entities(
            self.inner.update_many_non_atomic(entities)?,
        ))
    }
}
