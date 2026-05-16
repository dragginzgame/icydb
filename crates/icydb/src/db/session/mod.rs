pub mod delete;
pub(crate) mod generated;
pub mod load;
mod macros;

#[cfg(feature = "sql")]
use crate::db::sql::{SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput, render_value_text};
use crate::{
    db::{
        EntityFieldDescription, EntitySchemaDescription, StorageReport,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::QueryResponse,
    },
    error::{Error, ErrorKind, ErrorOrigin, RuntimeErrorKind},
    metrics::MetricsSink,
    traits::{CanisterKind, Entity},
    value::{InputValue, OutputValue},
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

/// SQL query attribution envelope used by generated canister endpoints.
#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlQueryPerfAttribution {
    pub compile_local_instructions: u64,
    pub execution: SqlExecutionPerfAttribution,
    pub pure_covering: Option<SqlPureCoveringPerfAttribution>,
    pub response_decode_local_instructions: u64,
    pub total_local_instructions: u64,
}

/// SQL execution-stage attribution.
#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlExecutionPerfAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_local_instructions: u64,
}

/// SQL pure-covering attribution.
#[cfg(feature = "sql")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlPureCoveringPerfAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
impl From<crate::db::SqlQueryExecutionAttribution> for SqlQueryPerfAttribution {
    fn from(attribution: crate::db::SqlQueryExecutionAttribution) -> Self {
        Self {
            compile_local_instructions: attribution.compile_local_instructions,
            execution: SqlExecutionPerfAttribution {
                planner_local_instructions: attribution.execution.planner_local_instructions,
                store_local_instructions: attribution.execution.store_local_instructions,
                executor_local_instructions: attribution.execution.executor_local_instructions,
            },
            pure_covering: attribution.pure_covering.map(|pure_covering| {
                SqlPureCoveringPerfAttribution {
                    decode_local_instructions: pure_covering.decode_local_instructions,
                    row_assembly_local_instructions: pure_covering.row_assembly_local_instructions,
                }
            }),
            response_decode_local_instructions: attribution.response_decode_local_instructions,
            total_local_instructions: attribution.total_local_instructions,
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

#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[expect(clippy::missing_const_for_fn)]
fn read_sql_response_decode_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn measure_sql_response_decode_stage<T>(run: impl FnOnce() -> T) -> (u64, T) {
    let start = read_sql_response_decode_local_instruction_counter();
    let result = run();
    let delta = read_sql_response_decode_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

// Fold the public SQL response-packaging phase onto the outward top-level perf
// contract so shell-facing totals remain exhaustive across compile, planner,
// store, executor, and decode.
#[cfg(all(feature = "sql", feature = "diagnostics"))]
const fn finalize_public_sql_query_attribution(
    mut attribution: crate::db::SqlQueryExecutionAttribution,
    response_decode_local_instructions: u64,
) -> crate::db::SqlQueryExecutionAttribution {
    attribution.response_decode_local_instructions = response_decode_local_instructions;
    attribution.execute_local_instructions = attribution
        .execution
        .planner_local_instructions
        .saturating_add(attribution.execution.store_local_instructions)
        .saturating_add(attribution.execution.executor_local_instructions)
        .saturating_add(
            attribution
                .execution
                .response_finalization_local_instructions,
        )
        .saturating_add(response_decode_local_instructions);
    attribution.total_local_instructions = attribution
        .compile_local_instructions
        .saturating_add(attribution.execute_local_instructions);

    attribution
}

impl<C: CanisterKind> DbSession<C> {
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
    /// split at the shared query seam.
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

    /// Execute one reduced SQL query against one concrete entity type.
    #[cfg(feature = "sql")]
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(crate::db::sql::sql_query_result_from_statement(
            self.inner.execute_sql_query::<E>(sql)?,
            E::MODEL.name().to_string(),
        ))
    }

    /// Execute one SQL query and return the shell perf envelope shape.
    #[cfg(all(feature = "sql", not(feature = "diagnostics")))]
    #[doc(hidden)]
    pub fn execute_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok((
            self.execute_sql_query::<E>(sql)?,
            SqlQueryPerfAttribution::default(),
        ))
    }

    /// Execute one reduced SQL query and report the top-level compile/execute
    /// cost split at the SQL seam.
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn execute_sql_query_with_perf_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, SqlQueryPerfAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, mut attribution) = self.inner.execute_sql_query_with_attribution::<E>(sql)?;
        let entity_name = E::MODEL.name().to_string();

        // Phase 1: measure the outward SQL response packaging step separately
        // so shell/dev perf output can distinguish executor work from result
        // decode and formatting prep.
        let (response_decode_local_instructions, result) =
            measure_sql_response_decode_stage(|| {
                crate::db::sql::sql_query_result_from_statement(result, entity_name)
            });
        attribution =
            finalize_public_sql_query_attribution(attribution, response_decode_local_instructions);

        Ok((result, SqlQueryPerfAttribution::from(attribution)))
    }

    /// Execute one reduced SQL query and report the top-level compile/execute
    /// cost split at the SQL seam.
    #[cfg(all(feature = "sql", feature = "diagnostics"))]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlQueryResult, crate::db::SqlQueryExecutionAttribution), Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let (result, mut attribution) = self.inner.execute_sql_query_with_attribution::<E>(sql)?;
        let entity_name = E::MODEL.name().to_string();
        let (response_decode_local_instructions, result) =
            measure_sql_response_decode_stage(|| {
                crate::db::sql::sql_query_result_from_statement(result, entity_name)
            });
        attribution =
            finalize_public_sql_query_attribution(attribution, response_decode_local_instructions);

        Ok((result, attribution))
    }

    /// Execute one reduced SQL mutation statement against one concrete entity type.
    #[cfg(feature = "sql")]
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(crate::db::sql::sql_query_result_from_statement(
            self.inner.execute_sql_update::<E>(sql)?,
            E::MODEL.name().to_string(),
        ))
    }

    /// Execute one supported SQL DDL statement against one concrete entity type.
    #[cfg(feature = "sql")]
    pub fn execute_sql_ddl<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(crate::db::sql::sql_query_result_from_statement(
            self.inner.execute_sql_ddl::<E>(sql)?,
            E::MODEL.name().to_string(),
        ))
    }

    #[cfg(feature = "sql")]
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
        E: crate::traits::EntityFor<C>,
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
                rendered.push(render_value_text(&OutputValue::from(value)));
            }
            rows.push(rendered);
        }

        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok(SqlQueryRowsOutput::from_projection(
            entity_name,
            SqlProjectionRows::new(columns, rows, row_count),
        ))
    }

    #[cfg(feature = "sql")]
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

    #[cfg(feature = "sql")]
    fn sql_query_rows_output_from_entity<E>(
        entity: E,
        selected_fields: Option<&[String]>,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Self::sql_query_rows_output_from_entities::<E>(
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

    pub fn execute_query<E>(&self, query: &Query<E>) -> Result<QueryResponse<E>, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        Ok(Self::query_response_from_core(
            self.inner.execute_query_result(query)?,
        ))
    }

    /// Build one trace payload for a query without executing it.
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
    #[cfg(feature = "sql")]
    pub fn insert_returning_all<E>(&self, entity: E) -> Result<SqlQueryRowsOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.insert(entity)?;

        Self::sql_query_rows_output_from_entity::<E>(entity, None)
    }

    /// Insert one full entity and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn insert_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.insert(entity)?;
        let fields = Self::returning_fields(fields);

        Self::sql_query_rows_output_from_entity::<E>(entity, Some(fields.as_slice()))
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
    #[cfg(feature = "sql")]
    pub fn create_returning_all<I>(&self, input: I) -> Result<SqlQueryRowsOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.create(input)?;

        Self::sql_query_rows_output_from_entity::<I::Entity>(entity, None)
    }

    /// Create one authored typed input and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn create_returning<I, F, S>(
        &self,
        input: I,
        fields: F,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        I: crate::traits::CreateInputFor<C>,
        I::Entity: crate::traits::EntityFor<C>,
        F: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.create(input)?;
        let fields = Self::returning_fields(fields);

        Self::sql_query_rows_output_from_entity::<I::Entity>(entity, Some(fields.as_slice()))
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// If any item fails pre-commit validation, no row in the batch is persisted.
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
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
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
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
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
    #[cfg(feature = "sql")]
    pub fn update_returning_all<E>(&self, entity: E) -> Result<SqlQueryRowsOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
    {
        let entity = self.inner.update(entity)?;

        Self::sql_query_rows_output_from_entity::<E>(entity, None)
    }

    /// Update one full entity and return one explicit field list.
    #[cfg(feature = "sql")]
    pub fn update_returning<E, I, S>(
        &self,
        entity: E,
        fields: I,
    ) -> Result<SqlQueryRowsOutput, Error>
    where
        E: crate::traits::EntityFor<C>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let entity = self.inner.update(entity)?;
        let fields = Self::returning_fields(fields);

        Self::sql_query_rows_output_from_entity::<E>(entity, Some(fields.as_slice()))
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
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
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

///
/// TESTS
///

#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
mod tests {
    use super::finalize_public_sql_query_attribution;
    use crate::db::SqlQueryExecutionAttribution;

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "the public diagnostics DTO test intentionally stays resilient to future attribution fields"
    )]
    fn public_sql_perf_attribution_total_stays_exhaustive_after_decode_finalize() {
        let mut attribution = SqlQueryExecutionAttribution::default();
        attribution.compile_local_instructions = 11;
        attribution.compile.cache_lookup_local_instructions = 1;
        attribution.compile.parse_local_instructions = 2;
        attribution.compile.parse_tokenize_local_instructions = 1;
        attribution.compile.parse_select_local_instructions = 1;
        attribution.compile.prepare_local_instructions = 3;
        attribution.compile.lower_local_instructions = 4;
        attribution.compile.bind_local_instructions = 1;
        attribution.plan_lookup_local_instructions = 13;
        attribution.execution.planner_local_instructions = 13;
        attribution.execution.store_local_instructions = 17;
        attribution.execution.executor_invocation_local_instructions = 17;
        attribution.execution.executor_local_instructions = 17;
        attribution.store_get_calls = 3;
        attribution.execute_local_instructions = 47;
        attribution.total_local_instructions = 58;

        let finalized = finalize_public_sql_query_attribution(attribution, 19);

        assert_eq!(
            finalized.execute_local_instructions,
            finalized
                .execution
                .planner_local_instructions
                .saturating_add(finalized.execution.store_local_instructions)
                .saturating_add(finalized.execution.executor_local_instructions)
                .saturating_add(finalized.execution.response_finalization_local_instructions)
                .saturating_add(finalized.response_decode_local_instructions),
            "public SQL execute totals should include planner, store, executor, and decode work",
        );
        assert_eq!(
            finalized.total_local_instructions,
            finalized
                .compile_local_instructions
                .saturating_add(finalized.execute_local_instructions),
            "public SQL total instructions should remain exhaustive across compiler, planner, store, executor, and decode",
        );
    }
}
