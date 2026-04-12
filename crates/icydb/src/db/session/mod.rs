pub mod delete;
pub(crate) mod generated;
pub mod load;
mod macros;

#[cfg(all(test, feature = "sql"))]
mod tests;

#[cfg(feature = "sql")]
use crate::db::{
    SqlStatementRoute,
    sql::{
        SqlGroupedRowsOutput, SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput,
        render_value_text,
    },
};
use crate::{
    db::{
        EntityFieldDescription, EntitySchemaDescription, PersistedRow, StorageReport,
        query::{MissingRowPolicy, Query, QueryTracePlan},
        response::{PagedGroupedResponse, Response, WriteBatchResponse, WriteResponse},
    },
    error::Error,
    metrics::MetricsSink,
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue},
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
/// SqlParsedStatement
///
/// Opaque parsed SQL statement envelope exposed by the facade.
/// Use this to parse once and reuse one canonical route+statement contract
/// across dynamic dispatch and typed execution.
///
#[cfg(feature = "sql")]
pub struct SqlParsedStatement {
    inner: core::db::SqlParsedStatement,
}

#[cfg(feature = "sql")]
impl SqlParsedStatement {
    #[must_use]
    const fn from_core(inner: core::db::SqlParsedStatement) -> Self {
        Self { inner }
    }

    /// Borrow canonical route metadata for this parsed SQL statement.
    #[must_use]
    pub const fn route(&self) -> &SqlStatementRoute {
        self.inner.route()
    }
}

///
/// DbSession
///
/// Public facade for session-scoped query execution, SQL dispatch, and
/// structural mutation policy.
/// Wraps the core session and converts core results and errors into the
/// outward-facing `icydb` response surface.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    const fn response_from_core<E>(inner: core::db::EntityResponse<E>) -> Response<E>
    where
        E: EntityKind,
    {
        Response::from_core(inner)
    }

    const fn write_response<E>(entity: E) -> WriteResponse<E>
    where
        E: EntityKind,
    {
        WriteResponse::new(entity)
    }

    fn write_batch_response<E>(
        inner: icydb_core::db::WriteBatchResponse<E>,
    ) -> WriteBatchResponse<E>
    where
        E: EntityKind,
    {
        WriteBatchResponse::from_core(inner)
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

    /// Build one typed query intent from one reduced SQL statement.
    #[cfg(feature = "sql")]
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, Error>
    where
        E: EntityKind<Canister = C>,
    {
        Ok(self.inner.query_from_sql::<E>(sql)?)
    }

    /// Parse one reduced SQL statement into canonical route metadata.
    #[cfg(feature = "sql")]
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, Error> {
        let parsed = self.parse_sql_statement(sql)?;

        Ok(parsed.route().clone())
    }

    /// Parse one reduced SQL statement into one reusable parsed envelope.
    #[cfg(feature = "sql")]
    pub fn parse_sql_statement(&self, sql: &str) -> Result<SqlParsedStatement, Error> {
        Ok(SqlParsedStatement::from_core(
            self.inner.parse_sql_statement(sql)?,
        ))
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement.
    #[cfg(feature = "sql")]
    pub fn execute_sql<E>(&self, sql: &str) -> Result<Response<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::response_from_core(self.inner.execute_sql::<E>(sql)?))
    }

    /// Execute one reduced SQL statement and return one unified SQL payload.
    #[cfg(feature = "sql")]
    pub fn execute_sql_dispatch<E>(&self, sql: &str) -> Result<SqlQueryResult, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        self.execute_sql_dispatch_parsed::<E>(&parsed)
    }

    /// Execute one parsed reduced SQL statement and return one unified SQL payload.
    #[cfg(feature = "sql")]
    pub fn execute_sql_dispatch_parsed<E>(
        &self,
        parsed: &SqlParsedStatement,
    ) -> Result<SqlQueryResult, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let result = self.inner.execute_sql_dispatch_parsed::<E>(&parsed.inner)?;

        Ok(Self::map_sql_dispatch_result(
            result,
            E::MODEL.name().to_string(),
        ))
    }

    #[cfg(feature = "sql")]
    pub(crate) fn map_sql_dispatch_result(
        result: core::db::SqlDispatchResult,
        entity_name: String,
    ) -> SqlQueryResult {
        match result {
            core::db::SqlDispatchResult::Projection {
                columns,
                rows,
                row_count,
            } => {
                let rows = Self::projection_rows_from_values(columns, rows, row_count);

                Self::projection_sql_query_result(entity_name, rows)
            }
            core::db::SqlDispatchResult::ProjectionText {
                columns,
                rows,
                row_count,
            } => Self::projection_sql_query_result(
                entity_name,
                SqlProjectionRows::new(columns, rows, row_count),
            ),
            core::db::SqlDispatchResult::Grouped {
                columns,
                rows,
                row_count,
                next_cursor,
            } => SqlQueryResult::Grouped(SqlGroupedRowsOutput {
                entity: entity_name,
                columns,
                rows: Self::grouped_rows_from_values(rows),
                row_count,
                next_cursor,
            }),
            core::db::SqlDispatchResult::Explain(explain) => SqlQueryResult::Explain {
                entity: entity_name,
                explain,
            },
            core::db::SqlDispatchResult::Describe(description) => {
                SqlQueryResult::Describe(description)
            }
            core::db::SqlDispatchResult::ShowIndexes(indexes) => SqlQueryResult::ShowIndexes {
                entity: entity_name,
                indexes,
            },
            core::db::SqlDispatchResult::ShowColumns(columns) => SqlQueryResult::ShowColumns {
                entity: entity_name,
                columns,
            },
            core::db::SqlDispatchResult::ShowEntities(entities) => {
                SqlQueryResult::ShowEntities { entities }
            }
        }
    }

    #[cfg(feature = "sql")]
    fn projection_sql_query_result(entity_name: String, rows: SqlProjectionRows) -> SqlQueryResult {
        SqlQueryResult::Projection(SqlQueryRowsOutput::from_projection(entity_name, rows))
    }

    #[cfg(feature = "sql")]
    fn projection_rows_from_values(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
    ) -> SqlProjectionRows {
        // Phase 1: render each projected row cell into stable text.
        let mut rendered_rows = Vec::with_capacity(rows.len());
        let mut max_column_count = 0usize;

        for row in rows {
            let rendered_row = Self::render_sql_value_row(row);
            max_column_count = max_column_count.max(rendered_row.len());
            rendered_rows.push(rendered_row);
        }

        // Phase 2: synthesize fallback labels only when core metadata and
        // rendered row width differ so the public payload stays rectangular.
        let columns = if max_column_count == 0 || columns.len() == max_column_count {
            columns
        } else {
            Self::projection_columns(max_column_count)
        };

        SqlProjectionRows::new(columns, rendered_rows, row_count)
    }

    #[cfg(feature = "sql")]
    fn grouped_rows_from_values(rows: Vec<core::db::GroupedRow>) -> Vec<Vec<String>> {
        let mut rendered_rows = Vec::with_capacity(rows.len());

        for row in rows {
            let mut rendered_row =
                Vec::with_capacity(row.group_key().len() + row.aggregate_values().len());
            Self::render_sql_values_into(row.group_key(), &mut rendered_row);
            Self::render_sql_values_into(row.aggregate_values(), &mut rendered_row);
            rendered_rows.push(rendered_row);
        }

        rendered_rows
    }

    #[cfg(feature = "sql")]
    fn render_sql_value_row(row: Vec<Value>) -> Vec<String> {
        let mut rendered_row = Vec::with_capacity(row.len());
        Self::render_sql_values_into(&row, &mut rendered_row);

        rendered_row
    }

    #[cfg(feature = "sql")]
    fn render_sql_values_into(values: &[Value], rendered_row: &mut Vec<String>) {
        for value in values {
            rendered_row.push(render_value_text(value));
        }
    }

    #[cfg(feature = "sql")]
    fn projection_columns(column_count: usize) -> Vec<String> {
        (0..column_count)
            .map(|index| format!("col_{index}"))
            .collect()
    }

    pub(crate) const fn paged_grouped_response(
        rows: Vec<core::db::GroupedRow>,
        next_cursor: Option<String>,
        execution_trace: Option<core::db::ExecutionTrace>,
    ) -> PagedGroupedResponse {
        PagedGroupedResponse::new(rows, next_cursor, execution_trace)
    }

    /// Execute one reduced SQL global aggregate `SELECT` statement.
    #[cfg(feature = "sql")]
    pub fn execute_sql_aggregate<E>(&self, sql: &str) -> Result<crate::value::Value, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(self.inner.execute_sql_aggregate::<E>(sql)?)
    }

    /// Execute one reduced SQL grouped `SELECT` statement with optional continuation cursor.
    #[cfg(feature = "sql")]
    pub fn execute_sql_grouped<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedResponse, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (rows, next_cursor, execution_trace) = self
            .inner
            .execute_sql_grouped_text_cursor::<E>(sql, cursor_token)?;

        Ok(Self::paged_grouped_response(
            rows,
            next_cursor,
            execution_trace,
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::response_from_core(self.inner.execute_query(query)?))
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (rows, next_cursor, execution_trace) = self
            .inner
            .execute_grouped_text_cursor(query, cursor_token)?;

        Ok(Self::paged_grouped_response(
            rows,
            next_cursor,
            execution_trace,
        ))
    }

    // ------------------------------------------------------------------
    // High-level write helpers (semantic)
    // ------------------------------------------------------------------

    pub fn insert<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_response(self.inner.insert(entity)?))
    }

    /// Insert one authored typed input.
    pub fn insert_typed<I>(&self, input: I) -> Result<WriteResponse<I::Entity>, Error>
    where
        I: crate::traits::EntityInsertInput,
        I::Entity: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_response(self.inner.insert_typed(input)?))
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
            self.inner.insert_many_non_atomic(entities)?,
        ))
    }

    pub fn replace<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_response(self.inner.replace(entity)?))
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
            self.inner.replace_many_non_atomic(entities)?,
        ))
    }

    pub fn update<E>(&self, entity: E) -> Result<WriteResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_response(self.inner.update(entity)?))
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
    ) -> Result<WriteResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_response(self.inner.mutate_structural::<E>(
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
    ) -> Result<WriteBatchResponse<E>, Error>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Ok(Self::write_batch_response(
            self.inner.update_many_non_atomic(entities)?,
        ))
    }
}
