//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod computed_projection;
mod execute;
mod explain;
mod projection;

use crate::{
    db::{
        DbSession, GroupedRow, PersistedRow, QueryError,
        executor::EntityAuthority,
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
        sql::parser::{SqlStatement, parse_sql},
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

#[cfg(test)]
use crate::db::{
    MissingRowPolicy, PagedGroupedExecutionWithTrace,
    sql::lowering::{
        bind_lowered_sql_query, lower_sql_command_from_prepared_statement, prepare_sql_statement,
    },
};

#[cfg(feature = "structural-read-metrics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "perf-attribution")]
pub use crate::db::{
    session::sql::execute::LoweredSqlStatementExecutorAttribution,
    session::sql::projection::SqlProjectionTextExecutorAttribution,
};

/// Unified SQL statement payload returned by shared SQL lane execution.
#[derive(Debug)]
pub enum SqlStatementResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        rows: Vec<Vec<crate::value::Value>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

impl<C: CanisterKind> DbSession<C> {
    // Resolve planner-visible indexes and build one execution-ready
    // structural plan at the session SQL boundary.
    pub(in crate::db::session::sql) fn build_structural_plan_with_visible_indexes_for_authority(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(VisibleIndexes<'_>, AccessPlannedQuery), QueryError> {
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = query.build_plan_with_visible_indexes(&visible_indexes)?;

        Ok((visible_indexes, plan))
    }

    // Enforce that the public typed SQL executors stay hard-bound to the
    // typed entity `E` instead of silently reusing unrelated entity names.
    fn ensure_typed_sql_statement_matches<E>(statement: &SqlStatement) -> Result<(), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let Some(sql_entity) = (match statement {
            SqlStatement::Select(select) => Some(select.entity.as_str()),
            SqlStatement::Delete(delete) => Some(delete.entity.as_str()),
            SqlStatement::Insert(insert) => Some(insert.entity.as_str()),
            SqlStatement::Update(update) => Some(update.entity.as_str()),
            SqlStatement::Explain(explain) => Some(match &explain.statement {
                crate::db::sql::parser::SqlExplainTarget::Select(select) => select.entity.as_str(),
                crate::db::sql::parser::SqlExplainTarget::Delete(delete) => delete.entity.as_str(),
            }),
            SqlStatement::Describe(describe) => Some(describe.entity.as_str()),
            SqlStatement::ShowIndexes(show_indexes) => Some(show_indexes.entity.as_str()),
            SqlStatement::ShowColumns(show_columns) => Some(show_columns.entity.as_str()),
            SqlStatement::ShowEntities(_) => None,
        }) else {
            return Ok(());
        };

        if crate::db::identifiers_tail_match(sql_entity, E::MODEL.name()) {
            return Ok(());
        }

        Err(QueryError::unsupported_query(format!(
            "typed SQL only supports entity '{}', but received '{sql_entity}'",
            E::MODEL.name()
        )))
    }

    // Keep the public SQL query surface aligned with its name and with
    // query-shaped canister entrypoints.
    fn ensure_sql_query_statement_supported(statement: &SqlStatement) -> Result<(), QueryError> {
        match statement {
            SqlStatement::Select(_)
            | SqlStatement::Explain(_)
            | SqlStatement::Describe(_)
            | SqlStatement::ShowIndexes(_)
            | SqlStatement::ShowColumns(_)
            | SqlStatement::ShowEntities(_) => Ok(()),
            SqlStatement::Insert(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
            )),
            SqlStatement::Update(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
            )),
            SqlStatement::Delete(_) => Err(QueryError::unsupported_query(
                "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
            )),
        }
    }

    // Keep the public SQL mutation surface aligned with state-changing SQL
    // while preserving one explicit read/introspection owner.
    fn ensure_sql_update_statement_supported(statement: &SqlStatement) -> Result<(), QueryError> {
        match statement {
            SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => Ok(()),
            SqlStatement::Select(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SELECT; use execute_sql_query::<E>()",
            )),
            SqlStatement::Explain(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects EXPLAIN; use execute_sql_query::<E>()",
            )),
            SqlStatement::Describe(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects DESCRIBE; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowIndexes(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW INDEXES; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowColumns(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW COLUMNS; use execute_sql_query::<E>()",
            )),
            SqlStatement::ShowEntities(_) => Err(QueryError::unsupported_query(
                "execute_sql_update rejects SHOW ENTITIES; use execute_sql_query::<E>()",
            )),
        }
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = parse_sql_statement(sql)?;

        Self::ensure_typed_sql_statement_matches::<E>(&parsed)?;
        Self::ensure_sql_query_statement_supported(&parsed)?;

        self.execute_sql_statement_inner::<E>(&parsed)
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = parse_sql_statement(sql)?;

        Self::ensure_typed_sql_statement_matches::<E>(&parsed)?;
        Self::ensure_sql_update_statement_supported(&parsed)?;

        self.execute_sql_statement_inner::<E>(&parsed)
    }

    #[cfg(test)]
    pub(in crate::db) fn execute_grouped_sql_query_for_tests<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = parse_sql_statement(sql)?;

        // Keep grouped computed SQL on the same computed-projection plan used
        // by the live statement executor while preserving grouped cursor
        // behavior for the grouped SELECT test helpers.
        if let Some(plan) = computed_projection::computed_sql_projection_plan(&parsed)? {
            let lowered = lower_sql_command_from_prepared_statement(
                prepare_sql_statement(plan.cloned_base_statement(), E::MODEL.name())
                    .map_err(QueryError::from_sql_lowering_error)?,
                E::MODEL.primary_key.name,
            )
            .map_err(QueryError::from_sql_lowering_error)?;
            let Some(query) = lowered.query().cloned() else {
                return Err(QueryError::unsupported_query(
                    "grouped SELECT helper requires grouped SELECT",
                ));
            };
            let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)?;

            if !query.has_grouping() {
                return Err(QueryError::unsupported_query(
                    "grouped SELECT helper rejects scalar computed text projection",
                ));
            }

            let execution = self.execute_grouped(&query, cursor_token)?;
            let (rows, continuation_cursor, execution_trace) = execution.into_parts();
            let rows =
                computed_projection::apply_computed_sql_projection_grouped_rows(rows, &plan)?;

            return Ok(PagedGroupedExecutionWithTrace::new(
                rows,
                continuation_cursor,
                execution_trace,
            ));
        }

        let lowered = lower_sql_command_from_prepared_statement(
            prepare_sql_statement(parsed, E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper requires grouped SELECT",
            ));
        };
        let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        if !query.has_grouping() {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper requires grouped SELECT",
            ));
        }

        self.execute_grouped(&query, cursor_token)
    }
}
