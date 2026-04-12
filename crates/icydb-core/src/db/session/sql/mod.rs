//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod aggregate;
mod computed_projection;
mod execute;
mod explain;
mod projection;
mod surface;

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
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

use crate::db::session::sql::surface::sql_statement_route_from_statement;

#[cfg(feature = "structural-read-metrics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
pub use crate::db::session::sql::surface::{
    SqlParsedStatement, SqlStatementResult, SqlStatementRoute,
};
#[cfg(feature = "perf-attribution")]
pub use crate::db::{
    session::sql::execute::LoweredSqlStatementExecutorAttribution,
    session::sql::projection::SqlProjectionTextExecutorAttribution,
};

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
    fn ensure_typed_sql_route_matches<E>(route: &SqlStatementRoute) -> Result<(), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let Some(sql_entity) = (match route {
            SqlStatementRoute::Query { entity }
            | SqlStatementRoute::Insert { entity }
            | SqlStatementRoute::Update { entity }
            | SqlStatementRoute::Explain { entity }
            | SqlStatementRoute::Describe { entity }
            | SqlStatementRoute::ShowIndexes { entity }
            | SqlStatementRoute::ShowColumns { entity } => Some(entity.as_str()),
            SqlStatementRoute::ShowEntities => None,
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

    /// Parse one reduced SQL statement and return one reusable parsed envelope.
    ///
    /// This method is the SQL parse authority for dynamic route selection.
    pub fn parse_sql_statement(&self, sql: &str) -> Result<SqlParsedStatement, QueryError> {
        let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;
        let route = sql_statement_route_from_statement(&statement);

        Ok(SqlParsedStatement::new(statement, route))
    }

    /// Parse one reduced SQL statement into canonical routing metadata.
    ///
    /// This method is the SQL statement authority for entity/surface routing
    /// outside typed-entity lowering paths.
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, QueryError> {
        let parsed = self.parse_sql_statement(sql)?;

        Ok(parsed.route().clone())
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        Self::ensure_typed_sql_route_matches::<E>(parsed.route())?;
        Self::ensure_sql_query_statement_supported(&parsed.statement)?;

        self.execute_sql_statement_parsed::<E>(&parsed)
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        Self::ensure_typed_sql_route_matches::<E>(parsed.route())?;
        Self::ensure_sql_update_statement_supported(&parsed.statement)?;

        self.execute_sql_statement_parsed::<E>(&parsed)
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
        let parsed = self.parse_sql_statement(sql)?;

        // Keep grouped computed SQL on the same computed-projection plan used
        // by the live statement executor while preserving grouped cursor
        // behavior for the legacy session test helpers.
        if let Some(plan) = computed_projection::computed_sql_projection_plan(&parsed.statement)? {
            let lowered = lower_sql_command_from_prepared_statement(
                prepare_sql_statement(plan.cloned_base_statement(), E::MODEL.name())
                    .map_err(QueryError::from_sql_lowering_error)?,
                E::MODEL.primary_key.name,
            )
            .map_err(QueryError::from_sql_lowering_error)?;
            let Some(query) = lowered.query().cloned() else {
                return Err(QueryError::unsupported_query(
                    "execute_sql_grouped requires grouped SELECT",
                ));
            };
            let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)?;

            if !query.has_grouping() {
                return Err(QueryError::unsupported_query(
                    "execute_sql_grouped rejects scalar computed text projection",
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
            prepare_sql_statement(parsed.statement, E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "execute_sql_grouped requires grouped SELECT",
            ));
        };
        let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        if !query.has_grouping() {
            return Err(QueryError::unsupported_query(
                "execute_sql_grouped requires grouped SELECT",
            ));
        }

        self.execute_grouped(&query, cursor_token)
    }
}
