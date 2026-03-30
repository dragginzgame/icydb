//! Module: db::session::sql
//! Responsibility: module-local ownership and contracts for db::session::sql.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod aggregate;
mod computed_projection;
mod dispatch;
mod explain;
mod projection;
mod surface;

use crate::{
    db::{
        DbSession, EntityResponse, PagedGroupedExecutionWithTrace, PersistedRow, Query, QueryError,
        sql::parser::parse_sql,
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

use crate::db::session::sql::surface::sql_statement_route_from_statement;

pub use crate::db::session::sql::surface::{
    SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
};

impl<C: CanisterKind> DbSession<C> {
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
    /// This method is the SQL dispatch authority for entity/surface routing
    /// outside typed-entity lowering paths.
    pub fn sql_statement_route(&self, sql: &str) -> Result<SqlStatementRoute, QueryError> {
        let parsed = self.parse_sql_statement(sql)?;

        Ok(parsed.route().clone())
    }

    /// Build one typed query intent from one reduced SQL statement.
    ///
    /// This parser/lowering entrypoint is intentionally constrained to the
    /// executable subset wired in the current release.
    pub fn query_from_sql<E>(&self, sql: &str) -> Result<Query<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        let parsed = self.parse_sql_statement(sql)?;
        let (_, query) = Self::bind_sql_query_lane_from_parsed::<E>(&parsed)?;

        Ok(query)
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement for entity `E`.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        Self::ensure_sql_query_grouping(&query, false)?;

        self.execute_query(&query)
    }

    /// Execute one reduced SQL grouped `SELECT` statement and return grouped rows.
    pub fn execute_sql_grouped<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let query = self.query_from_sql::<E>(sql)?;
        Self::ensure_sql_query_grouping(&query, true)?;

        self.execute_grouped(&query, cursor_token)
    }
}
