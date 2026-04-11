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
        DbSession, EntityResponse, GroupedTextCursorPageWithTrace, MissingRowPolicy,
        PagedGroupedExecutionWithTrace, PersistedRow, Query, QueryError,
        executor::EntityAuthority,
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes},
        },
        sql::{
            lowering::{bind_lowered_sql_query, lower_sql_command_from_prepared_statement},
            parser::{SqlStatement, parse_sql},
        },
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

use crate::db::session::sql::aggregate::{
    SqlAggregateSurface, parsed_requires_dedicated_sql_aggregate_lane,
    unsupported_sql_aggregate_lane_message,
};
use crate::db::session::sql::surface::{
    SqlSurface, session_sql_lane, sql_statement_route_from_statement, unsupported_sql_lane_message,
};

pub use crate::db::session::sql::surface::{
    SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
};
#[cfg(feature = "perf-attribution")]
pub use crate::db::{
    executor::SqlProjectionTextExecutorAttribution,
    session::sql::dispatch::LoweredSqlDispatchExecutorAttribution,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlComputedProjectionSurface {
    QueryFrom,
    ExecuteSql,
    ExecuteSqlGrouped,
}

const fn unsupported_sql_computed_projection_message(
    surface: SqlComputedProjectionSurface,
) -> &'static str {
    match surface {
        SqlComputedProjectionSurface::QueryFrom => {
            "query_from_sql does not accept computed text projection; use execute_sql_dispatch(...)"
        }
        SqlComputedProjectionSurface::ExecuteSql => {
            "execute_sql rejects computed text projection; use execute_sql_dispatch(...)"
        }
        SqlComputedProjectionSurface::ExecuteSqlGrouped => {
            "execute_sql_grouped rejects computed text projection; use execute_sql_dispatch(...)"
        }
    }
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

    // Lower one parsed SQL statement onto the structural query lane while
    // keeping dedicated global aggregate execution outside this shared path.
    fn query_from_sql_parsed<E>(
        parsed: &SqlParsedStatement,
        lane_surface: SqlSurface,
        computed_surface: SqlComputedProjectionSurface,
        surface: SqlAggregateSurface,
    ) -> Result<Query<E>, QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        if computed_projection::computed_sql_projection_plan(&parsed.statement)?.is_some() {
            return Err(QueryError::unsupported_query(
                unsupported_sql_computed_projection_message(computed_surface),
            ));
        }

        if parsed_requires_dedicated_sql_aggregate_lane(parsed) {
            return Err(QueryError::unsupported_query(
                unsupported_sql_aggregate_lane_message(surface),
            ));
        }

        let lowered = lower_sql_command_from_prepared_statement(
            parsed.prepare(E::MODEL.name())?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let lane = session_sql_lane(&lowered);
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                lane_surface,
                lane,
            )));
        };
        let query = bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        Ok(query)
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

        Self::query_from_sql_parsed::<E>(
            &parsed,
            SqlSurface::QueryFrom,
            SqlComputedProjectionSurface::QueryFrom,
            SqlAggregateSurface::QueryFrom,
        )
    }

    /// Execute one reduced SQL `SELECT`/`DELETE` statement for entity `E`.
    pub fn execute_sql<E>(&self, sql: &str) -> Result<EntityResponse<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;
        let query = Self::query_from_sql_parsed::<E>(
            &parsed,
            SqlSurface::ExecuteSql,
            SqlComputedProjectionSurface::ExecuteSql,
            SqlAggregateSurface::ExecuteSql,
        )?;
        Self::ensure_sql_query_grouping(&query, dispatch::SqlGroupingSurface::Scalar)?;

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
        let parsed = self.parse_sql_statement(sql)?;

        if matches!(&parsed.statement, SqlStatement::Delete(_)) {
            return Err(QueryError::unsupported_query(
                "execute_sql_grouped rejects DELETE; use execute_sql_dispatch(...)",
            ));
        }

        let query = Self::query_from_sql_parsed::<E>(
            &parsed,
            SqlSurface::ExecuteSqlGrouped,
            SqlComputedProjectionSurface::ExecuteSqlGrouped,
            SqlAggregateSurface::ExecuteSqlGrouped,
        )?;
        Self::ensure_sql_query_grouping(&query, dispatch::SqlGroupingSurface::Grouped)?;

        self.execute_grouped(&query, cursor_token)
    }

    /// Execute one reduced SQL grouped `SELECT` statement and return one text cursor directly.
    #[doc(hidden)]
    pub fn execute_sql_grouped_text_cursor<E>(
        &self,
        sql: &str,
        cursor_token: Option<&str>,
    ) -> Result<GroupedTextCursorPageWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        if matches!(&parsed.statement, SqlStatement::Delete(_)) {
            return Err(QueryError::unsupported_query(
                "execute_sql_grouped rejects DELETE; use execute_sql_dispatch(...)",
            ));
        }

        let query = Self::query_from_sql_parsed::<E>(
            &parsed,
            SqlSurface::ExecuteSqlGrouped,
            SqlComputedProjectionSurface::ExecuteSqlGrouped,
            SqlAggregateSurface::ExecuteSqlGrouped,
        )?;
        Self::ensure_sql_query_grouping(&query, dispatch::SqlGroupingSurface::Grouped)?;

        self.execute_grouped_text_cursor(&query, cursor_token)
    }
}
