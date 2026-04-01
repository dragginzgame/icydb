//! Module: db::session::sql::dispatch
//! Responsibility: module-local ownership and contracts for db::session::sql::dispatch.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod computed;
mod lowered;

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        executor::{EntityAuthority, execute_sql_projection_rows_for_canister},
        query::intent::StructuralQuery,
        session::sql::{
            SqlDispatchResult, SqlParsedStatement, SqlStatementRoute, computed_projection,
            projection::{
                SqlProjectionPayload, projection_labels_from_entity_model,
                projection_labels_from_structural_query, sql_projection_rows_from_kernel_rows,
            },
            surface::{SqlSurface, session_sql_lane, unsupported_sql_lane_message},
        },
        sql::lowering::{
            LoweredSqlQuery, bind_lowered_sql_query, lower_sql_command_from_prepared_statement,
        },
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Lower one parsed SQL statement into the shared query lane and bind the
    // resulting lowered query shape onto one typed query owner exactly once.
    pub(in crate::db::session::sql) fn bind_sql_query_lane_from_parsed<E>(
        parsed: &SqlParsedStatement,
    ) -> Result<(LoweredSqlQuery, Query<E>), QueryError>
    where
        E: EntityKind<Canister = C>,
    {
        // Keep `query_from_sql` structural-only: computed text projection is a
        // session-owned dispatch surface, not part of the lowered typed-query
        // contract for this slice.
        if computed_projection::computed_sql_projection_plan(&parsed.statement)?.is_some() {
            return Err(QueryError::unsupported_query(
                "query_from_sql does not accept computed text projection; use execute_sql_dispatch(...)",
            ));
        }

        let lowered =
            parsed.lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key.name)?;
        let lane = session_sql_lane(&lowered);
        let Some(query) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                lane,
            )));
        };
        let typed = bind_lowered_sql_query::<E>(query.clone(), MissingRowPolicy::Ignore)
            .map_err(QueryError::from_sql_lowering_error)?;

        Ok((query, typed))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        let columns = projection_labels_from_structural_query(&query)?;
        let projected = execute_sql_projection_rows_for_canister(
            &self.db,
            self.debug,
            authority,
            query.build_plan()?,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    // Execute one typed SQL delete query while keeping the row payload on the
    // typed delete executor boundary that still owns non-runtime-hook delete
    // commit-window application.
    fn execute_typed_sql_delete<E>(&self, query: &Query<E>) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = query.plan()?.into_executable();
        let deleted = self
            .with_metrics(|| self.delete_executor::<E>().execute_sql_projection(plan))
            .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows);

        Ok(SqlProjectionPayload::new(
            projection_labels_from_entity_model(E::MODEL),
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    // Validate that one SQL-derived query intent matches the grouped/scalar
    // execution surface that is about to consume it.
    pub(in crate::db::session::sql) fn ensure_sql_query_grouping<E>(
        query: &Query<E>,
        grouped: bool,
    ) -> Result<(), QueryError>
    where
        E: EntityKind,
    {
        match (grouped, query.has_grouping()) {
            (true, true) | (false, false) => Ok(()),
            (false, true) => Err(QueryError::grouped_requires_execute_grouped()),
            (true, false) => Err(QueryError::unsupported_query(
                "execute_sql_grouped requires grouped SQL query intent",
            )),
        }
    }

    /// Execute one reduced SQL statement into one unified SQL dispatch payload.
    pub fn execute_sql_dispatch<E>(&self, sql: &str) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        self.execute_sql_dispatch_parsed::<E>(&parsed)
    }

    /// Execute one parsed reduced SQL statement into one unified SQL payload.
    pub fn execute_sql_dispatch_parsed<E>(
        &self,
        parsed: &SqlParsedStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => {
                if let Some(plan) =
                    computed_projection::computed_sql_projection_plan(&parsed.statement)?
                {
                    return self.execute_computed_sql_projection_dispatch::<E>(plan);
                }

                let (query, typed_query) = Self::bind_sql_query_lane_from_parsed::<E>(parsed)?;

                Self::ensure_sql_query_grouping(&typed_query, false)?;

                match query {
                    LoweredSqlQuery::Select(select) => self
                        .execute_lowered_sql_dispatch_select_core(
                            &select,
                            EntityAuthority::for_type::<E>(),
                        ),
                    LoweredSqlQuery::Delete(_) => self.execute_typed_sql_delete(&typed_query),
                }
            }
            SqlStatementRoute::Explain { .. } => {
                if let Some((mode, plan)) =
                    computed_projection::computed_sql_projection_explain_plan(&parsed.statement)?
                {
                    return Self::explain_computed_sql_projection_dispatch::<E>(mode, plan)
                        .map(SqlDispatchResult::Explain);
                }

                let lowered = lower_sql_command_from_prepared_statement(
                    parsed.prepare(E::MODEL.name())?,
                    E::MODEL.primary_key.name,
                )
                .map_err(QueryError::from_sql_lowering_error)?;

                lowered
                    .explain_for_model(E::MODEL)
                    .map(SqlDispatchResult::Explain)
            }
            SqlStatementRoute::Describe { .. } => {
                Ok(SqlDispatchResult::Describe(self.describe_entity::<E>()))
            }
            SqlStatementRoute::ShowIndexes { .. } => {
                Ok(SqlDispatchResult::ShowIndexes(self.show_indexes::<E>()))
            }
            SqlStatementRoute::ShowColumns { .. } => {
                Ok(SqlDispatchResult::ShowColumns(self.show_columns::<E>()))
            }
            SqlStatementRoute::ShowEntities => {
                Ok(SqlDispatchResult::ShowEntities(self.show_entities()))
            }
        }
    }

    /// Execute one parsed reduced SQL statement through the generated canister
    /// query/explain surface for one already-resolved dynamic authority.
    ///
    /// This keeps the canister SQL facade on the same reduced SQL ownership
    /// boundary as typed dispatch without forcing the outer facade to reopen
    /// typed-generic routing just to preserve parity for computed projections.
    #[doc(hidden)]
    pub fn execute_generated_query_surface_dispatch_for_authority(
        &self,
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => {
                if let Some(plan) =
                    computed_projection::computed_sql_projection_plan(&parsed.statement)?
                {
                    return self
                        .execute_computed_sql_projection_dispatch_for_authority(plan, authority);
                }

                let lowered = parsed.lower_generated_query_surface_for_entity(
                    authority.model().name(),
                    authority.model().primary_key.name,
                )?;

                self.execute_lowered_sql_dispatch_query_for_authority(&lowered, authority)
            }
            SqlStatementRoute::Explain { .. } => {
                if let Some((mode, plan)) =
                    computed_projection::computed_sql_projection_explain_plan(&parsed.statement)?
                {
                    return Self::explain_computed_sql_projection_dispatch_for_authority(
                        mode, plan, authority,
                    )
                    .map(SqlDispatchResult::Explain);
                }

                let lowered = parsed.lower_generated_query_surface_for_entity(
                    authority.model().name(),
                    authority.model().primary_key.name,
                )?;

                lowered
                    .explain_for_model(authority.model())
                    .map(SqlDispatchResult::Explain)
            }
            SqlStatementRoute::Describe { .. }
            | SqlStatementRoute::ShowIndexes { .. }
            | SqlStatementRoute::ShowColumns { .. }
            | SqlStatementRoute::ShowEntities => Err(QueryError::unsupported_query(
                "generated SQL query surface requires query or EXPLAIN statement lanes",
            )),
        }
    }
}
