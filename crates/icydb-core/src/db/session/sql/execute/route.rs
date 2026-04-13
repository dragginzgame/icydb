use crate::{
    db::{
        DbSession, QueryError,
        executor::EntityAuthority,
        session::sql::SqlStatementResult,
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSelectQueryShape, LoweredSelectShape, LoweredSqlCommand,
            LoweredSqlLaneKind, LoweredSqlQuery, SqlLoweringError,
            lower_sql_command_from_prepared_statement, lowered_sql_command_lane,
            prepare_sql_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::CanisterKind,
};

// Keep query-lane lowering beside the unified statement executor because no
// other runtime surface needs a separate lowered query-lane boundary anymore.
// The public query/update facade already classifies statement families, so this
// helper only enforces the narrower postcondition that query-lane lowering
// stays on one query-compatible lowered route.
fn lower_sql_query_lane_for_entity(
    statement: &SqlStatement,
    expected_entity: &'static str,
    primary_key_field: &str,
) -> Result<LoweredSqlCommand, QueryError> {
    let lowered = lower_sql_command_from_prepared_statement(
        prepare_sql_statement(statement.clone(), expected_entity)
            .map_err(QueryError::from_sql_lowering_error)?,
        primary_key_field,
    )
    .map_err(|err| match err {
        SqlLoweringError::UnexpectedQueryLaneStatement => QueryError::invariant(
            "query-lane SQL lowering reached a non query-compatible statement",
        ),
        other => QueryError::from_sql_lowering_error(other),
    })?;
    let lane = lowered_sql_command_lane(&lowered);

    match lane {
        LoweredSqlLaneKind::Query | LoweredSqlLaneKind::Explain => Ok(lowered),
        LoweredSqlLaneKind::Describe
        | LoweredSqlLaneKind::ShowIndexes
        | LoweredSqlLaneKind::ShowColumns
        | LoweredSqlLaneKind::ShowEntities => Err(QueryError::invariant(
            "query-lane SQL lowering produced a non query-compatible lowered lane",
        )),
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Lower one parsed SQL query/explain route once for one resolved authority
    // and preserve grouped-column metadata for grouped SELECT execution.
    fn lowered_sql_query_statement_inputs_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
        unsupported_message: &'static str,
    ) -> Result<LoweredSqlQuery, QueryError> {
        let lowered = lower_sql_query_lane_for_entity(
            statement,
            authority.model().name(),
            authority.model().primary_key.name,
        )?;
        let query = lowered
            .into_query()
            .ok_or_else(|| QueryError::unsupported_query(unsupported_message))?;

        Ok(query)
    }

    // Execute one parsed SQL query route through the shared aggregate,
    // computed-projection, and lowered query lane so every single-entity SQL
    // statement surface only differs at the final SELECT/DELETE packaging boundary.
    pub(in crate::db::session::sql::execute) fn execute_sql_query_route_for_authority(
        &self,
        statement: &SqlStatement,
        authority: EntityAuthority,
        unsupported_message: &'static str,
        execute_select: impl FnOnce(
            &Self,
            LoweredSelectShape,
            EntityAuthority,
            bool,
        ) -> Result<SqlStatementResult, QueryError>,
        execute_delete: impl FnOnce(
            &Self,
            LoweredBaseQueryShape,
            EntityAuthority,
        ) -> Result<SqlStatementResult, QueryError>,
    ) -> Result<SqlStatementResult, QueryError> {
        if Self::sql_query_requires_aggregate_lane(statement) {
            let command =
                Self::compile_sql_aggregate_command_core_for_authority(statement, authority)?;

            return self.execute_global_aggregate_statement_for_authority(
                command,
                authority,
                Self::sql_query_aggregate_label_override(statement),
            );
        }

        let query = Self::lowered_sql_query_statement_inputs_for_authority(
            statement,
            authority,
            unsupported_message,
        )?;

        match query {
            LoweredSqlQuery::Select(select) => {
                let grouped_surface = select.shape() == LoweredSelectQueryShape::Grouped;
                execute_select(self, select, authority, grouped_surface)
            }
            LoweredSqlQuery::Delete(delete) => execute_delete(self, delete, authority),
        }
    }

    // Execute one parsed SQL EXPLAIN route through the shared computed-
    // projection and lowered explain lanes so the single-entity SQL executor does
    // not duplicate the same explain classification tree.
    pub(in crate::db::session::sql::execute) fn execute_sql_explain_route_for_authority(
        &self,
        statement: &SqlStatement,
        authority: EntityAuthority,
    ) -> Result<SqlStatementResult, QueryError> {
        let lowered = lower_sql_query_lane_for_entity(
            statement,
            authority.model().name(),
            authority.model().primary_key.name,
        )?;
        if let Some(explain) =
            self.explain_lowered_sql_execution_for_authority(&lowered, authority)?
        {
            return Ok(SqlStatementResult::Explain(explain));
        }

        self.explain_lowered_sql_for_authority(&lowered, authority)
            .map(SqlStatementResult::Explain)
    }
}
