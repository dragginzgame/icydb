use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        session::sql::CompiledSqlCommand,
        sql::lowering::{
            LoweredSqlCommand, LoweredSqlQuery, SqlLoweringError,
            bind_lowered_sql_select_query_structural,
            compile_sql_global_aggregate_command_core_from_prepared,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::CanisterKind,
};

// Keep query-lane lowering beside the SQL compile seam so the session-owned
// compiled command can lower one parsed statement once before execution picks a
// runtime path.
fn lower_sql_query_lane_for_entity(
    statement: &SqlStatement,
    authority: EntityAuthority,
) -> Result<LoweredSqlCommand, QueryError> {
    let lowered = lower_sql_command_from_prepared_statement(
        prepare_sql_statement(statement.clone(), authority.model().name())
            .map_err(QueryError::from_sql_lowering_error)?,
        authority.model(),
    )
    .map_err(|err| match err {
        SqlLoweringError::UnexpectedQueryLaneStatement => QueryError::invariant(
            "query-lane SQL lowering reached a non query-compatible statement",
        ),
        other => QueryError::from_sql_lowering_error(other),
    })?;

    Ok(lowered)
}

impl<C: CanisterKind> DbSession<C> {
    // Prepare one parsed SQL statement against one resolved authority so
    // compile-time normalization and entity-match validation happen exactly once.
    fn prepare_sql_statement_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
    ) -> Result<crate::db::sql::lowering::PreparedSqlStatement, QueryError> {
        prepare_sql_statement(statement.clone(), authority.model().name())
            .map_err(QueryError::from_sql_lowering_error)
    }

    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    pub(in crate::db::session::sql) fn compile_sql_statement_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
    ) -> Result<CompiledSqlCommand, QueryError> {
        match statement {
            SqlStatement::Select(_) if Self::sql_query_requires_aggregate_lane(statement) => {
                let prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;
                let command = compile_sql_global_aggregate_command_core_from_prepared(
                    prepared,
                    authority.model(),
                    MissingRowPolicy::Ignore,
                )
                .map_err(QueryError::from_sql_lowering_error)?;

                Ok(CompiledSqlCommand::GlobalAggregate {
                    command,
                    label_overrides: Self::sql_query_aggregate_label_overrides(statement),
                })
            }
            SqlStatement::Select(_) => {
                let lowered = lower_sql_query_lane_for_entity(statement, authority)?;
                let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
                    return Err(QueryError::invariant(
                        "compiled SQL SELECT lane must lower to lowered SQL SELECT",
                    ));
                };
                let query = bind_lowered_sql_select_query_structural(
                    authority.model(),
                    select,
                    MissingRowPolicy::Ignore,
                )
                .map_err(QueryError::from_sql_lowering_error)?;

                Ok(CompiledSqlCommand::Select {
                    query,
                    compiled_cache_key: None,
                })
            }
            SqlStatement::Delete(_) => {
                let prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;
                let normalized_statement = prepared.clone().into_statement();
                let lowered =
                    lower_sql_command_from_prepared_statement(prepared, authority.model())
                        .map_err(QueryError::from_sql_lowering_error)?;
                let Some(LoweredSqlQuery::Delete(query)) = lowered.into_query() else {
                    return Err(QueryError::invariant(
                        "compiled SQL DELETE lane must lower to lowered SQL DELETE",
                    ));
                };
                let SqlStatement::Delete(statement) = normalized_statement else {
                    return Err(QueryError::invariant(
                        "prepared SQL DELETE compilation must preserve DELETE statement ownership",
                    ));
                };

                Ok(CompiledSqlCommand::Delete { query, statement })
            }
            SqlStatement::Insert(_) => {
                let prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;
                let SqlStatement::Insert(statement) = prepared.into_statement() else {
                    return Err(QueryError::invariant(
                        "prepared SQL INSERT compilation must preserve INSERT statement ownership",
                    ));
                };

                Ok(CompiledSqlCommand::Insert(statement))
            }
            SqlStatement::Update(_) => {
                let prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;
                let SqlStatement::Update(statement) = prepared.into_statement() else {
                    return Err(QueryError::invariant(
                        "prepared SQL UPDATE compilation must preserve UPDATE statement ownership",
                    ));
                };

                Ok(CompiledSqlCommand::Update(statement))
            }
            SqlStatement::Explain(_) => {
                let prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;
                let lowered =
                    lower_sql_command_from_prepared_statement(prepared, authority.model())
                        .map_err(QueryError::from_sql_lowering_error)?;

                Ok(CompiledSqlCommand::Explain(lowered))
            }
            SqlStatement::Describe(_) => {
                let _prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;

                Ok(CompiledSqlCommand::DescribeEntity)
            }
            SqlStatement::ShowIndexes(_) => {
                let _prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;

                Ok(CompiledSqlCommand::ShowIndexesEntity)
            }
            SqlStatement::ShowColumns(_) => {
                let _prepared = Self::prepare_sql_statement_for_authority(statement, authority)?;

                Ok(CompiledSqlCommand::ShowColumnsEntity)
            }
            SqlStatement::ShowEntities(_) => Ok(CompiledSqlCommand::ShowEntities),
        }
    }
}
