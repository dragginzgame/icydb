use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        session::sql::{CompiledSqlCommand, measure_sql_stage},
        sql::lowering::{
            LoweredSqlQuery, SqlLoweringError,
            compile_sql_global_aggregate_command_core_from_prepared,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::CanisterKind,
};

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
    #[expect(clippy::too_many_lines)]
    pub(in crate::db::session::sql) fn compile_sql_statement_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
        compiled_cache_key: crate::db::session::sql::SqlCompiledCommandCacheKey,
    ) -> Result<(CompiledSqlCommand, u64, u64, u64, u64), QueryError> {
        match statement {
            SqlStatement::Select(_) => {
                let (aggregate_lane_check_local_instructions, requires_aggregate_lane) =
                    measure_sql_stage(|| {
                        Ok::<_, QueryError>(Self::sql_query_requires_aggregate_lane(statement))
                    });
                let requires_aggregate_lane = requires_aggregate_lane?;

                if requires_aggregate_lane {
                    let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                        Self::prepare_sql_statement_for_authority(statement, authority)
                    });
                    let prepared = prepared?;
                    let (lower_local_instructions, command) = measure_sql_stage(|| {
                        compile_sql_global_aggregate_command_core_from_prepared(
                            prepared,
                            authority.model(),
                            MissingRowPolicy::Ignore,
                        )
                        .map_err(QueryError::from_sql_lowering_error)
                    });
                    let command = command?;

                    Ok((
                        CompiledSqlCommand::GlobalAggregate { command },
                        aggregate_lane_check_local_instructions,
                        prepare_local_instructions,
                        lower_local_instructions,
                        0,
                    ))
                } else {
                    let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                        Self::prepare_sql_statement_for_authority(statement, authority)
                    });
                    let prepared = prepared?;
                    let (lower_local_instructions, lowered) = measure_sql_stage(|| {
                        lower_sql_command_from_prepared_statement(prepared, authority.model()).map_err(
                        |err| match err {
                            SqlLoweringError::UnexpectedQueryLaneStatement => {
                                QueryError::invariant(
                                    "query-lane SQL lowering reached a non query-compatible statement",
                                )
                            }
                            other => QueryError::from_sql_lowering_error(other),
                        },
                    )
                    });
                    let lowered = lowered?;
                    let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
                        return Err(QueryError::invariant(
                            "compiled SQL SELECT lane must lower to lowered SQL SELECT",
                        ));
                    };
                    let (bind_local_instructions, query) = measure_sql_stage(|| {
                        Self::structural_query_from_lowered_select(select, authority)
                    });
                    let query = query?;

                    Ok((
                        CompiledSqlCommand::new_select(query, compiled_cache_key),
                        aggregate_lane_check_local_instructions,
                        prepare_local_instructions,
                        lower_local_instructions,
                        bind_local_instructions,
                    ))
                }
            }
            SqlStatement::Delete(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let prepared = prepared?;
                let normalized_statement = prepared.clone().into_statement();
                let (lower_local_instructions, lowered) = measure_sql_stage(|| {
                    lower_sql_command_from_prepared_statement(prepared, authority.model())
                        .map_err(QueryError::from_sql_lowering_error)
                });
                let lowered = lowered?;
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

                Ok((
                    CompiledSqlCommand::Delete { query, statement },
                    0,
                    prepare_local_instructions,
                    lower_local_instructions,
                    0,
                ))
            }
            SqlStatement::Insert(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let prepared = prepared?;
                let SqlStatement::Insert(statement) = prepared.into_statement() else {
                    return Err(QueryError::invariant(
                        "prepared SQL INSERT compilation must preserve INSERT statement ownership",
                    ));
                };

                Ok((
                    CompiledSqlCommand::Insert(statement),
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::Update(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let prepared = prepared?;
                let SqlStatement::Update(statement) = prepared.into_statement() else {
                    return Err(QueryError::invariant(
                        "prepared SQL UPDATE compilation must preserve UPDATE statement ownership",
                    ));
                };

                Ok((
                    CompiledSqlCommand::Update(statement),
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::Explain(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let prepared = prepared?;
                let (lower_local_instructions, lowered) = measure_sql_stage(|| {
                    lower_sql_command_from_prepared_statement(prepared, authority.model())
                        .map_err(QueryError::from_sql_lowering_error)
                });
                let lowered = lowered?;

                Ok((
                    CompiledSqlCommand::Explain(lowered),
                    0,
                    prepare_local_instructions,
                    lower_local_instructions,
                    0,
                ))
            }
            SqlStatement::Describe(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let _prepared = prepared?;

                Ok((
                    CompiledSqlCommand::DescribeEntity,
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::ShowIndexes(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let _prepared = prepared?;

                Ok((
                    CompiledSqlCommand::ShowIndexesEntity,
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::ShowColumns(_) => {
                let (prepare_local_instructions, prepared) = measure_sql_stage(|| {
                    Self::prepare_sql_statement_for_authority(statement, authority)
                });
                let _prepared = prepared?;

                Ok((
                    CompiledSqlCommand::ShowColumnsEntity,
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::ShowEntities(_) => Ok((CompiledSqlCommand::ShowEntities, 0, 0, 0, 0)),
        }
    }
}
