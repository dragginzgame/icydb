use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        session::sql::{CompiledSqlCommand, measure_sql_stage},
        sql::identifier::identifiers_tail_match,
        sql::lowering::{
            LoweredSqlQuery, SqlLoweringError, bind_lowered_sql_select_query_structural,
            compile_sql_global_aggregate_command_core_from_prepared,
            is_sql_global_aggregate_statement, lower_sql_command_from_prepared_statement,
            prepare_sql_statement,
        },
        sql::parser::SqlStatement,
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    #[expect(clippy::too_many_lines)]
    pub(in crate::db::session::sql) fn compile_sql_statement_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
        compiled_cache_key: crate::db::session::sql::SqlCompiledCommandCacheKey,
    ) -> Result<(CompiledSqlCommand, u64, u64, u64, u64), QueryError> {
        // Reuse one local preparation closure so the session compile surface
        // stops hopping through another module-level authority wrapper before
        // it reaches the real prepared-statement owner.
        let prepare_statement = || {
            measure_sql_stage(|| {
                prepare_sql_statement(statement.clone(), authority.model().name())
                    .map_err(QueryError::from_sql_lowering_error)
            })
        };

        // Keep metadata-only entity checks local to the compile lane now that
        // they no longer need a second wrapper layer either.
        let validate_metadata_entity = |sql_entity: &str| {
            if identifiers_tail_match(sql_entity, authority.model().name()) {
                return Ok(());
            }

            Err(QueryError::from_sql_lowering_error(
                SqlLoweringError::EntityMismatch {
                    sql_entity: sql_entity.to_string(),
                    expected_entity: authority.model().name(),
                },
            ))
        };

        match statement {
            SqlStatement::Select(_) => {
                let (prepare_local_instructions, prepared) = prepare_statement();
                let prepared = prepared?;
                let (aggregate_lane_check_local_instructions, requires_aggregate_lane) =
                    measure_sql_stage(|| {
                        Ok::<_, QueryError>(is_sql_global_aggregate_statement(prepared.statement()))
                    });
                let requires_aggregate_lane = requires_aggregate_lane?;

                if requires_aggregate_lane {
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
                        CompiledSqlCommand::GlobalAggregate {
                            command: Box::new(command),
                        },
                        aggregate_lane_check_local_instructions,
                        prepare_local_instructions,
                        lower_local_instructions,
                        0,
                    ))
                } else {
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
                        bind_lowered_sql_select_query_structural(
                            authority.model(),
                            select,
                            MissingRowPolicy::Ignore,
                        )
                        .map_err(QueryError::from_sql_lowering_error)
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, validated) = measure_sql_stage(|| {
                    let SqlStatement::Describe(statement) = statement else {
                        return Err(QueryError::invariant(
                            "compiled SQL DESCRIBE lane must preserve DESCRIBE statement ownership",
                        ));
                    };

                    validate_metadata_entity(statement.entity.as_str())
                });
                validated?;

                Ok((
                    CompiledSqlCommand::DescribeEntity,
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::ShowIndexes(_) => {
                let (prepare_local_instructions, validated) = measure_sql_stage(|| {
                    let SqlStatement::ShowIndexes(statement) = statement else {
                        return Err(QueryError::invariant(
                            "compiled SQL SHOW INDEXES lane must preserve SHOW INDEXES statement ownership",
                        ));
                    };

                    validate_metadata_entity(statement.entity.as_str())
                });
                validated?;

                Ok((
                    CompiledSqlCommand::ShowIndexesEntity,
                    0,
                    prepare_local_instructions,
                    0,
                    0,
                ))
            }
            SqlStatement::ShowColumns(_) => {
                let (prepare_local_instructions, validated) = measure_sql_stage(|| {
                    let SqlStatement::ShowColumns(statement) = statement else {
                        return Err(QueryError::invariant(
                            "compiled SQL SHOW COLUMNS lane must preserve SHOW COLUMNS statement ownership",
                        ));
                    };

                    validate_metadata_entity(statement.entity.as_str())
                });
                validated?;

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
