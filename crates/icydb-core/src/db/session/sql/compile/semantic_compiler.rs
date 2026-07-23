//! Module: db::session::sql::compile::semantic_compiler
//! Responsibility: cache-independent semantic compilation of parsed SQL
//! statements.
//! Does not own: SQL text parsing, compiled-command cache lookup, or execution.
//! Boundary: lowers prepared SQL into session-owned compiled command artifacts.

use std::sync::Arc;

#[cfg(feature = "sql-explain")]
use crate::db::sql::lowering::lower_sql_explain_command_from_prepared_statement_with_schema;
use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        schema::SchemaInfo,
        session::sql::{
            CompiledSqlCommand, CompiledSqlInsertCommand, SqlCompiledCommandSurface,
            compile::{SqlCompileArtifacts, SqlCompilePhaseAttribution},
            measured,
        },
        sql::{
            lowering::{
                PreparedSqlStatement, bind_lowered_sql_delete_query_structural_with_schema,
                bind_lowered_sql_select_query_structural_with_schema,
                bind_sql_select_statement_structural_with_schema,
                compile_sql_global_aggregate_command_from_prepared_with_schema,
                extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
                lower_prepared_sql_delete_statement,
                lower_prepared_sql_select_statement_with_schema, prepare_sql_statement,
            },
            parser::{
                SqlExpr, SqlInsertSource, SqlOrderDirection, SqlOrderTerm, SqlSelectStatement,
                SqlStatement,
            },
        },
    },
    model::entity::EntityModel,
    traits::CanisterKind,
};
use icydb_diagnostic_code::SqlLoweringCode;

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    fn compile_sql_statement_semantic_artifacts(
        statement: &SqlStatement,
        authority: EntityAuthority,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let model = authority.model();
        let entity_name = schema.entity_name().ok_or_else(QueryError::invariant)?;

        match statement {
            SqlStatement::Select(_) => Self::compile_select(statement, entity_name, model, schema),
            SqlStatement::Delete(_) => Self::compile_delete(statement, entity_name, model, schema),
            SqlStatement::Insert(_) => Self::compile_insert(statement, entity_name, model, schema),
            SqlStatement::Update(_) => Self::compile_update(statement, entity_name),
            SqlStatement::Ddl(_) => Err(QueryError::sql_lowering(
                SqlLoweringCode::SqlDdlExecutionUnsupported,
            )),
            #[cfg(feature = "sql-explain")]
            SqlStatement::Explain(_) => {
                Self::compile_explain(statement, entity_name, model, schema)
            }
            SqlStatement::Describe(_) => Self::compile_describe(statement, entity_name),
            SqlStatement::ShowConstraints(_) => {
                Self::compile_show_constraints(statement, entity_name)
            }
            SqlStatement::ShowIndexes(_) => Self::compile_show_indexes(statement, entity_name),
            SqlStatement::ShowColumns(_) => Self::compile_show_columns(statement, entity_name),
            SqlStatement::ShowEntities(statement) => Ok(Self::compile_show_entities(
                statement.entity.clone(),
                statement.verbose,
            )),
            SqlStatement::ShowStores(statement) => Ok(Self::compile_show_stores(statement.verbose)),
            SqlStatement::ShowMemory(_) => Ok(Self::compile_show_memory()),
        }
    }

    // Prepare one statement against a resolved schema entity name while
    // preserving the prepare-stage counter as a first-class compile artifact
    // field.
    fn prepare_statement_for_entity_name(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<(u64, PreparedSqlStatement), QueryError> {
        measured(|| {
            prepare_sql_statement(statement, entity_name)
                .map_err(QueryError::from_sql_lowering_error)
        })
    }

    // Compile SELECT by owning only lane detection. Each lane keeps its own
    // lowering/binding behavior so aggregate and scalar SELECTs do not share a
    // branch with different semantic assumptions.
    fn compile_select(
        statement: &SqlStatement,
        entity_name: &str,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;
        let (aggregate_lane_check_local_instructions, requires_aggregate_lane) =
            measured(|| Ok(prepared.statement().is_global_aggregate_lane_shape()))?;

        if requires_aggregate_lane {
            Self::compile_select_global_aggregate(
                prepared,
                model,
                schema,
                aggregate_lane_check_local_instructions,
                prepare_local_instructions,
            )
        } else {
            Self::compile_select_non_aggregate(
                prepared,
                model,
                schema,
                aggregate_lane_check_local_instructions,
                prepare_local_instructions,
            )
        }
    }

    // Compile one prepared SELECT that belongs on the global aggregate lane.
    // This path intentionally stays separate from scalar SELECT binding so
    // aggregate-specific lowering and future aggregate detection changes have
    // one narrow owner.
    fn compile_select_global_aggregate(
        prepared: PreparedSqlStatement,
        model: &'static EntityModel,
        schema: &SchemaInfo,
        aggregate_lane_check_local_instructions: u64,
        prepare_local_instructions: u64,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (lower_local_instructions, command) = measured(|| {
            compile_sql_global_aggregate_command_from_prepared_with_schema(
                prepared,
                model,
                MissingRowPolicy::Ignore,
                schema,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::global_aggregate(command),
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            0,
        ))
    }

    // Compile one prepared SELECT that remains on the ordinary scalar query
    // lane. Projection/query binding stays here instead of sharing branches
    // with the aggregate path.
    fn compile_select_non_aggregate(
        prepared: PreparedSqlStatement,
        model: &'static EntityModel,
        schema: &SchemaInfo,
        aggregate_lane_check_local_instructions: u64,
        prepare_local_instructions: u64,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (lower_local_instructions, select) = measured(|| {
            lower_prepared_sql_select_statement_with_schema(prepared, model, schema)
                .map_err(QueryError::from_sql_lowering_error)
        })?;
        let (bind_local_instructions, query) = measured(|| {
            bind_lowered_sql_select_query_structural_with_schema(
                model,
                select,
                MissingRowPolicy::Ignore,
                schema,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::select(query),
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ))
    }

    // Compile DELETE through the same prepare/lower/bind phases as ordinary
    // SELECTs while preserving DELETE-specific RETURNING extraction.
    fn compile_delete(
        statement: &SqlStatement,
        entity_name: &str,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;
        let (lower_local_instructions, delete) = measured(|| {
            lower_prepared_sql_delete_statement(prepared)
                .map_err(QueryError::from_sql_lowering_error)
        })?;
        let returning = delete.returning().cloned();
        let query = delete.into_base_query();
        let (bind_local_instructions, query) = measured(|| {
            bind_lowered_sql_delete_query_structural_with_schema(
                model,
                query,
                MissingRowPolicy::Ignore,
                schema,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Delete {
                query: Arc::new(query),
                returning,
            },
            0,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ))
    }

    // Compile INSERT after the shared prepare phase. Prepared statement
    // extraction intentionally remains outside the lower/bind counters because
    // the historical INSERT path has no separate lower or bind stage.
    fn compile_insert(
        statement: &SqlStatement,
        entity_name: &str,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;
        let statement = extract_prepared_sql_insert_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;
        let (bind_local_instructions, source_query) =
            Self::compile_insert_select_source_query(&statement.source, model, schema)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Insert(CompiledSqlInsertCommand::new(statement, source_query)),
            0,
            prepare_local_instructions,
            0,
            bind_local_instructions,
        ))
    }

    // Compile the SELECT source for INSERT SELECT once while the SQL compiled
    // command cache owns the accepted schema snapshot and model authority.
    fn compile_insert_select_source_query(
        source: &SqlInsertSource,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<(u64, Option<crate::db::query::intent::StructuralQuery>), QueryError> {
        let SqlInsertSource::Select(source) = source else {
            return Ok((0, None));
        };
        let source = insert_select_source_with_primary_key_order(
            source.as_ref(),
            schema.primary_key_names(),
        )?;
        let (bind_local_instructions, query) = measured(|| {
            bind_sql_select_statement_structural_with_schema(
                source,
                model,
                MissingRowPolicy::Ignore,
                schema,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok((bind_local_instructions, Some(query)))
    }

    // Compile UPDATE after the shared prepare phase. Like INSERT, UPDATE owns
    // only prepared-statement extraction here to preserve existing attribution.
    fn compile_update(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;
        let statement = extract_prepared_sql_update_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Update(statement),
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile EXPLAIN by lowering its prepared target but deliberately not
    // binding it into an executable query, matching the explain-only contract.
    #[cfg(feature = "sql-explain")]
    fn compile_explain(
        statement: &SqlStatement,
        entity_name: &str,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;
        let (lower_local_instructions, lowered) = measured(|| {
            lower_sql_explain_command_from_prepared_statement_with_schema(prepared, model, schema)
                .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Explain(Box::new(lowered)),
            0,
            prepare_local_instructions,
            lower_local_instructions,
            0,
        ))
    }

    // Compile DESCRIBE by validating the prepared surface and returning the
    // fixed introspection command without a lower or bind stage.
    fn compile_describe(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::DescribeEntity,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW INDEXES by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_indexes(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowIndexesEntity,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW CONSTRAINTS by validating the prepared surface and
    // returning the fixed accepted-catalog introspection command.
    fn compile_show_constraints(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowConstraintsEntity,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW COLUMNS by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_columns(
        statement: &SqlStatement,
        entity_name: &str,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_entity_name(statement, entity_name)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowColumnsEntity,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW ENTITIES without entity-bound preparation because the
    // command is catalog-backed and historically reports no compile sub-stages.
    const fn compile_show_entities(entity: Option<String>, verbose: bool) -> SqlCompileArtifacts {
        SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowEntities { entity, verbose },
            0,
            0,
            0,
            0,
        )
    }

    // Compile SHOW STORES without entity-bound preparation because the command
    // is catalog-wide and historically reports no compile sub-stages.
    const fn compile_show_stores(verbose: bool) -> SqlCompileArtifacts {
        SqlCompileArtifacts::new(CompiledSqlCommand::ShowStores { verbose }, 0, 0, 0, 0)
    }

    // Compile SHOW MEMORY without entity-bound preparation because the command
    // is catalog-wide and historically reports no compile sub-stages.
    const fn compile_show_memory() -> SqlCompileArtifacts {
        SqlCompileArtifacts::new(CompiledSqlCommand::ShowMemory, 0, 0, 0, 0)
    }

    // Own the complete parsed-statement compile boundary: surface validation
    // happens here before the cache-independent semantic compiler runs, so no
    // caller can accidentally compile a query through the update lane or the
    // inverse.
    fn compile_sql_statement_entry(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        authority: EntityAuthority,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        Self::ensure_sql_statement_supported_for_surface(statement, surface)?;

        Self::compile_sql_statement_semantic_artifacts(statement, authority, schema)
    }

    // Wrap the complete compile entrypoint with the attribution shape used by
    // callers. The semantic artifact remains the single authority for command
    // output and stage-local compile counters.
    pub(in crate::db::session::sql) fn compile_sql_statement_measured(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        authority: EntityAuthority,
        schema: &SchemaInfo,
    ) -> Result<(SqlCompileArtifacts, SqlCompilePhaseAttribution), QueryError> {
        let artifacts = Self::compile_sql_statement_entry(statement, surface, authority, schema)?;
        debug_assert!(
            !artifacts.shape.is_aggregate || artifacts.bind == 0,
            "aggregate SQL artifacts must not report scalar bind work"
        );
        debug_assert!(
            !artifacts.shape.is_mutation || artifacts.aggregate_lane_check == 0,
            "mutation SQL artifacts must not report SELECT lane checks"
        );
        let attribution = artifacts.phase_attribution();

        Ok((artifacts, attribution))
    }
}

fn insert_select_source_with_primary_key_order(
    source: &SqlSelectStatement,
    primary_key_names: &[String],
) -> Result<SqlSelectStatement, QueryError> {
    if primary_key_names.is_empty() {
        return Err(QueryError::invariant());
    }

    let mut source = source.clone();
    for primary_key_name in primary_key_names {
        if source
            .order_by
            .iter()
            .any(|term| matches!(&term.field, SqlExpr::Field(field) if field == primary_key_name))
        {
            continue;
        }

        source.order_by.push(SqlOrderTerm {
            field: SqlExpr::Field(primary_key_name.clone()),
            direction: SqlOrderDirection::Asc,
        });
    }

    Ok(source)
}
