//! Module: db::session::sql::compile::core
//! Responsibility: cache-independent semantic compilation of parsed SQL
//! statements.
//! Does not own: SQL text parsing, compiled-command cache lookup, or execution.
//! Boundary: lowers prepared SQL into session-owned compiled command artifacts.

use std::sync::Arc;

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        schema::SchemaInfo,
        session::sql::{
            CompiledSqlCommand, SqlCompiledCommandSurface,
            compile::{SqlCompileArtifacts, SqlCompilePhaseAttribution, SqlQueryShape},
            measured,
        },
        sql::{
            lowering::{
                PreparedSqlStatement, bind_lowered_sql_delete_query_structural_with_schema,
                bind_lowered_sql_select_query_structural_with_schema,
                compile_sql_global_aggregate_command_core_from_prepared_with_schema,
                extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
                lower_prepared_sql_delete_statement, lower_prepared_sql_select_statement,
                lower_sql_command_from_prepared_statement, prepare_sql_statement,
            },
            parser::SqlStatement,
        },
    },
    model::entity::EntityModel,
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    fn compile_sql_statement_core(
        statement: &SqlStatement,
        authority: EntityAuthority,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let model = authority.model();

        match statement {
            SqlStatement::Select(_) => Self::compile_select(statement, model, schema),
            SqlStatement::Delete(_) => Self::compile_delete(statement, model, schema),
            SqlStatement::Insert(_) => Self::compile_insert(statement, model),
            SqlStatement::Update(_) => Self::compile_update(statement, model),
            SqlStatement::Explain(_) => Self::compile_explain(statement, model),
            SqlStatement::Describe(_) => Self::compile_describe(statement, model),
            SqlStatement::ShowIndexes(_) => Self::compile_show_indexes(statement, model),
            SqlStatement::ShowColumns(_) => Self::compile_show_columns(statement, model),
            SqlStatement::ShowEntities(_) => Ok(Self::compile_show_entities()),
        }
    }

    // Prepare one statement against a resolved entity model while preserving
    // the prepare-stage counter as a first-class compile artifact field.
    fn prepare_statement_for_model(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<(u64, PreparedSqlStatement), QueryError> {
        measured(|| {
            prepare_sql_statement(statement, model.name())
                .map_err(QueryError::from_sql_lowering_error)
        })
    }

    // Compile SELECT by owning only lane detection. Each lane keeps its own
    // lowering/binding behavior so aggregate and scalar SELECTs do not share a
    // branch with different semantic assumptions.
    fn compile_select(
        statement: &SqlStatement,
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
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
            compile_sql_global_aggregate_command_core_from_prepared_with_schema(
                prepared,
                model,
                MissingRowPolicy::Ignore,
                schema,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::GlobalAggregate {
                command: Box::new(command),
            },
            SqlQueryShape::read_rows(true),
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
            lower_prepared_sql_select_statement(prepared, model)
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
            CompiledSqlCommand::Select {
                query: Arc::new(query),
            },
            SqlQueryShape::read_rows(false),
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
        model: &'static EntityModel,
        schema: &SchemaInfo,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
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

        let shape = SqlQueryShape::mutation(returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Delete {
                query: Arc::new(query),
                returning,
            },
            shape,
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
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let statement = extract_prepared_sql_insert_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        let shape = SqlQueryShape::mutation(statement.returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Insert(statement),
            shape,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile UPDATE after the shared prepare phase. Like INSERT, UPDATE owns
    // only prepared-statement extraction here to preserve existing attribution.
    fn compile_update(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let statement = extract_prepared_sql_update_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        let shape = SqlQueryShape::mutation(statement.returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Update(statement),
            shape,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile EXPLAIN by lowering its prepared target but deliberately not
    // binding it into an executable query, matching the explain-only contract.
    fn compile_explain(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let (lower_local_instructions, lowered) = measured(|| {
            lower_sql_command_from_prepared_statement(prepared, model)
                .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Explain(Box::new(lowered)),
            SqlQueryShape::metadata(),
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
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::DescribeEntity,
            SqlQueryShape::metadata(),
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
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowIndexesEntity,
            SqlQueryShape::metadata(),
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
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowColumnsEntity,
            SqlQueryShape::metadata(),
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW ENTITIES without entity-bound preparation because the
    // command is catalog-wide and historically reports no compile sub-stages.
    fn compile_show_entities() -> SqlCompileArtifacts {
        SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowEntities,
            SqlQueryShape::metadata(),
            0,
            0,
            0,
            0,
        )
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

        Self::compile_sql_statement_core(statement, authority, schema)
    }

    // Wrap the complete compile entrypoint with the attribution shape used by
    // callers. The core artifact remains the single authority for command
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
