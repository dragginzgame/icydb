//! Module: db::session::sql::compile::semantic_compiler
//! Responsibility: cache-independent semantic compilation of parsed SQL
//! statements.
//! Does not own: SQL text parsing, compiled-command cache lookup, or execution.
//! Boundary: lowers prepared SQL into session-owned compiled command artifacts.

use crate::db::query::preparation::PreparationWork;
use std::rc::Rc;

#[cfg(feature = "sql")]
use crate::db::sql::lowering::lower_sql_explain_command_from_prepared_statement_with_schema;
use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        schema::SchemaInfo,
        session::sql::{CompiledSqlCommand, CompiledSqlInsertCommand, SqlCompiledCommandSurface},
        sql::{
            lowering::{
                PreparedSqlStatement, bind_lowered_sql_delete_query_structural_with_schema,
                bind_lowered_sql_select_query_structural_with_schema,
                bind_sql_select_statement_structural_with_schema,
                compile_sql_global_aggregate_command_from_prepared_with_schema,
                extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
                lower_prepared_sql_delete_statement,
                lower_prepared_sql_select_statement_with_schema, prepare_bound_sql_statement,
                prepare_sql_statement,
            },
            parser::{
                SqlExpr, SqlInsertSource, SqlOrderDirection, SqlOrderTerm, SqlSelectStatement,
                SqlStatement,
            },
        },
    },
    traits::CanisterKind,
    value::InputValue,
};
use icydb_diagnostic_code::{DiagnosticExecutionLane, SqlLoweringCode};

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    fn compile_sql_statement_semantic(
        statement: &SqlStatement,
        schema: &SchemaInfo,
        bindings: &[InputValue],
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let entity_name = schema.entity_name().ok_or_else(QueryError::invariant)?;

        match statement {
            SqlStatement::Select(_) => {
                Self::compile_select(statement, entity_name, schema, bindings, work)
            }
            SqlStatement::Delete(_) => Self::compile_delete(statement, entity_name, schema, work),
            SqlStatement::Insert(_) => Self::compile_insert(statement, entity_name, schema, work),
            SqlStatement::Update(_) => Self::compile_update(statement, entity_name, work),
            SqlStatement::Ddl(_) => Err(QueryError::sql_lowering(
                SqlLoweringCode::SqlDdlExecutionUnsupported,
            )),
            #[cfg(feature = "sql")]
            SqlStatement::Explain(_) => Self::compile_explain(statement, entity_name, schema, work),
            SqlStatement::Describe(_) => Self::compile_describe(statement, entity_name, work),
            SqlStatement::ShowConstraints(_) => {
                Self::compile_show_constraints(statement, entity_name, work)
            }
            SqlStatement::ShowIndexes(_) => {
                Self::compile_show_indexes(statement, entity_name, work)
            }
            SqlStatement::ShowColumns(_) => {
                Self::compile_show_columns(statement, entity_name, work)
            }
            SqlStatement::ShowRelations(_) => {
                Self::compile_show_relations(statement, entity_name, work)
            }
            SqlStatement::ShowEntities(statement) => Ok(Self::compile_show_entities(
                statement.entity.clone(),
                statement.verbose,
            )),
            SqlStatement::ShowStores(statement) => Ok(Self::compile_show_stores(statement.verbose)),
            SqlStatement::ShowMemory(_) => Ok(Self::compile_show_memory()),
        }
    }

    // Prepare one statement against a resolved schema entity name.
    fn prepare_statement_for_entity_name(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<PreparedSqlStatement, QueryError> {
        prepare_sql_statement(statement, entity_name, work)
            .map_err(QueryError::from_sql_lowering_error)
    }

    // Compile SELECT by owning only lane detection. Each lane keeps its own
    // lowering/binding behavior so aggregate and scalar SELECTs do not share a
    // branch with different semantic assumptions.
    fn compile_select(
        statement: &SqlStatement,
        entity_name: &str,
        schema: &SchemaInfo,
        bindings: &[InputValue],
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let prepared = if bindings.is_empty() {
            Self::prepare_statement_for_entity_name(statement, entity_name, work)?
        } else {
            prepare_bound_sql_statement(statement, entity_name, schema, bindings, work)?
        };
        let requires_aggregate_lane = prepared
            .statement()
            .is_global_aggregate_lane_shape(work)
            .map_err(QueryError::from_sql_lowering_error)?;

        if requires_aggregate_lane {
            Self::compile_select_global_aggregate(prepared, schema, work)
        } else {
            Self::compile_select_non_aggregate(prepared, schema, work)
        }
    }

    // Compile one prepared SELECT that belongs on the global aggregate lane.
    // This path intentionally stays separate from scalar SELECT binding so
    // aggregate-specific lowering and future aggregate detection changes have
    // one narrow owner.
    fn compile_select_global_aggregate(
        prepared: PreparedSqlStatement,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let command = compile_sql_global_aggregate_command_from_prepared_with_schema(
            prepared,
            MissingRowPolicy::Ignore,
            schema,
            work,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(CompiledSqlCommand::global_aggregate(command))
    }

    // Compile one prepared SELECT that remains on the ordinary scalar query
    // lane. Projection/query binding stays here instead of sharing branches
    // with the aggregate path.
    fn compile_select_non_aggregate(
        prepared: PreparedSqlStatement,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let select = lower_prepared_sql_select_statement_with_schema(prepared, schema, work)
            .map_err(QueryError::from_sql_lowering_error)?;
        let query = bind_lowered_sql_select_query_structural_with_schema(
            select,
            MissingRowPolicy::Ignore,
            schema,
            work,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(CompiledSqlCommand::select(query))
    }

    // Compile DELETE through the same prepare/lower/bind phases as ordinary
    // SELECTs while preserving DELETE-specific RETURNING extraction.
    fn compile_delete(
        statement: &SqlStatement,
        entity_name: &str,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;
        let delete = lower_prepared_sql_delete_statement(prepared, work)
            .map_err(QueryError::from_sql_lowering_error)?;
        let returning = delete.returning().cloned();
        let query = delete.into_base_query();
        let query = bind_lowered_sql_delete_query_structural_with_schema(
            query,
            MissingRowPolicy::Ignore,
            schema,
            work,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(CompiledSqlCommand::Delete {
            query: Rc::new(query),
            returning,
        })
    }

    // Compile INSERT after the shared prepare phase.
    fn compile_insert(
        statement: &SqlStatement,
        entity_name: &str,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;
        let statement = extract_prepared_sql_insert_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;
        let source_query =
            Self::compile_insert_select_source_query(&statement.source, schema, work)?;

        Ok(CompiledSqlCommand::Insert(CompiledSqlInsertCommand::new(
            statement,
            source_query,
        )))
    }

    // Compile the SELECT source for INSERT SELECT once while the SQL compiled
    // command cache owns the accepted schema snapshot and model authority.
    fn compile_insert_select_source_query(
        source: &SqlInsertSource,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<Option<crate::db::query::intent::StructuralQuery>, QueryError> {
        let SqlInsertSource::Select(source) = source else {
            return Ok(None);
        };
        let source = insert_select_source_with_primary_key_order(
            source.as_ref(),
            schema.primary_key_names(),
        )?;
        let query = bind_sql_select_statement_structural_with_schema(
            source,
            MissingRowPolicy::Ignore,
            schema,
            work,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(Some(query))
    }

    // Compile UPDATE after the shared prepare phase.
    fn compile_update(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;
        let statement = extract_prepared_sql_update_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        Ok(CompiledSqlCommand::Update(statement))
    }

    // Compile EXPLAIN by lowering its prepared target but deliberately not
    // binding it into an executable query, matching the explain-only contract.
    #[cfg(feature = "sql")]
    fn compile_explain(
        statement: &SqlStatement,
        entity_name: &str,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;
        let lowered =
            lower_sql_explain_command_from_prepared_statement_with_schema(prepared, schema, work)
                .map_err(QueryError::from_sql_lowering_error)?;

        Ok(CompiledSqlCommand::Explain(Box::new(lowered)))
    }

    // Compile DESCRIBE by validating the prepared surface and returning the
    // fixed introspection command without a lower or bind stage.
    fn compile_describe(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let _prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;

        let SqlStatement::Describe(describe) = statement else {
            return Err(QueryError::invariant());
        };
        Ok(CompiledSqlCommand::DescribeEntity {
            mode: describe.mode,
        })
    }

    // Compile SHOW INDEXES by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_indexes(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let _prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;

        Ok(CompiledSqlCommand::ShowIndexesEntity)
    }

    // Compile SHOW CONSTRAINTS by validating the prepared surface and
    // returning the fixed accepted-catalog introspection command.
    fn compile_show_constraints(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let _prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;

        Ok(CompiledSqlCommand::ShowConstraintsEntity)
    }

    // Compile SHOW COLUMNS by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_columns(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let _prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;

        let SqlStatement::ShowColumns(show_columns) = statement else {
            return Err(QueryError::invariant());
        };
        Ok(CompiledSqlCommand::ShowColumnsEntity {
            mode: show_columns.mode,
        })
    }

    // Compile SHOW RELATIONS into the fixed accepted-catalog projection.
    fn compile_show_relations(
        statement: &SqlStatement,
        entity_name: &str,
        work: &PreparationWork<'_>,
    ) -> Result<CompiledSqlCommand, QueryError> {
        let _prepared = Self::prepare_statement_for_entity_name(statement, entity_name, work)?;

        Ok(CompiledSqlCommand::ShowRelationsEntity)
    }

    // Compile SHOW ENTITIES without entity-bound preparation because the
    // command is catalog-backed and historically reports no compile sub-stages.
    const fn compile_show_entities(entity: Option<String>, verbose: bool) -> CompiledSqlCommand {
        CompiledSqlCommand::ShowEntities { entity, verbose }
    }

    // Compile SHOW STORES without entity-bound preparation because the command
    // is catalog-wide and historically reports no compile sub-stages.
    const fn compile_show_stores(verbose: bool) -> CompiledSqlCommand {
        CompiledSqlCommand::ShowStores { verbose }
    }

    // Compile SHOW MEMORY without entity-bound preparation because the command
    // is catalog-wide and historically reports no compile sub-stages.
    const fn compile_show_memory() -> CompiledSqlCommand {
        CompiledSqlCommand::ShowMemory
    }

    // Own the complete parsed-statement compile boundary: surface validation
    // happens here before the cache-independent semantic compiler runs, so no
    // caller can accidentally compile a query through the update lane or the
    // inverse.
    pub(in crate::db::session::sql) fn compile_sql_statement(
        &self,
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        schema: &SchemaInfo,
        bindings: &[InputValue],
    ) -> Result<CompiledSqlCommand, QueryError> {
        Self::ensure_sql_statement_supported_for_surface(statement, surface)?;

        let lane = match (surface, statement) {
            (SqlCompiledCommandSurface::Mutation, _) => DiagnosticExecutionLane::Mutation,
            (_, SqlStatement::Explain(_)) => DiagnosticExecutionLane::Diagnostic,
            _ => DiagnosticExecutionLane::TrustedRead,
        };
        PreparationWork::run(self.db.request_execution_scope(), lane, |work| {
            Self::compile_sql_statement_semantic(statement, schema, bindings, work)
        })
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
