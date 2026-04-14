//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while owner-local
//! submodules keep aggregate, write, and explain details out of the root.

mod aggregate;
mod lowered;
mod route;
mod write;

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::EntityAuthority,
        query::{intent::StructuralQuery, plan::AccessPlannedQuery},
        session::sql::{
            CompiledSqlCommand, SqlCompiledCommandCacheKey, SqlStatementResult,
            projection::{SqlProjectionPayload, execute_sql_projection_rows_for_canister},
        },
    },
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Build the shared structural SQL projection execution inputs once so
    // value-row and rendered-row statement surfaces only differ in final packaging.
    fn prepare_structural_sql_projection_execution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<(Vec<String>, AccessPlannedQuery), QueryError> {
        // Phase 1: build the structural access plan once and freeze its outward
        // column contract for all projection materialization surfaces.
        let entry =
            self.planned_sql_select_with_visibility(&query, authority, compiled_cache_key)?;
        let (plan, columns) = entry.into_parts();

        Ok((columns, plan))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<SqlProjectionPayload, QueryError> {
        // Phase 1: build the shared structural plan and outward column contract once.
        let (columns, plan) =
            self.prepare_structural_sql_projection_execution(query, authority, compiled_cache_key)?;

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics.
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_compiled_sql<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority = EntityAuthority::for_type::<E>();

        match compiled {
            CompiledSqlCommand::Select {
                query,
                compiled_cache_key,
            } => {
                if query.has_grouping() {
                    return self.execute_structural_sql_grouped_statement_select_core(
                        query.clone(),
                        authority,
                        compiled_cache_key.as_ref(),
                    );
                }

                let payload = self.execute_structural_sql_projection(
                    query.clone(),
                    authority,
                    compiled_cache_key.as_ref(),
                )?;

                Ok(payload.into_statement_result())
            }
            CompiledSqlCommand::Delete { query, statement } => {
                self.execute_sql_delete_statement::<E>(query.clone(), statement)
            }
            CompiledSqlCommand::GlobalAggregate {
                command,
                label_override,
            } => self.execute_global_aggregate_statement_for_authority(
                command.clone(),
                authority,
                label_override.clone(),
            ),
            CompiledSqlCommand::Explain(lowered) => {
                if let Some(explain) =
                    self.explain_lowered_sql_execution_for_authority(lowered, authority)?
                {
                    return Ok(SqlStatementResult::Explain(explain));
                }

                self.explain_lowered_sql_for_authority(lowered, authority)
                    .map(SqlStatementResult::Explain)
            }
            CompiledSqlCommand::Insert(statement) => {
                self.execute_sql_insert_statement::<E>(statement)
            }
            CompiledSqlCommand::Update(statement) => {
                self.execute_sql_update_statement::<E>(statement)
            }
            CompiledSqlCommand::DescribeEntity => {
                Ok(SqlStatementResult::Describe(self.describe_entity::<E>()))
            }
            CompiledSqlCommand::ShowIndexesEntity => {
                Ok(SqlStatementResult::ShowIndexes(self.show_indexes::<E>()))
            }
            CompiledSqlCommand::ShowColumnsEntity => {
                Ok(SqlStatementResult::ShowColumns(self.show_columns::<E>()))
            }
            CompiledSqlCommand::ShowEntities => {
                Ok(SqlStatementResult::ShowEntities(self.show_entities()))
            }
        }
    }

    /// Compile and then execute one parsed reduced SQL statement into one
    /// unified SQL payload for session-owned tests.
    #[cfg(test)]
    pub(in crate::db) fn execute_sql_statement_inner<E>(
        &self,
        sql_statement: &crate::db::sql::parser::SqlStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = Self::compile_sql_statement_inner::<E>(sql_statement)?;

        self.execute_compiled_sql::<E>(&compiled)
    }
}
