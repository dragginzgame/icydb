//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod attribution;
mod cache;
mod compile;
mod compile_cache;
mod compiled;
mod execute;
mod projection;
mod result;

#[cfg(feature = "diagnostics")]
use crate::db::DataStore;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
#[cfg(feature = "diagnostics")]
use crate::db::{GroupedCountAttribution, GroupedExecutionAttribution};
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        schema::execute_sql_ddl_field_path_index_addition,
        schema::{AcceptedSchemaSnapshot, SchemaInfo},
        session::query::QueryPlanCacheAttribution,
        session::sql::projection::{
            projection_fixed_scales_from_projection_spec, projection_labels_from_projection_spec,
        },
        sql::{
            ddl::{PreparedSqlDdlCommand, prepare_sql_ddl_statement},
            parser::{SqlStatement, parse_sql_with_attribution},
        },
    },
    traits::{CanisterKind, EntityValue, Path},
};

pub(in crate::db::session::sql) use crate::db::diagnostics::measure_local_instruction_delta as measure_sql_stage;
pub use crate::db::sql::ddl::{SqlDdlExecutionStatus, SqlDdlMutationKind, SqlDdlPreparationReport};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use attribution::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub use attribution::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
};
pub(in crate::db) use cache::{SqlCacheAttribution, SqlCompiledCommandCacheKey};
pub(in crate::db::session::sql) use cache::{
    SqlCompiledCommandSurface, sql_compiled_command_cache_miss_reason,
};
pub(in crate::db::session::sql) use compile::{
    SqlCompileAttributionBuilder, SqlCompilePhaseAttribution,
};
pub(in crate::db) use compiled::{CompiledSqlCommand, SqlProjectionContract};
pub use result::SqlStatementResult;

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
#[cfg(test)]
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

// Measure one SQL compile stage and immediately surface the stage result. The
// helper keeps attribution capture uniform while avoiding repeated
// `(cost, result); result?` boilerplate across the compile pipeline.
fn measured<T>(stage: impl FnOnce() -> Result<T, QueryError>) -> Result<(u64, T), QueryError> {
    let (local_instructions, result) = measure_sql_stage(stage);
    let value = result?;

    Ok((local_instructions, value))
}

impl<C: CanisterKind> DbSession<C> {
    // Resolve one SQL SELECT through a caller-selected accepted authority and
    // accepted schema snapshot. Typed SQL entrypoints use this to avoid passing
    // generated authority through the runtime cache boundary.
    fn sql_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority(
                authority.clone(),
                accepted_schema,
                query,
            )?;
        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    // Resolve one typed SQL SELECT through accepted schema authority selected
    // at the session boundary.
    fn sql_select_prepared_plan_for_entity<E>(
        &self,
        query: &StructuralQuery,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (accepted_schema, authority) = self
            .accepted_entity_authority::<E>()
            .map_err(QueryError::execute)?;

        self.sql_select_prepared_plan_for_accepted_authority(query, authority, &accepted_schema)
    }

    fn sql_select_projection_from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        authority: EntityAuthority,
        cache_attribution: QueryPlanCacheAttribution,
    ) -> (
        SharedPreparedExecutionPlan,
        SqlProjectionContract,
        SqlCacheAttribution,
    ) {
        let projection_spec = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let projection = SqlProjectionContract::new(
            projection_labels_from_projection_spec(&projection_spec),
            projection_fixed_scales_from_projection_spec(&projection_spec),
        );

        (
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        )
    }

    // Keep query/update surface gating owned by one helper so the SQL
    // compiled-command lane does not duplicate the same statement-family split
    // just to change the outward error wording.
    fn ensure_sql_statement_supported_for_surface(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
    ) -> Result<(), QueryError> {
        match (surface, statement) {
            (
                SqlCompiledCommandSurface::Query,
                SqlStatement::Select(_)
                | SqlStatement::Explain(_)
                | SqlStatement::Describe(_)
                | SqlStatement::ShowIndexes(_)
                | SqlStatement::ShowColumns(_)
                | SqlStatement::ShowEntities(_),
            )
            | (
                SqlCompiledCommandSurface::Update,
                SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_),
            ) => Ok(()),
            (_, SqlStatement::Ddl(_)) => Err(QueryError::unsupported_query(
                "SQL DDL execution is not supported in this release",
            )),
            (SqlCompiledCommandSurface::Query, SqlStatement::Insert(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Query, SqlStatement::Update(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Query, SqlStatement::Delete(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Select(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SELECT; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Explain(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects EXPLAIN; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Describe(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects DESCRIBE; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowIndexes(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW INDEXES; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowColumns(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW COLUMNS; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowEntities(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW ENTITIES; use execute_sql_query::<E>()",
                ))
            }
        }
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_query::<E>(sql)?;

        self.execute_compiled_sql_owned::<E>(compiled)
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure the compile side of the new seam, including parse,
        // surface validation, and semantic command construction.
        let (compile_local_instructions, compiled) =
            measure_sql_stage(|| self.compile_sql_query_with_cache_attribution::<E>(sql));
        let (compiled, compile_cache_attribution, compile_phase_attribution) = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let store_get_calls_before = DataStore::current_get_call_count();
        let pure_covering_decode_before = current_pure_covering_decode_local_instructions();
        let pure_covering_row_assembly_before =
            current_pure_covering_row_assembly_local_instructions();
        let (result, execute_cache_attribution, execute_phase_attribution) =
            self.execute_compiled_sql_with_phase_attribution::<E>(&compiled)?;
        let store_get_calls =
            DataStore::current_get_call_count().saturating_sub(store_get_calls_before);
        let pure_covering_decode_local_instructions =
            current_pure_covering_decode_local_instructions()
                .saturating_sub(pure_covering_decode_before);
        let pure_covering_row_assembly_local_instructions =
            current_pure_covering_row_assembly_local_instructions()
                .saturating_sub(pure_covering_row_assembly_before);
        let execute_local_instructions = execute_phase_attribution
            .planner_local_instructions
            .saturating_add(execute_phase_attribution.store_local_instructions)
            .saturating_add(execute_phase_attribution.executor_local_instructions)
            .saturating_add(execute_phase_attribution.response_finalization_local_instructions);
        let cache_attribution = compile_cache_attribution.merge(execute_cache_attribution);
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);
        let grouped = matches!(&result, SqlStatementResult::Grouped { .. }).then_some(
            GroupedExecutionAttribution {
                stream_local_instructions: execute_phase_attribution
                    .grouped_stream_local_instructions,
                fold_local_instructions: execute_phase_attribution.grouped_fold_local_instructions,
                finalize_local_instructions: execute_phase_attribution
                    .grouped_finalize_local_instructions,
                count: GroupedCountAttribution::from_executor(
                    execute_phase_attribution.grouped_count,
                ),
            },
        );
        let pure_covering = (pure_covering_decode_local_instructions > 0
            || pure_covering_row_assembly_local_instructions > 0)
            .then_some(SqlPureCoveringAttribution {
                decode_local_instructions: pure_covering_decode_local_instructions,
                row_assembly_local_instructions: pure_covering_row_assembly_local_instructions,
            });

        Ok((
            result,
            SqlQueryExecutionAttribution {
                compile_local_instructions,
                compile: SqlCompileAttribution {
                    cache_key_local_instructions: compile_phase_attribution.cache_key,
                    cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                    parse_local_instructions: compile_phase_attribution.parse,
                    parse_tokenize_local_instructions: compile_phase_attribution.parse_tokenize,
                    parse_select_local_instructions: compile_phase_attribution.parse_select,
                    parse_expr_local_instructions: compile_phase_attribution.parse_expr,
                    parse_predicate_local_instructions: compile_phase_attribution.parse_predicate,
                    aggregate_lane_check_local_instructions: compile_phase_attribution
                        .aggregate_lane_check,
                    prepare_local_instructions: compile_phase_attribution.prepare,
                    lower_local_instructions: compile_phase_attribution.lower,
                    bind_local_instructions: compile_phase_attribution.bind,
                    cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                },
                plan_lookup_local_instructions: execute_phase_attribution
                    .planner_local_instructions,
                execution: SqlExecutionAttribution {
                    planner_local_instructions: execute_phase_attribution
                        .planner_local_instructions,
                    store_local_instructions: execute_phase_attribution.store_local_instructions,
                    executor_invocation_local_instructions: execute_phase_attribution
                        .executor_invocation_local_instructions,
                    executor_local_instructions: execute_phase_attribution
                        .executor_local_instructions,
                    response_finalization_local_instructions: execute_phase_attribution
                        .response_finalization_local_instructions,
                },
                grouped,
                scalar_aggregate: SqlScalarAggregateAttribution::from_executor(
                    execute_phase_attribution.scalar_aggregate_terminal,
                ),
                pure_covering,
                store_get_calls,
                response_decode_local_instructions: 0,
                execute_local_instructions,
                total_local_instructions,
                cache: SqlQueryCacheAttribution {
                    sql_compiled_command_hits: cache_attribution.sql_compiled_command_cache_hits,
                    sql_compiled_command_misses: cache_attribution
                        .sql_compiled_command_cache_misses,
                    shared_query_plan_hits: cache_attribution.shared_query_plan_cache_hits,
                    shared_query_plan_misses: cache_attribution.shared_query_plan_cache_misses,
                },
            },
        ))
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_update::<E>(sql)?;

        self.execute_compiled_sql_owned::<E>(compiled)
    }

    /// Prepare one SQL DDL statement against the accepted schema catalog.
    ///
    /// This is a non-executing surface: it proves the statement can bind,
    /// derive an accepted-after snapshot, and pass schema mutation admission,
    /// then returns a prepared-only report without mutating schema or index
    /// storage.
    pub fn prepare_sql_ddl<E>(&self, sql: &str) -> Result<SqlDdlPreparationReport, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (_, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;

        Ok(prepared.report().clone())
    }

    fn prepare_sql_ddl_command<E>(
        &self,
        sql: &str,
    ) -> Result<(AcceptedSchemaSnapshot, PreparedSqlDdlCommand), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (statement, _) =
            parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;
        let (accepted_schema, _) = self
            .accepted_entity_authority::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, &accepted_schema);
        let prepared = prepare_sql_ddl_statement(&statement, &accepted_schema, &schema_info)
            .map_err(|err| {
                QueryError::unsupported_query(format!(
                    "SQL DDL preparation failed before execution: {err}"
                ))
            })?;

        Ok((accepted_schema, prepared))
    }

    /// Execute one SQL DDL statement.
    ///
    /// The 0.155 execution boundary routes the single supported DDL shape
    /// through schema-owned physical rebuild and accepted-snapshot publication.
    pub fn execute_sql_ddl<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (accepted_before, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;
        let store = self
            .db
            .recovered_store(E::Store::PATH)
            .map_err(QueryError::execute)?;

        execute_sql_ddl_field_path_index_addition(
            store,
            E::ENTITY_TAG,
            E::PATH,
            &accepted_before,
            prepared.derivation(),
        )
        .map_err(QueryError::execute)?;

        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published),
        ))
    }
}
