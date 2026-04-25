//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod cache;
mod compiled;
mod execute;
mod projection;

#[cfg(feature = "diagnostics")]
use candid::CandidType;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;
use std::sync::Arc;

#[cfg(feature = "diagnostics")]
use crate::db::DataStore;
#[cfg(feature = "diagnostics")]
use crate::db::executor::GroupedCountAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::projection::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
use crate::{
    db::{
        DbSession, GroupedRow, MissingRowPolicy, PersistedRow, QueryError,
        commit::CommitSchemaFingerprint,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        session::sql::projection::{
            projection_fixed_scales_from_projection_spec, projection_labels_from_projection_spec,
        },
        sql::lowering::{
            bind_lowered_sql_delete_query_structural, bind_lowered_sql_select_query_structural,
            compile_sql_global_aggregate_command_core_from_prepared,
            extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
            lower_prepared_sql_delete_statement, lower_prepared_sql_select_statement,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::{SqlStatement, parse_sql_with_attribution},
    },
    traits::{CanisterKind, EntityValue},
    value::OutputValue,
};

pub(in crate::db::session::sql) use cache::SqlCompiledCommandSurface;
pub(in crate::db) use cache::{SqlCacheAttribution, SqlCompiledCommandCacheKey};
pub(in crate::db) use compiled::{CompiledSqlCommand, SqlProjectionContract};

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

/// Unified SQL statement payload returned by shared SQL lane execution.
#[derive(Debug)]
pub enum SqlStatementResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

///
/// SqlQueryExecutionAttribution
///
/// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
/// split at the new compile/execute seam.
/// This keeps future cache validation focused on one concrete question:
/// whether repeated queries stop paying compile cost while execute cost stays
/// otherwise comparable.
///

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile_cache_key_local_instructions: u64,
    pub compile_cache_lookup_local_instructions: u64,
    pub compile_parse_local_instructions: u64,
    pub compile_parse_tokenize_local_instructions: u64,
    pub compile_parse_select_local_instructions: u64,
    pub compile_parse_expr_local_instructions: u64,
    pub compile_parse_predicate_local_instructions: u64,
    pub compile_aggregate_lane_check_local_instructions: u64,
    pub compile_prepare_local_instructions: u64,
    pub compile_lower_local_instructions: u64,
    pub compile_bind_local_instructions: u64,
    pub compile_cache_insert_local_instructions: u64,
    pub plan_lookup_local_instructions: u64,
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count_borrowed_hash_computations: u64,
    pub grouped_count_bucket_candidate_checks: u64,
    pub grouped_count_existing_group_hits: u64,
    pub grouped_count_new_group_inserts: u64,
    pub grouped_count_row_materialization_local_instructions: u64,
    pub grouped_count_group_lookup_local_instructions: u64,
    pub grouped_count_existing_group_update_local_instructions: u64,
    pub grouped_count_new_group_insert_local_instructions: u64,
    pub pure_covering_decode_local_instructions: u64,
    pub pure_covering_row_assembly_local_instructions: u64,
    pub store_get_calls: u64,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub sql_compiled_command_cache_hits: u64,
    pub sql_compiled_command_cache_misses: u64,
    pub shared_query_plan_cache_hits: u64,
    pub shared_query_plan_cache_misses: u64,
}

// SqlExecutePhaseAttribution keeps the execute side split into select-plan
// work, physical store/index access, and narrower runtime execution so shell
// tooling can show all three.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlExecutePhaseAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: GroupedCountAttribution,
}

///
/// SqlCompilePhaseAttribution
///
/// SqlCompilePhaseAttribution keeps the SQL-front-end compile miss path split
/// into the concrete stages that still exist after the shared lower-cache
/// collapse.
/// This lets perf audits distinguish cache lookup, parsing, prepared-statement
/// normalization, lowered-command construction, structural binding, and cache
/// insertion cost instead of treating compile as one opaque bucket.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SqlCompilePhaseAttribution {
    pub cache_key: u64,
    pub cache_lookup: u64,
    pub parse: u64,
    pub parse_tokenize: u64,
    pub parse_select: u64,
    pub parse_expr: u64,
    pub parse_predicate: u64,
    pub aggregate_lane_check: u64,
    pub prepare: u64,
    pub lower: u64,
    pub bind: u64,
    pub cache_insert: u64,
}

impl SqlCompilePhaseAttribution {
    #[must_use]
    const fn cache_hit(cache_key: u64, cache_lookup: u64) -> Self {
        Self {
            cache_key,
            cache_lookup,
            parse: 0,
            parse_tokenize: 0,
            parse_select: 0,
            parse_expr: 0,
            parse_predicate: 0,
            aggregate_lane_check: 0,
            prepare: 0,
            lower: 0,
            bind: 0,
            cache_insert: 0,
        }
    }
}

#[cfg(feature = "diagnostics")]
impl SqlExecutePhaseAttribution {
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
            store_local_instructions,
            executor_invocation_local_instructions: execute_local_instructions,
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions),
            response_finalization_local_instructions: 0,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: GroupedCountAttribution::none(),
        }
    }
}

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
#[cfg(test)]
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_sql_local_instruction_counter() -> u64 {
    #[cfg(all(feature = "diagnostics", target_arch = "wasm32"))]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(all(feature = "diagnostics", target_arch = "wasm32")))]
    {
        0
    }
}

pub(in crate::db::session::sql) fn measure_sql_stage<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    #[cfg(feature = "diagnostics")]
    let start = read_sql_local_instruction_counter();

    let result = run();

    #[cfg(feature = "diagnostics")]
    let delta = read_sql_local_instruction_counter().saturating_sub(start);

    #[cfg(not(feature = "diagnostics"))]
    let delta = 0;

    (delta, result)
}

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    #[expect(clippy::too_many_lines)]
    fn compile_sql_statement_for_authority(
        statement: &SqlStatement,
        authority: EntityAuthority,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    ) -> Result<(CompiledSqlCommand, u64, u64, u64, u64), QueryError> {
        // Reuse one local preparation closure so the session compile surface
        // reaches the prepared-statement owner directly without another
        // single-purpose module hop.
        let prepare_statement = || {
            measure_sql_stage(|| {
                prepare_sql_statement(statement.clone(), authority.model().name())
                    .map_err(QueryError::from_sql_lowering_error)
            })
        };

        match statement {
            SqlStatement::Select(_) => {
                let (prepare_local_instructions, prepared) = prepare_statement();
                let prepared = prepared?;
                let (aggregate_lane_check_local_instructions, requires_aggregate_lane) =
                    measure_sql_stage(|| {
                        Ok::<_, QueryError>(prepared.statement().is_global_aggregate_lane_shape())
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
                    let (lower_local_instructions, select) = measure_sql_stage(|| {
                        lower_prepared_sql_select_statement(prepared, authority.model())
                            .map_err(QueryError::from_sql_lowering_error)
                    });
                    let select = select?;
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
                        CompiledSqlCommand::Select {
                            query: Arc::new(query),
                            compiled_cache_key,
                        },
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
                let (lower_local_instructions, lowered) = measure_sql_stage(|| {
                    lower_prepared_sql_delete_statement(prepared)
                        .map_err(QueryError::from_sql_lowering_error)
                });
                let delete = lowered?;
                let returning = delete.returning().cloned();
                let query = delete.into_base_query();
                let (bind_local_instructions, query) = measure_sql_stage(|| {
                    Ok::<_, QueryError>(bind_lowered_sql_delete_query_structural(
                        authority.model(),
                        query,
                        MissingRowPolicy::Ignore,
                    ))
                });
                let query = query?;

                Ok((
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
            SqlStatement::Insert(_) => {
                let (prepare_local_instructions, prepared) = prepare_statement();
                let prepared = prepared?;
                let statement = extract_prepared_sql_insert_statement(prepared)
                    .map_err(QueryError::from_sql_lowering_error)?;

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
                let statement = extract_prepared_sql_update_statement(prepared)
                    .map_err(QueryError::from_sql_lowering_error)?;

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
                    CompiledSqlCommand::Explain(Box::new(lowered)),
                    0,
                    prepare_local_instructions,
                    lower_local_instructions,
                    0,
                ))
            }
            SqlStatement::Describe(_) => {
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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
                let (prepare_local_instructions, prepared) = prepare_statement();
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

    // Resolve one SQL SELECT entirely through the shared lower query-plan
    // cache and derive only the outward SQL projection contract locally.
    fn sql_select_prepared_plan(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        cache_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self.cached_shared_query_plan_for_authority(
            authority,
            cache_schema_fingerprint,
            query,
        )?;
        let projection_spec = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let projection = SqlProjectionContract::new(
            projection_labels_from_projection_spec(&projection_spec),
            projection_fixed_scales_from_projection_spec(&projection_spec),
        );

        Ok((
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
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

        self.execute_compiled_sql::<E>(&compiled)
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

        Ok((
            result,
            SqlQueryExecutionAttribution {
                compile_local_instructions,
                compile_cache_key_local_instructions: compile_phase_attribution.cache_key,
                compile_cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                compile_parse_local_instructions: compile_phase_attribution.parse,
                compile_parse_tokenize_local_instructions: compile_phase_attribution.parse_tokenize,
                compile_parse_select_local_instructions: compile_phase_attribution.parse_select,
                compile_parse_expr_local_instructions: compile_phase_attribution.parse_expr,
                compile_parse_predicate_local_instructions: compile_phase_attribution
                    .parse_predicate,
                compile_aggregate_lane_check_local_instructions: compile_phase_attribution
                    .aggregate_lane_check,
                compile_prepare_local_instructions: compile_phase_attribution.prepare,
                compile_lower_local_instructions: compile_phase_attribution.lower,
                compile_bind_local_instructions: compile_phase_attribution.bind,
                compile_cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                plan_lookup_local_instructions: execute_phase_attribution
                    .planner_local_instructions,
                planner_local_instructions: execute_phase_attribution.planner_local_instructions,
                store_local_instructions: execute_phase_attribution.store_local_instructions,
                executor_invocation_local_instructions: execute_phase_attribution
                    .executor_invocation_local_instructions,
                executor_local_instructions: execute_phase_attribution.executor_local_instructions,
                response_finalization_local_instructions: execute_phase_attribution
                    .response_finalization_local_instructions,
                grouped_stream_local_instructions: execute_phase_attribution
                    .grouped_stream_local_instructions,
                grouped_fold_local_instructions: execute_phase_attribution
                    .grouped_fold_local_instructions,
                grouped_finalize_local_instructions: execute_phase_attribution
                    .grouped_finalize_local_instructions,
                grouped_count_borrowed_hash_computations: execute_phase_attribution
                    .grouped_count
                    .borrowed_hash_computations,
                grouped_count_bucket_candidate_checks: execute_phase_attribution
                    .grouped_count
                    .bucket_candidate_checks,
                grouped_count_existing_group_hits: execute_phase_attribution
                    .grouped_count
                    .existing_group_hits,
                grouped_count_new_group_inserts: execute_phase_attribution
                    .grouped_count
                    .new_group_inserts,
                grouped_count_row_materialization_local_instructions: execute_phase_attribution
                    .grouped_count
                    .row_materialization_local_instructions,
                grouped_count_group_lookup_local_instructions: execute_phase_attribution
                    .grouped_count
                    .group_lookup_local_instructions,
                grouped_count_existing_group_update_local_instructions: execute_phase_attribution
                    .grouped_count
                    .existing_group_update_local_instructions,
                grouped_count_new_group_insert_local_instructions: execute_phase_attribution
                    .grouped_count
                    .new_group_insert_local_instructions,
                pure_covering_decode_local_instructions,
                pure_covering_row_assembly_local_instructions,
                store_get_calls,
                response_decode_local_instructions: 0,
                execute_local_instructions,
                total_local_instructions,
                sql_compiled_command_cache_hits: cache_attribution.sql_compiled_command_cache_hits,
                sql_compiled_command_cache_misses: cache_attribution
                    .sql_compiled_command_cache_misses,
                shared_query_plan_cache_hits: cache_attribution.shared_query_plan_cache_hits,
                shared_query_plan_cache_misses: cache_attribution.shared_query_plan_cache_misses,
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

        self.execute_compiled_sql::<E>(&compiled)
    }

    // Compile one SQL query-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_query_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_query_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_surface_with_cache_attribution::<E>(sql, SqlCompiledCommandSurface::Query)
    }

    // Compile one SQL update-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_update<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_update_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_update_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_surface_with_cache_attribution::<E>(sql, SqlCompiledCommandSurface::Update)
    }

    // Reuse one internal compile shell for both outward SQL surfaces so query
    // and update no longer duplicate cache-key construction and surface
    // validation plumbing before they reach the real compile/cache owner.
    fn compile_sql_surface_with_cache_attribution<E>(
        &self,
        sql: &str,
        surface: SqlCompiledCommandSurface,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_key_local_instructions, cache_key) = measure_sql_stage(|| {
            Ok::<_, QueryError>(SqlCompiledCommandCacheKey::for_entity::<E>(surface, sql))
        });
        let cache_key = cache_key?;

        self.compile_sql_statement_with_cache::<E, _>(
            cache_key,
            cache_key_local_instructions,
            sql,
            |statement| Self::ensure_sql_statement_supported_for_surface(statement, surface),
        )
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache<E, F>(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        cache_key_local_instructions: u64,
        sql: &str,
        ensure_surface_supported: F,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
        F: FnOnce(&SqlStatement) -> Result<(), QueryError>,
    {
        let (cache_lookup_local_instructions, cached) = measure_sql_stage(|| {
            let cached =
                self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
            Ok::<_, QueryError>(cached)
        });
        let cached = cached?;
        if let Some(compiled) = cached {
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                SqlCompilePhaseAttribution::cache_hit(
                    cache_key_local_instructions,
                    cache_lookup_local_instructions,
                ),
            ));
        }

        let (parse_local_instructions, parsed) = measure_sql_stage(|| {
            parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)
        });
        let (parsed, parse_attribution) = parsed?;
        let parse_select_local_instructions = parse_local_instructions
            .saturating_sub(parse_attribution.tokenize)
            .saturating_sub(parse_attribution.expr)
            .saturating_sub(parse_attribution.predicate);
        ensure_surface_supported(&parsed)?;
        let authority = EntityAuthority::for_type::<E>();
        let (
            compiled,
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ) = Self::compile_sql_statement_for_authority(&parsed, authority, cache_key.clone())?;

        let (cache_insert_local_instructions, cache_insert) = measure_sql_stage(|| {
            self.with_sql_compiled_command_cache(|cache| {
                cache.insert(cache_key, compiled.clone());
            });
            Ok::<_, QueryError>(())
        });
        cache_insert?;

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            SqlCompilePhaseAttribution {
                cache_key: cache_key_local_instructions,
                cache_lookup: cache_lookup_local_instructions,
                parse: parse_local_instructions,
                parse_tokenize: parse_attribution.tokenize,
                parse_select: parse_select_local_instructions,
                parse_expr: parse_attribution.expr,
                parse_predicate: parse_attribution.predicate,
                aggregate_lane_check: aggregate_lane_check_local_instructions,
                prepare: prepare_local_instructions,
                lower: lower_local_instructions,
                bind: bind_local_instructions,
                cache_insert: cache_insert_local_instructions,
            },
        ))
    }
}
