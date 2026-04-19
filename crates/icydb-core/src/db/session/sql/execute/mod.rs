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

#[cfg(feature = "diagnostics")]
use crate::db::executor::pipeline::execute_initial_grouped_rows_for_canister_with_phase_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::physical_access::with_physical_access_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        session::sql::{
            CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandCacheKey,
            SqlStatementResult,
            projection::{SqlProjectionPayload, execute_sql_projection_rows_for_canister},
        },
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(feature = "diagnostics")]
#[expect(
    clippy::missing_const_for_fn,
    reason = "the wasm32 branch reads the runtime performance counter and cannot be const"
)]
fn read_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "diagnostics")]
fn measure_execute_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "diagnostics")]
fn measure_execute_phase_with_physical_access<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> ((u64, u64), Result<T, E>) {
    let (store_local_instructions, (execute_local_instructions, result)) =
        with_physical_access_attribution(|| measure_execute_phase(run));

    (
        (execute_local_instructions, store_local_instructions),
        result,
    )
}

impl<C: CanisterKind> DbSession<C> {
    // Execute one SQL projection from one shared lower prepared plan plus
    // one thin SQL projection contract so cached and explicit-bypass paths
    // share the same final row-materialization shell.
    fn execute_structural_sql_projection_from_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: &SqlCompiledCommandCacheKey,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (prepared_plan, projection, cache_attribution) = self.sql_select_prepared_plan(
            query,
            authority,
            compiled_cache_key.schema_fingerprint(),
        )?;

        self.execute_structural_sql_projection_from_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    // Execute one structural SQL load query through only the shared lower
    // query-plan cache for lowered or aggregate-only bypass paths.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection_without_sql_cache(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let cache_schema_fingerprint = crate::db::schema::commit_schema_fingerprint_for_model(
            authority.model().path,
            authority.model(),
        );
        let (prepared_plan, projection, cache_attribution) =
            self.sql_select_prepared_plan(&query, authority, cache_schema_fingerprint)?;

        self.execute_structural_sql_projection_from_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_compiled_sql<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) = self.execute_compiled_sql_with_cache_attribution::<E>(compiled)?;

        Ok(result)
    }

    // Keep one perf-only execution entrypoint that returns cache attribution
    // together with planner/runtime instruction splits for shell-facing tools.
    #[cfg(feature = "diagnostics")]
    fn execute_non_select_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if matches!(compiled, CompiledSqlCommand::Select { .. }) {
            return Err(QueryError::execute(
                InternalError::query_executor_invariant(
                    "non-select SQL phase attribution helper received SELECT",
                ),
            ));
        }

        let ((execute_local_instructions, store_local_instructions), result) =
            measure_execute_phase_with_physical_access(|| {
                self.execute_compiled_sql_with_cache_attribution::<E>(compiled)
            });
        let (result, cache_attribution) = result?;

        Ok((
            result,
            cache_attribution,
            SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                execute_local_instructions,
                store_local_instructions,
            ),
        ))
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
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
                    let (planner_local_instructions, resolved_query_plan) =
                        measure_execute_phase(|| {
                            self.sql_select_prepared_plan(
                                query,
                                authority,
                                compiled_cache_key.schema_fingerprint(),
                            )
                        });
                    let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                    let ((execute_local_instructions, store_local_instructions), statement_result) =
                        measure_execute_phase_with_physical_access(move || {
                            self.execute_grouped_sql_statement_from_prepared_plan_with(
                                prepared_plan,
                                projection,
                                authority,
                                |session, authority, plan| {
                                    execute_initial_grouped_rows_for_canister_with_phase_attribution(
                                        &session.db,
                                        session.debug,
                                        authority,
                                        plan,
                                    )
                                    .map_err(QueryError::execute)
                                },
                            )
                        });
                    let (statement_result, grouped_phase_attribution) = statement_result?;

                    return Ok((
                        statement_result,
                        cache_attribution,
                        SqlExecutePhaseAttribution {
                            planner_local_instructions,
                            store_local_instructions,
                            executor_local_instructions: execute_local_instructions
                                .saturating_sub(store_local_instructions),
                            grouped_stream_local_instructions: grouped_phase_attribution
                                .stream_local_instructions,
                            grouped_fold_local_instructions: grouped_phase_attribution
                                .fold_local_instructions,
                            grouped_finalize_local_instructions: grouped_phase_attribution
                                .finalize_local_instructions,
                            grouped_count: grouped_phase_attribution.grouped_count,
                        },
                    ));
                }

                let (planner_local_instructions, resolved_query_plan) =
                    measure_execute_phase(|| {
                        self.sql_select_prepared_plan(
                            query,
                            authority,
                            compiled_cache_key.schema_fingerprint(),
                        )
                    });
                let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                let ((execute_local_instructions, store_local_instructions), payload) =
                    measure_execute_phase_with_physical_access(move || {
                        self.execute_structural_sql_projection_from_prepared_plan(
                            prepared_plan,
                            projection,
                            SqlCacheAttribution::default(),
                        )
                        .map(|(payload, _)| payload)
                    });
                let payload = payload?;

                Ok((
                    payload.into_statement_result(),
                    cache_attribution,
                    SqlExecutePhaseAttribution {
                        planner_local_instructions,
                        store_local_instructions,
                        executor_local_instructions: execute_local_instructions
                            .saturating_sub(store_local_instructions),
                        grouped_stream_local_instructions: 0,
                        grouped_fold_local_instructions: 0,
                        grouped_finalize_local_instructions: 0,
                        grouped_count: crate::db::executor::GroupedCountAttribution::none(),
                    },
                ))
            }
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::GlobalAggregate { .. }
            | CompiledSqlCommand::Explain(..)
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..)
            | CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities => {
                self.execute_non_select_compiled_sql_with_phase_attribution::<E>(compiled)
            }
        }
    }

    pub(in crate::db) fn execute_compiled_sql_with_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
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
                        query,
                        authority,
                        compiled_cache_key,
                    );
                }

                let (payload, cache_attribution) =
                    self.execute_structural_sql_projection(query, authority, compiled_cache_key)?;

                Ok((payload.into_statement_result(), cache_attribution))
            }
            CompiledSqlCommand::Delete { query, statement } => self
                .execute_sql_delete_statement::<E>(query.clone(), statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::GlobalAggregate { command } => self
                .execute_global_aggregate_statement_for_authority::<E>(*command.clone(), authority),
            CompiledSqlCommand::Explain(lowered) => {
                if let Some(explain) =
                    self.explain_lowered_sql_execution_for_authority(lowered, authority)?
                {
                    return Ok((
                        SqlStatementResult::Explain(explain),
                        SqlCacheAttribution::default(),
                    ));
                }

                self.explain_lowered_sql_for_authority(lowered, authority)
                    .map(SqlStatementResult::Explain)
                    .map(|result| (result, SqlCacheAttribution::default()))
            }
            CompiledSqlCommand::Insert(statement) => self
                .execute_sql_insert_statement::<E>(statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::Update(statement) => self
                .execute_sql_update_statement::<E>(statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::DescribeEntity => Ok((
                SqlStatementResult::Describe(self.describe_entity::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowIndexesEntity => Ok((
                SqlStatementResult::ShowIndexes(self.show_indexes::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowColumnsEntity => Ok((
                SqlStatementResult::ShowColumns(self.show_columns::<E>()),
                SqlCacheAttribution::default(),
            )),
            CompiledSqlCommand::ShowEntities => Ok((
                SqlStatementResult::ShowEntities(self.show_entities()),
                SqlCacheAttribution::default(),
            )),
        }
    }

    /// Compile and then execute one parsed reduced SQL statement into one
    /// unified SQL payload for session-owned tests.
    #[cfg(test)]
    pub(in crate::db) fn execute_sql_statement_inner<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = crate::db::session::sql::parse_sql_statement(sql)?;
        let (compiled, _, _) = match statement {
            crate::db::sql::parser::SqlStatement::Insert(_)
            | crate::db::sql::parser::SqlStatement::Update(_)
            | crate::db::sql::parser::SqlStatement::Delete(_) => {
                self.compile_sql_update_with_cache_attribution::<E>(sql)?
            }
            crate::db::sql::parser::SqlStatement::Select(_)
            | crate::db::sql::parser::SqlStatement::Explain(_)
            | crate::db::sql::parser::SqlStatement::Describe(_)
            | crate::db::sql::parser::SqlStatement::ShowIndexes(_)
            | crate::db::sql::parser::SqlStatement::ShowColumns(_)
            | crate::db::sql::parser::SqlStatement::ShowEntities(_) => {
                self.compile_sql_query_with_cache_attribution::<E>(sql)?
            }
        };

        self.execute_compiled_sql::<E>(&compiled)
    }
}
