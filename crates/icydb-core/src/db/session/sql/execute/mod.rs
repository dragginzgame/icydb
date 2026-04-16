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

#[cfg(feature = "perf-attribution")]
use crate::db::executor::pipeline::execute_initial_grouped_rows_for_canister;
#[cfg(feature = "perf-attribution")]
use crate::db::physical_access::with_physical_access_attribution;
#[cfg(feature = "perf-attribution")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::EntityAuthority,
        query::intent::StructuralQuery,
        session::sql::{
            CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandCacheKey,
            SqlStatementResult,
            projection::{SqlProjectionPayload, execute_sql_projection_rows_for_canister},
        },
    },
    traits::{CanisterKind, EntityValue},
};

type PreparedStructuralSqlProjectionExecution = (
    Vec<String>,
    Vec<Option<u32>>,
    crate::db::executor::SharedPreparedExecutionPlan,
    SqlCacheAttribution,
);

#[cfg(feature = "perf-attribution")]
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

#[cfg(feature = "perf-attribution")]
fn measure_execute_phase<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "perf-attribution")]
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
    // Build the shared structural SQL projection execution inputs once so
    // value-row and rendered-row statement surfaces only differ in final packaging.
    // This keeps the SQL select cache aligned with the shared prepared-plan
    // boundary instead of discarding the frozen scalar projection resident.
    fn prepare_structural_sql_projection_execution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<PreparedStructuralSqlProjectionExecution, QueryError> {
        // Phase 1: build the structural access plan once and freeze its outward
        // column contract for all projection materialization surfaces.
        let (entry, cache_attribution) =
            self.planned_sql_select_with_visibility(&query, authority, compiled_cache_key)?;
        let (prepared_plan, columns, fixed_scales) = entry.into_parts();

        Ok((columns, fixed_scales, prepared_plan, cache_attribution))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        // Phase 1: build the shared structural plan and outward column contract once.
        let (columns, fixed_scales, prepared_plan, cache_attribution) =
            self.prepare_structural_sql_projection_execution(query, authority, compiled_cache_key)?;

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics.
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
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

    // Split scalar SELECT execution into plan construction and runtime work so
    // perf tooling can show the planner cost separately from row execution.
    #[cfg(feature = "perf-attribution")]
    fn execute_structural_sql_projection_with_phase_attribution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<
        (
            SqlProjectionPayload,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        let (planner_local_instructions, prepared) = measure_execute_phase(|| {
            self.prepare_structural_sql_projection_execution(query, authority, compiled_cache_key)
        });
        let (columns, fixed_scales, prepared_plan, cache_attribution) = prepared?;

        let ((execute_local_instructions, store_local_instructions), payload) =
            measure_execute_phase_with_physical_access(move || {
                let projected =
                    execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                        .map_err(QueryError::execute)?;
                let (rows, row_count) = projected.into_parts();

                Ok::<SqlProjectionPayload, QueryError>(SqlProjectionPayload::new(
                    columns,
                    fixed_scales,
                    rows,
                    row_count,
                ))
            });
        let payload = payload?;

        Ok((
            payload,
            cache_attribution,
            SqlExecutePhaseAttribution {
                planner_local_instructions,
                store_local_instructions,
                executor_local_instructions: execute_local_instructions
                    .saturating_sub(store_local_instructions),
            },
        ))
    }

    // Split grouped SELECT execution at the same session boundary: first plan
    // selection/cache resolution, then grouped runtime plus result packaging.
    #[cfg(feature = "perf-attribution")]
    fn execute_structural_sql_grouped_statement_select_with_phase_attribution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: Option<&SqlCompiledCommandCacheKey>,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        let (planner_local_instructions, prepared) = measure_execute_phase(|| {
            self.planned_sql_select_with_visibility(&query, authority, compiled_cache_key)
        });
        let (entry, cache_attribution) = prepared?;
        let (prepared_plan, columns, _) = entry.into_parts();
        let plan = prepared_plan.logical_plan().clone();

        let ((execute_local_instructions, store_local_instructions), statement_result) =
            measure_execute_phase_with_physical_access(move || {
                let page = execute_initial_grouped_rows_for_canister(
                    &self.db, self.debug, authority, plan,
                )
                .map_err(QueryError::execute)?;
                let next_cursor = page
                    .next_cursor
                    .map(|cursor| {
                        let Some(token) = cursor.as_grouped() else {
                            return Err(QueryError::grouped_paged_emitted_scalar_continuation());
                        };

                        token.encode_hex().map_err(|err| {
                            QueryError::serialize_internal(format!(
                                "failed to serialize grouped continuation cursor: {err}"
                            ))
                        })
                    })
                    .transpose()?;

                Ok::<SqlStatementResult, QueryError>(
                    crate::db::session::sql::projection::grouped_sql_statement_result(
                        columns,
                        page.rows,
                        next_cursor,
                    ),
                )
            });
        let statement_result = statement_result?;

        Ok((
            statement_result,
            cache_attribution,
            SqlExecutePhaseAttribution {
                planner_local_instructions,
                store_local_instructions,
                executor_local_instructions: execute_local_instructions
                    .saturating_sub(store_local_instructions),
            },
        ))
    }

    // Keep one perf-only execution entrypoint that returns cache attribution
    // together with planner/runtime instruction splits for shell-facing tools.
    #[cfg(feature = "perf-attribution")]
    #[expect(
        clippy::too_many_lines,
        reason = "the compiled SQL execution matrix keeps every statement family on one explicit perf-attributed seam"
    )]
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
                    return self
                        .execute_structural_sql_grouped_statement_select_with_phase_attribution(
                            query.clone(),
                            authority,
                            compiled_cache_key.as_ref(),
                        );
                }

                let (payload, cache_attribution, phase_attribution) = self
                    .execute_structural_sql_projection_with_phase_attribution(
                        query.clone(),
                        authority,
                        compiled_cache_key.as_ref(),
                    )?;

                Ok((
                    payload.into_statement_result(),
                    cache_attribution,
                    phase_attribution,
                ))
            }
            CompiledSqlCommand::Delete { query, statement } => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_sql_delete_statement::<E>(query.clone(), statement)
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::GlobalAggregate {
                command,
                label_overrides,
            } => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_global_aggregate_statement_for_authority::<E>(
                            command.clone(),
                            authority,
                            label_overrides.clone(),
                        )
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
            CompiledSqlCommand::Explain(lowered) => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        if let Some(explain) =
                            self.explain_lowered_sql_execution_for_authority(lowered, authority)?
                        {
                            return Ok::<SqlStatementResult, QueryError>(
                                SqlStatementResult::Explain(explain),
                            );
                        }

                        self.explain_lowered_sql_for_authority(lowered, authority)
                            .map(SqlStatementResult::Explain)
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::Insert(statement) => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_sql_insert_statement::<E>(statement)
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::Update(statement) => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_sql_update_statement::<E>(statement)
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::DescribeEntity => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        Ok::<SqlStatementResult, QueryError>(SqlStatementResult::Describe(
                            self.describe_entity::<E>(),
                        ))
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::ShowIndexesEntity => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        Ok::<SqlStatementResult, QueryError>(SqlStatementResult::ShowIndexes(
                            self.show_indexes::<E>(),
                        ))
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::ShowColumnsEntity => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        Ok::<SqlStatementResult, QueryError>(SqlStatementResult::ShowColumns(
                            self.show_columns::<E>(),
                        ))
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
            }
            CompiledSqlCommand::ShowEntities => {
                let ((execute_local_instructions, store_local_instructions), result) =
                    measure_execute_phase_with_physical_access(|| {
                        Ok::<SqlStatementResult, QueryError>(SqlStatementResult::ShowEntities(
                            self.show_entities(),
                        ))
                    });
                let result = result?;

                Ok((
                    result,
                    SqlCacheAttribution::default(),
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    ),
                ))
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
                        query.clone(),
                        authority,
                        compiled_cache_key.as_ref(),
                    );
                }

                let (payload, cache_attribution) = self.execute_structural_sql_projection(
                    query.clone(),
                    authority,
                    compiled_cache_key.as_ref(),
                )?;

                Ok((payload.into_statement_result(), cache_attribution))
            }
            CompiledSqlCommand::Delete { query, statement } => self
                .execute_sql_delete_statement::<E>(query.clone(), statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::GlobalAggregate {
                command,
                label_overrides,
            } => self.execute_global_aggregate_statement_for_authority::<E>(
                command.clone(),
                authority,
                label_overrides.clone(),
            ),
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
        sql_statement: &crate::db::sql::parser::SqlStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = Self::compile_sql_statement_inner::<E>(sql_statement)?;

        self.execute_compiled_sql::<E>(&compiled)
    }
}
