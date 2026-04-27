//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while keeping
//! only route and write wiring in child modules.

mod explain;
mod global_aggregate;
mod write;
mod write_returning;

#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    pipeline::execute_initial_grouped_rows_for_canister_with_phase_attribution,
    with_scalar_aggregate_terminal_attribution,
};
#[cfg(feature = "diagnostics")]
use crate::db::physical_access::with_physical_access_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::{SqlExecutePhaseAttribution, measure_sql_stage};
#[cfg(feature = "diagnostics")]
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan, StructuralGroupedProjectionResult,
        },
        query::intent::StructuralQuery,
        session::{
            finalize_structural_grouped_projection_result,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandCacheKey,
                SqlStatementResult,
                projection::{SqlProjectionPayload, execute_sql_projection_rows_for_canister},
            },
            sql_grouped_cursor_from_bytes,
        },
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(feature = "diagnostics")]
fn measure_execute_phase_with_physical_access<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> ((u64, u64), Result<T, E>) {
    let (store_local_instructions, (execute_local_instructions, result)) =
        with_physical_access_attribution(|| measure_sql_stage(run));

    (
        (execute_local_instructions, store_local_instructions),
        result,
    )
}

///
/// GroupedSqlDiagnosticsCollector
///
/// GroupedSqlDiagnosticsCollector carries the diagnostics-only response
/// finalization counter through the shared grouped SQL execution core.
/// Normal execution passes no collector, so the response path remains the
/// direct statement-result finalizer used outside diagnostics builds.
///

struct GroupedSqlDiagnosticsCollector<'a> {
    #[cfg(feature = "diagnostics")]
    response_finalization_local_instructions: &'a mut u64,
    #[cfg(not(feature = "diagnostics"))]
    _marker: std::marker::PhantomData<&'a mut u64>,
}

impl<'a> GroupedSqlDiagnosticsCollector<'a> {
    // Build one diagnostics collector over the caller-owned response counter.
    #[cfg(feature = "diagnostics")]
    const fn new(response_finalization_local_instructions: &'a mut u64) -> Self<'a> {
        Self {
            response_finalization_local_instructions,
        }
    }

    // Finalize a grouped SQL result while recording diagnostics-only response
    // attribution when diagnostics are enabled.
    fn finalize_grouped_sql_statement<C: CanisterKind>(
        self,
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        result: StructuralGroupedProjectionResult,
    ) -> Result<SqlStatementResult, QueryError> {
        #[cfg(feature = "diagnostics")]
        {
            let (response_finalization_local_instructions, statement_result) =
                measure_sql_stage(|| {
                    DbSession::<C>::grouped_sql_statement_result_from_result(
                        columns,
                        fixed_scales,
                        result,
                    )
                });
            *self.response_finalization_local_instructions =
                response_finalization_local_instructions;

            statement_result
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            let _ = self;
            DbSession::<C>::grouped_sql_statement_result_from_result(columns, fixed_scales, result)
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Convert one grouped executor result plus SQL projection labels into the
    // statement result shape shared by normal and diagnostics SQL execution.
    fn grouped_sql_statement_result_from_result(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        result: StructuralGroupedProjectionResult,
    ) -> Result<SqlStatementResult, QueryError> {
        let row_count = result.row_count();
        let grouped = finalize_structural_grouped_projection_result(result, None)?;
        let (rows, continuation_cursor, _) = grouped.into_parts();
        let next_cursor = sql_grouped_cursor_from_bytes(continuation_cursor);

        Ok(SqlStatementResult::Grouped {
            columns,
            fixed_scales,
            rows,
            row_count,
            next_cursor,
        })
    }

    // Execute one SQL projection from one shared lower prepared plan plus
    // one thin SQL projection contract so cached and explicit-bypass paths
    // share the same final row-materialization shell.
    fn execute_sql_projection_from_structural_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let (rows, row_count) =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                .map_err(QueryError::execute)?;

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
    }

    // Execute one grouped SQL statement from one shared lowered prepared plan
    // plus one thin SQL projection contract. Normal and diagnostics surfaces
    // share this plan-to-statement shell; diagnostics only swaps response
    // finalization through the optional collector.
    fn execute_grouped_sql_core<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        authority: EntityAuthority,
        diagnostics: Option<GroupedSqlDiagnosticsCollector<'_>>,
        execute_grouped: impl FnOnce(
            &Self,
            EntityAuthority,
            crate::db::query::plan::AccessPlannedQuery,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let plan = prepared_plan.logical_plan().clone();
        let (result, extra) = execute_grouped(self, authority, plan)?;
        let statement_result = if let Some(diagnostics) = diagnostics {
            diagnostics.finalize_grouped_sql_statement::<C>(columns, fixed_scales, result)?
        } else {
            Self::grouped_sql_statement_result_from_result(columns, fixed_scales, result)?
        };

        Ok((statement_result, extra))
    }

    // Execute one grouped SQL statement through the shared grouped SQL core
    // without diagnostics response attribution.
    fn execute_grouped_sql_statement_from_prepared_plan<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        authority: EntityAuthority,
        execute_grouped: impl FnOnce(
            &Self,
            EntityAuthority,
            crate::db::query::plan::AccessPlannedQuery,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        self.execute_grouped_sql_core(prepared_plan, projection, authority, None, execute_grouped)
    }

    // Diagnostics-only grouped SQL execution split that keeps runtime
    // invocation and session response-envelope finalization in separate
    // counters while sharing the same grouped SQL core as normal execution.
    #[cfg(feature = "diagnostics")]
    fn execute_grouped_sql_statement_with_response_attribution<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        authority: EntityAuthority,
        execute_grouped: impl FnOnce(
            &Self,
            EntityAuthority,
            crate::db::query::plan::AccessPlannedQuery,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T, u64), QueryError> {
        let mut response_finalization_local_instructions = 0;
        let diagnostics =
            GroupedSqlDiagnosticsCollector::new(&mut response_finalization_local_instructions);
        let (statement_result, extra) = self.execute_grouped_sql_core(
            prepared_plan,
            projection,
            authority,
            Some(diagnostics),
            execute_grouped,
        )?;

        Ok((
            statement_result,
            extra,
            response_finalization_local_instructions,
        ))
    }

    // Execute one SQL load query from a structural lowered query through only the shared lower
    // query-plan cache for lowered or aggregate-only bypass paths.
    pub(in crate::db::session::sql) fn execute_sql_projection_from_structural_query_without_sql_cache(
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

        self.execute_sql_projection_from_structural_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    #[cfg(test)]
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

    /// Execute one owned compiled reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_compiled_sql_owned<E>(
        &self,
        compiled: CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) = self.execute_compiled_sql_owned_with_cache_attribution::<E>(compiled)?;

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

        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = with_scalar_aggregate_terminal_attribution(|| {
            measure_execute_phase_with_physical_access(|| {
                self.execute_compiled_sql_with_cache_attribution::<E>(compiled)
            })
        });
        let (result, cache_attribution) = result?;
        let mut phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
            execute_local_instructions,
            store_local_instructions,
        );
        phase_attribution.scalar_aggregate_terminal = scalar_aggregate_terminal;

        Ok((result, cache_attribution, phase_attribution))
    }

    // Execute one compiled SQL command while preserving diagnostics-only
    // cache, planning, executor, and response-finalization phase attribution
    // at the session/executor handoff.
    #[cfg(feature = "diagnostics")]
    #[expect(
        clippy::too_many_lines,
        reason = "diagnostics phase attribution keeps the SQL command-family split explicit at one session boundary"
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
                    let (planner_local_instructions, resolved_query_plan) =
                        measure_sql_stage(|| {
                            self.sql_select_prepared_plan(
                                query,
                                authority,
                                compiled_cache_key.schema_fingerprint(),
                            )
                        });
                    let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                    let ((execute_local_instructions, store_local_instructions), statement_result) =
                        measure_execute_phase_with_physical_access(move || {
                            self.execute_grouped_sql_statement_with_response_attribution(
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
                    let (
                        statement_result,
                        grouped_phase_attribution,
                        response_finalization_local_instructions,
                    ) = statement_result?;

                    return Ok((
                        statement_result,
                        cache_attribution,
                        SqlExecutePhaseAttribution {
                            planner_local_instructions,
                            store_local_instructions,
                            executor_invocation_local_instructions: execute_local_instructions
                                .saturating_sub(response_finalization_local_instructions),
                            executor_local_instructions: execute_local_instructions
                                .saturating_sub(store_local_instructions)
                                .saturating_sub(response_finalization_local_instructions),
                            response_finalization_local_instructions,
                            grouped_stream_local_instructions: grouped_phase_attribution
                                .stream_local_instructions,
                            grouped_fold_local_instructions: grouped_phase_attribution
                                .fold_local_instructions,
                            grouped_finalize_local_instructions: grouped_phase_attribution
                                .finalize_local_instructions,
                            grouped_count: grouped_phase_attribution.grouped_count,
                            scalar_aggregate_terminal:
                                crate::db::executor::ScalarAggregateTerminalAttribution::none(),
                        },
                    ));
                }

                let (planner_local_instructions, resolved_query_plan) = measure_sql_stage(|| {
                    self.sql_select_prepared_plan(
                        query,
                        authority,
                        compiled_cache_key.schema_fingerprint(),
                    )
                });
                let (prepared_plan, projection, cache_attribution) = resolved_query_plan?;

                let ((execute_local_instructions, store_local_instructions), payload) =
                    measure_execute_phase_with_physical_access(move || {
                        self.execute_sql_projection_from_structural_prepared_plan(
                            prepared_plan,
                            projection,
                            SqlCacheAttribution::default(),
                        )
                        .map(|(payload, _)| payload)
                    });
                let payload = payload?;
                let (response_finalization_local_instructions, statement_result) =
                    measure_sql_stage(|| Ok::<_, QueryError>(payload.into_statement_result()));
                let statement_result = statement_result?;

                Ok((
                    statement_result,
                    cache_attribution,
                    SqlExecutePhaseAttribution {
                        planner_local_instructions,
                        store_local_instructions,
                        executor_invocation_local_instructions: execute_local_instructions,
                        executor_local_instructions: execute_local_instructions
                            .saturating_sub(store_local_instructions),
                        response_finalization_local_instructions,
                        grouped_stream_local_instructions: 0,
                        grouped_fold_local_instructions: 0,
                        grouped_finalize_local_instructions: 0,
                        grouped_count: crate::db::executor::GroupedCountAttribution::none(),
                        scalar_aggregate_terminal:
                            crate::db::executor::ScalarAggregateTerminalAttribution::none(),
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

    fn execute_select_compiled_sql_with_cache_attribution(
        &self,
        query: &StructuralQuery,
        compiled_cache_key: &SqlCompiledCommandCacheKey,
        authority: EntityAuthority,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        if query.has_grouping() {
            let (prepared_plan, projection, cache_attribution) = self.sql_select_prepared_plan(
                query,
                authority,
                compiled_cache_key.schema_fingerprint(),
            )?;
            let (statement_result, ()) = self.execute_grouped_sql_statement_from_prepared_plan(
                prepared_plan,
                projection,
                authority,
                |session, authority, plan| {
                    crate::db::executor::pipeline::execute_initial_grouped_rows_for_canister(
                        &session.db,
                        session.debug,
                        authority,
                        plan,
                    )
                    .map_err(QueryError::execute)
                    .map(|result| (result, ()))
                },
            )?;

            return Ok((statement_result, cache_attribution));
        }

        let (prepared_plan, projection, cache_attribution) = self.sql_select_prepared_plan(
            query,
            authority,
            compiled_cache_key.schema_fingerprint(),
        )?;
        let (payload, cache_attribution) = self
            .execute_sql_projection_from_structural_prepared_plan(
                prepared_plan,
                projection,
                cache_attribution,
            )?;

        Ok((payload.into_statement_result(), cache_attribution))
    }

    #[cfg(any(test, feature = "diagnostics"))]
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
            } => self.execute_select_compiled_sql_with_cache_attribution(
                query,
                compiled_cache_key,
                authority,
            ),
            CompiledSqlCommand::Delete { query, returning } => self
                .execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref())
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

    pub(in crate::db) fn execute_compiled_sql_owned_with_cache_attribution<E>(
        &self,
        compiled: CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority = EntityAuthority::for_type::<E>();

        match compiled {
            CompiledSqlCommand::Select {
                query,
                compiled_cache_key,
            } => self.execute_select_compiled_sql_with_cache_attribution(
                query.as_ref(),
                &compiled_cache_key,
                authority,
            ),
            CompiledSqlCommand::Delete { query, returning } => self
                .execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref())
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::GlobalAggregate { command } => {
                self.execute_global_aggregate_statement_for_authority::<E>(*command, authority)
            }
            CompiledSqlCommand::Explain(lowered) => {
                if let Some(explain) =
                    self.explain_lowered_sql_execution_for_authority(&lowered, authority)?
                {
                    return Ok((
                        SqlStatementResult::Explain(explain),
                        SqlCacheAttribution::default(),
                    ));
                }

                self.explain_lowered_sql_for_authority(&lowered, authority)
                    .map(SqlStatementResult::Explain)
                    .map(|result| (result, SqlCacheAttribution::default()))
            }
            CompiledSqlCommand::Insert(statement) => self
                .execute_sql_insert_statement::<E>(&statement)
                .map(|result| (result, SqlCacheAttribution::default())),
            CompiledSqlCommand::Update(statement) => self
                .execute_sql_update_statement::<E>(&statement)
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

        self.execute_compiled_sql_owned::<E>(compiled)
    }
}
