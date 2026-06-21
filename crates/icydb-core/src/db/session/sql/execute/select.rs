//! Module: db::session::sql::execute::select
//! Responsibility: SQL SELECT projection, grouped execution, and cache-aware
//! prepared-plan execution.
//! Does not own: SQL command routing, write execution, or EXPLAIN rendering.
//! Boundary: keeps SELECT plan-to-result adaptation out of the SQL execution hub.

#[cfg(feature = "diagnostics")]
use crate::db::session::{
    query::QueryPlanCompilePhaseAttribution,
    sql::{SqlExecutePhaseAttribution, measure_sql_stage},
};
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan, StructuralGroupedProjectionResult,
        },
        query::intent::StructuralQuery,
        schema::AcceptedSchemaSnapshot,
        session::{
            finalize_structural_grouped_projection_result,
            sql::SqlProjectionContract,
            sql::projection::{SqlProjectionPayload, execute_sql_projection_rows_for_canister},
            sql::{SqlCacheAttribution, SqlCompiledCommandExecutionContext, SqlStatementResult},
            sql_grouped_cursor_from_bytes,
        },
    },
    traits::{CanisterKind, EntityValue},
};

use super::diagnostics::GroupedSqlDiagnosticsCollector;
#[cfg(feature = "diagnostics")]
use super::diagnostics::measure_execute_phase_with_physical_access;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::projection::execute_sql_projection_rows_for_canister_with_direct_data_row_attribution;

impl<C: CanisterKind> DbSession<C> {
    // Convert one grouped executor result plus SQL projection labels into the
    // statement result shape shared by normal and diagnostics SQL execution.
    pub(in crate::db::session::sql::execute) fn grouped_sql_statement_result_from_result(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        result: StructuralGroupedProjectionResult,
    ) -> Result<SqlStatementResult, QueryError> {
        let row_count = result.row_count();
        let grouped = finalize_structural_grouped_projection_result(result, None)?;
        let (rows, continuation_cursor, _) = grouped.into_rows_cursor_and_trace();
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
        projection: SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (columns, fixed_scales) = projection.into_components();
        let (rows, row_count) =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                .map_err(QueryError::execute)?;

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
    }

    // Execute one SQL projection and immediately shape it into the public
    // statement-result envelope. Diagnostics keeps using the payload-returning
    // sibling so it can measure response finalization separately.
    fn execute_sql_statement_from_structural_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let (payload, cache_attribution) = self
            .execute_sql_projection_from_structural_prepared_plan(
                prepared_plan,
                projection,
                cache_attribution,
            )?;

        Ok((payload.into_statement_result(), cache_attribution))
    }

    // Execute one grouped SQL statement from one shared lowered prepared plan
    // plus one thin SQL projection contract. Normal and diagnostics surfaces
    // share this plan-to-statement shell; diagnostics only swaps response
    // finalization through the optional collector.
    fn execute_grouped_sql_core<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        diagnostics: Option<GroupedSqlDiagnosticsCollector<'_>>,
        execute_grouped: impl FnOnce(
            &Self,
            SharedPreparedExecutionPlan,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        let (columns, fixed_scales) = projection.into_components();
        let (result, extra) = execute_grouped(self, prepared_plan)?;
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
        projection: SqlProjectionContract,
        execute_grouped: impl FnOnce(
            &Self,
            SharedPreparedExecutionPlan,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        self.execute_grouped_sql_core(prepared_plan, projection, None, execute_grouped)
    }

    // Diagnostics-only grouped SQL execution split that keeps runtime
    // invocation and session response-envelope finalization in separate
    // counters while sharing the same grouped SQL core as normal execution.
    #[cfg(feature = "diagnostics")]
    fn execute_grouped_sql_statement_with_response_attribution<T>(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        execute_grouped: impl FnOnce(
            &Self,
            SharedPreparedExecutionPlan,
        )
            -> Result<(StructuralGroupedProjectionResult, T), QueryError>,
    ) -> Result<(SqlStatementResult, T, u64), QueryError> {
        let mut response_finalization_local_instructions = 0;
        let diagnostics =
            GroupedSqlDiagnosticsCollector::new(&mut response_finalization_local_instructions);
        let (statement_result, extra) = self.execute_grouped_sql_core(
            prepared_plan,
            projection,
            Some(diagnostics),
            execute_grouped,
        )?;

        Ok((
            statement_result,
            extra,
            response_finalization_local_instructions,
        ))
    }

    // Execute one SQL load query from a structural lowered query through the
    // shared lower query-plan cache while bypassing only the compiled SQL
    // command cache for lowered or aggregate-only paths.
    pub(in crate::db::session::sql) fn execute_sql_projection_from_structural_query_without_sql_compiled_cache(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<(SqlProjectionPayload, SqlCacheAttribution), QueryError> {
        let (prepared_plan, projection, cache_attribution) = self
            .sql_select_prepared_plan_for_accepted_authority(&query, authority, accepted_schema)?;

        self.execute_sql_projection_from_structural_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    #[cfg(feature = "diagnostics")]
    const fn grouped_select_execute_phase_attribution(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        grouped_phase_attribution: crate::db::executor::GroupedExecutePhaseAttribution,
    ) -> SqlExecutePhaseAttribution {
        SqlExecutePhaseAttribution {
            planner_local_instructions,
            planner_schema_info_local_instructions: plan_compile_attribution.schema_info,
            planner_prepare_local_instructions: plan_compile_attribution.prepare,
            planner_cache_key_local_instructions: plan_compile_attribution.cache_key,
            planner_cache_lookup_local_instructions: plan_compile_attribution.cache_lookup,
            planner_plan_build_local_instructions: plan_compile_attribution.plan_build,
            planner_cache_insert_local_instructions: plan_compile_attribution.cache_insert,
            store_local_instructions,
            executor_invocation_local_instructions: execute_local_instructions
                .saturating_sub(response_finalization_local_instructions),
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions)
                .saturating_sub(response_finalization_local_instructions),
            response_finalization_local_instructions,
            grouped_stream_local_instructions: grouped_phase_attribution.stream_local_instructions,
            grouped_fold_local_instructions: grouped_phase_attribution.fold_local_instructions,
            grouped_finalize_local_instructions: grouped_phase_attribution
                .finalize_local_instructions,
            grouped_count: grouped_phase_attribution.grouped_count,
            scalar_aggregate_terminal:
                crate::db::executor::ScalarAggregateTerminalAttribution::none(),
            direct_data_row: None,
            kernel_row: None,
        }
    }

    #[cfg(feature = "diagnostics")]
    const fn projection_select_execute_phase_attribution(
        planner_local_instructions: u64,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        execute_local_instructions: u64,
        store_local_instructions: u64,
        response_finalization_local_instructions: u64,
        direct_data_row: Option<crate::db::DirectDataRowAttribution>,
        kernel_row: Option<crate::db::KernelRowAttribution>,
    ) -> SqlExecutePhaseAttribution {
        SqlExecutePhaseAttribution {
            planner_local_instructions,
            planner_schema_info_local_instructions: plan_compile_attribution.schema_info,
            planner_prepare_local_instructions: plan_compile_attribution.prepare,
            planner_cache_key_local_instructions: plan_compile_attribution.cache_key,
            planner_cache_lookup_local_instructions: plan_compile_attribution.cache_lookup,
            planner_plan_build_local_instructions: plan_compile_attribution.plan_build,
            planner_cache_insert_local_instructions: plan_compile_attribution.cache_insert,
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
            direct_data_row,
            kernel_row,
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn execute_select_compiled_sql_with_phase_attribution_from_resolver<E>(
        &self,
        query: &StructuralQuery,
        resolve_plan: impl FnOnce() -> Result<
            (
                SharedPreparedExecutionPlan,
                SqlProjectionContract,
                SqlCacheAttribution,
                QueryPlanCompilePhaseAttribution,
            ),
            QueryError,
        >,
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
        if query.has_grouping() {
            let (planner_local_instructions, resolved_query_plan) = measure_sql_stage(resolve_plan);
            let (prepared_plan, projection, cache_attribution, plan_compile_attribution) =
                resolved_query_plan?;

            let ((execute_local_instructions, store_local_instructions), statement_result) =
                measure_execute_phase_with_physical_access(move || {
                    self.execute_grouped_sql_statement_with_response_attribution(
                        prepared_plan,
                        projection,
                        |session, prepared_plan| {
                            let plan = prepared_plan.typed_clone::<E>();
                            session
                                .execute_grouped_with_phase_attribution(plan, None)
                                .map(|(result, _trace, phase_attribution)| {
                                    (result, phase_attribution)
                                })
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
                Self::grouped_select_execute_phase_attribution(
                    planner_local_instructions,
                    plan_compile_attribution,
                    execute_local_instructions,
                    store_local_instructions,
                    response_finalization_local_instructions,
                    grouped_phase_attribution,
                ),
            ));
        }

        let (planner_local_instructions, resolved_query_plan) = measure_sql_stage(resolve_plan);
        let (prepared_plan, projection, cache_attribution, plan_compile_attribution) =
            resolved_query_plan?;

        let ((execute_local_instructions, store_local_instructions), payload) =
            measure_execute_phase_with_physical_access(move || {
                let (columns, fixed_scales) = projection.into_components();
                execute_sql_projection_rows_for_canister_with_direct_data_row_attribution(
                    &self.db,
                    self.debug,
                    prepared_plan,
                )
                .map(|((rows, row_count), direct_data_row, kernel_row)| {
                    (
                        SqlProjectionPayload::new(columns, fixed_scales, rows, row_count),
                        direct_data_row,
                        kernel_row,
                    )
                })
                .map_err(QueryError::execute)
            });
        let (payload, direct_data_row, kernel_row) = payload?;
        let (response_finalization_local_instructions, statement_result) =
            measure_sql_stage(|| Ok::<_, QueryError>(payload.into_statement_result()));
        let statement_result = statement_result?;

        Ok((
            statement_result,
            cache_attribution,
            Self::projection_select_execute_phase_attribution(
                planner_local_instructions,
                plan_compile_attribution,
                execute_local_instructions,
                store_local_instructions,
                response_finalization_local_instructions,
                direct_data_row,
                kernel_row,
            ),
        ))
    }

    pub(super) fn execute_select_compiled_sql_with_cache_attribution<E>(
        &self,
        query: &StructuralQuery,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let authority = catalog
            .accepted_entity_authority_for::<E>()
            .map_err(QueryError::execute)?;

        let (prepared_plan, projection, cache_attribution) = self
            .sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
                query,
                authority,
                catalog.snapshot(),
                catalog.fingerprint(),
            )?;

        self.execute_select_compiled_sql_from_prepared_plan::<E>(
            query,
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    pub(super) fn execute_select_compiled_sql_with_context<E>(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some((prepared_plan, projection)) = context.command().cached_select_plan(
            context.schema_fingerprint_method_version(),
            context.schema_fingerprint(),
        ) {
            return self.execute_select_compiled_sql_from_prepared_plan::<E>(
                query,
                prepared_plan,
                projection,
                SqlCacheAttribution::shared_query_plan_cache_hit(),
            );
        }

        let authority = match context.accepted_authority() {
            Some(authority) => authority.clone(),
            None => context
                .accepted_catalog()
                .accepted_entity_authority_for::<E>()
                .map_err(QueryError::execute)?,
        };

        let resolved = self
            .sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
                query,
                authority,
                context.accepted_schema(),
                context.schema_fingerprint(),
            );
        if let Ok((prepared_plan, projection, _)) = &resolved {
            context.command().set_cached_select_plan(
                context.schema_fingerprint_method_version(),
                context.schema_fingerprint(),
                prepared_plan.clone(),
                projection.clone(),
            );
        }
        let (prepared_plan, projection, cache_attribution) = resolved?;

        self.execute_select_compiled_sql_from_prepared_plan::<E>(
            query,
            prepared_plan,
            projection,
            cache_attribution,
        )
    }

    fn execute_select_compiled_sql_from_prepared_plan<E>(
        &self,
        query: &StructuralQuery,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if query.has_grouping() {
            let (statement_result, ()) = self.execute_grouped_sql_statement_from_prepared_plan(
                prepared_plan,
                projection,
                |session, prepared_plan| {
                    let plan = prepared_plan.typed_clone::<E>();
                    session
                        .execute_grouped_with_trace(plan, None)
                        .map(|(result, _trace)| (result, ()))
                },
            )?;

            return Ok((statement_result, cache_attribution));
        }

        self.execute_sql_statement_from_structural_prepared_plan(
            prepared_plan,
            projection,
            cache_attribution,
        )
    }
}
