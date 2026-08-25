//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            execute_structural_aggregate_rows_for_canister,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlStatementResult,
                projection::sql_projection_statement_result_from_value_rows,
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
};

#[cfg(feature = "diagnostics")]
use super::aggregate_plan::MeasuredPreparedAggregatePlanResolution;
use super::aggregate_plan::PreparedAggregatePlanResolution;
use super::aggregate_request::PreparedAggregateRequestBundle;
#[cfg(feature = "diagnostics")]
use super::diagnostics::measure_scalar_aggregate_execute_phase_with_physical_access;
use super::exact_count::{ExactCountOutcome, ExactCountTarget};
#[cfg(feature = "diagnostics")]
use crate::db::session::{
    query::QueryPlanCompilePhaseAttribution, sql::SqlExecutePhaseAttribution,
};

impl<C: CanisterKind> DbSession<C> {
    fn execute_global_aggregate_with_prepared_plan(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let schema_info = catalog.accepted_schema_info();
        let bundle =
            PreparedAggregateRequestBundle::from_global_command(command, schema_info.clone())?;
        let (request, projection) = bundle.into_parts();
        let rows = self
            .with_metrics(|| {
                execute_structural_aggregate_rows_for_canister(
                    &self.db,
                    self.debug,
                    prepared_plan,
                    request,
                )
            })
            .map_err(QueryError::execute)?;
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let (columns, fixed_scales) = projection.into_components();

        Ok((
            sql_projection_statement_result_from_value_rows(
                catalog.enum_catalog(),
                columns,
                fixed_scales,
                rows,
                row_count,
            )?,
            cache_attribution,
        ))
    }

    fn execute_global_aggregate_after_exact_count_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        exact_count_target: ExactCountTarget,
        resolve_prepared_plan: impl FnOnce(Option<EntityAuthority>) -> PreparedAggregatePlanResolution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let exact_resolution =
            self.execute_exact_count_target(command.projection(), exact_count_target)?;
        let fallback_authority = match exact_resolution {
            ExactCountOutcome::Direct {
                result,
                cache_attribution,
                ..
            } => {
                return Ok((result, cache_attribution));
            }
            ExactCountOutcome::Prepared {
                prepared_plan,
                cache_attribution,
            } => {
                return self.execute_global_aggregate_with_prepared_plan(
                    command,
                    catalog,
                    prepared_plan,
                    cache_attribution,
                );
            }
            ExactCountOutcome::Fallback { authority, .. } => authority,
        };

        let (prepared_plan, cache_attribution) = resolve_prepared_plan(fallback_authority)?;

        self.execute_global_aggregate_with_prepared_plan(
            command,
            catalog,
            prepared_plan,
            cache_attribution,
        )
    }

    #[cfg(feature = "diagnostics")]
    fn execute_measured_global_aggregate_after_exact_count_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        exact_count_target: ExactCountTarget,
        exact_plan_compile_attribution: QueryPlanCompilePhaseAttribution,
        resolve_prepared_plan: impl FnOnce(
            Option<EntityAuthority>,
        ) -> MeasuredPreparedAggregatePlanResolution,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        let exact_resolution = self.execute_measured_exact_count_target(
            command.projection(),
            exact_count_target,
            exact_plan_compile_attribution,
        )?;
        let (
            fallback_authority,
            exact_execute_local_instructions,
            exact_store_local_instructions,
            cached_prepared_plan,
        ) = match exact_resolution {
            ExactCountOutcome::Direct {
                result,
                cache_attribution,
                phase_attribution,
            } => {
                let Some(phase_attribution) = phase_attribution else {
                    return Err(QueryError::invariant());
                };

                return Ok((result, cache_attribution, *phase_attribution));
            }
            ExactCountOutcome::Prepared {
                prepared_plan,
                cache_attribution,
            } => (None, 0, 0, Some((prepared_plan, cache_attribution))),
            ExactCountOutcome::Fallback {
                authority,
                execute_local_instructions,
                store_local_instructions,
            } => (
                authority,
                execute_local_instructions,
                store_local_instructions,
                None,
            ),
        };

        let (prepared_plan, cache_attribution, mut plan_compile_attribution) =
            if let Some((prepared_plan, cache_attribution)) = cached_prepared_plan {
                (
                    prepared_plan,
                    cache_attribution,
                    QueryPlanCompilePhaseAttribution::default(),
                )
            } else {
                let (prepared_plan, cache_attribution, plan_compile_attribution) =
                    resolve_prepared_plan(fallback_authority)?;
                (prepared_plan, cache_attribution, plan_compile_attribution)
            };
        plan_compile_attribution.merge(exact_plan_compile_attribution);
        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = measure_scalar_aggregate_execute_phase_with_physical_access(|| {
            self.execute_global_aggregate_with_prepared_plan(
                command,
                catalog,
                prepared_plan,
                cache_attribution,
            )
        });
        let (result, cache_attribution) = result?;
        let phase_attribution =
            SqlExecutePhaseAttribution::from_query_plan_execute_total_and_store_total(
                plan_compile_attribution.planner_local_instructions(),
                plan_compile_attribution,
                execute_local_instructions.saturating_add(exact_execute_local_instructions),
                store_local_instructions.saturating_add(exact_store_local_instructions),
            )
            .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

        Ok((result, cache_attribution, phase_attribution))
    }

    // Execute one borrowed compiled global aggregate while reusing its
    // compiled-command resident shared plan when the schema fingerprint still
    // matches the accepted snapshot carried by this execution context.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_catalog(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let exact_count_target =
            self.resolve_compiled_exact_count_target(compiled, command, catalog)?;

        self.execute_global_aggregate_after_exact_count_target(
            command,
            catalog,
            exact_count_target,
            |fallback_authority| {
                self.resolve_compiled_global_aggregate_prepared_plan(
                    compiled,
                    command,
                    catalog,
                    fallback_authority,
                )
            },
        )
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_phase_attribution(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        let (exact_count_target, exact_plan_compile_attribution) = self
            .resolve_compiled_exact_count_target_with_phase_attribution(
                compiled, command, catalog,
            )?;

        self.execute_measured_global_aggregate_after_exact_count_target(
            command,
            catalog,
            exact_count_target,
            exact_plan_compile_attribution,
            |fallback_authority| {
                self.resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution(
                    compiled,
                    command,
                    catalog,
                    fallback_authority,
                )
            },
        )
    }
}
