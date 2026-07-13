//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

#[cfg(test)]
use crate::db::query::admission::QueryAdmissionPolicy;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
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
#[cfg(feature = "diagnostics")]
use super::direct_count::MeasuredDirectCountCardinalityOutcome;
use super::direct_count::{
    DirectCountCardinalityOutcome, DirectCountCardinalityTarget, direct_count_rows_statement_result,
};
#[cfg(test)]
use super::select::{enforce_read_admission_policy, enforce_sql_read_response_byte_policy};
#[cfg(feature = "diagnostics")]
use crate::db::session::{
    query::QueryPlanCompilePhaseAttribution, sql::SqlExecutePhaseAttribution,
};

impl<C: CanisterKind> DbSession<C> {
    fn execute_global_aggregate_with_prepared_plan<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        prepared_plan: &SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let projection = command.projection();

        if command.facts().is_direct_count_rows() {
            let value = self.execute_direct_count_rows_global_aggregate::<E>(prepared_plan)?;

            return direct_count_rows_statement_result(projection, value, cache_attribution);
        }
        let schema_info = catalog.accepted_schema_info_for::<E>();
        let bundle = PreparedAggregateRequestBundle::from_global_command(command, schema_info)?;
        let (request, projection) = bundle.into_parts();
        let result = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_structural_aggregate_result(prepared_plan, request)
            })
            .map_err(QueryError::execute)?;
        let rows = result.into_value_rows();
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

    fn execute_global_aggregate_after_direct_count_target<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        direct_count_target: DirectCountCardinalityTarget,
        resolve_prepared_plan: impl FnOnce(Option<EntityAuthority>) -> PreparedAggregatePlanResolution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let direct_resolution = self.execute_direct_count_cardinality_target::<E>(
            command.projection(),
            direct_count_target,
        )?;
        let fallback_authority = match direct_resolution {
            DirectCountCardinalityOutcome::Direct(result, cache_attribution) => {
                return Ok((result, cache_attribution));
            }
            DirectCountCardinalityOutcome::Fallback { authority } => authority,
        };

        let resolved = resolve_prepared_plan(fallback_authority)?;
        let (prepared_plan, cache_attribution) = resolved.into_parts();

        self.execute_global_aggregate_with_prepared_plan::<E>(
            command,
            catalog,
            &prepared_plan,
            cache_attribution,
        )
    }

    #[cfg(feature = "diagnostics")]
    fn execute_measured_global_aggregate_after_direct_count_target<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        direct_count_target: DirectCountCardinalityTarget,
        direct_plan_compile_attribution: QueryPlanCompilePhaseAttribution,
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
    >
    where
        E: PersistedRow<Canister = C>,
    {
        let direct_resolution = self.execute_measured_direct_count_cardinality_target::<E>(
            command.projection(),
            direct_count_target,
            direct_plan_compile_attribution,
        )?;
        let (
            fallback_authority,
            direct_execute_local_instructions,
            direct_store_local_instructions,
        ) = match direct_resolution {
            MeasuredDirectCountCardinalityOutcome::Direct {
                result,
                cache_attribution,
                phase_attribution,
            } => return Ok((result, cache_attribution, *phase_attribution)),
            MeasuredDirectCountCardinalityOutcome::Fallback {
                authority,
                execute_local_instructions,
                store_local_instructions,
            } => (
                authority,
                execute_local_instructions,
                store_local_instructions,
            ),
        };

        let (resolved, mut plan_compile_attribution) = resolve_prepared_plan(fallback_authority)?;
        let (prepared_plan, cache_attribution) = resolved.into_parts();
        plan_compile_attribution.merge(direct_plan_compile_attribution);
        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = measure_scalar_aggregate_execute_phase_with_physical_access(|| {
            self.execute_global_aggregate_with_prepared_plan::<E>(
                command,
                catalog,
                &prepared_plan,
                cache_attribution,
            )
        });
        let (result, cache_attribution) = result?;
        let phase_attribution =
            SqlExecutePhaseAttribution::from_query_plan_execute_total_and_store_total(
                plan_compile_attribution.planner_local_instructions(),
                plan_compile_attribution,
                execute_local_instructions.saturating_add(direct_execute_local_instructions),
                store_local_instructions.saturating_add(direct_store_local_instructions),
            )
            .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

        Ok((result, cache_attribution, phase_attribution))
    }

    // Execute one borrowed prepared SQL aggregate command through executor-owned
    // structural aggregate execution after resolving the accepted catalog.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_ref<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;

        self.execute_global_aggregate_statement_ref_with_catalog::<E>(command, &catalog)
    }

    // Execute one borrowed prepared SQL aggregate command when the caller
    // already owns the accepted catalog loaded during SQL compile.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_ref_with_catalog<
        E,
    >(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let direct_count_target =
            self.build_direct_count_cardinality_target::<E>(command, catalog)?;

        self.execute_global_aggregate_after_direct_count_target::<E>(
            command,
            catalog,
            direct_count_target,
            |fallback_authority| {
                let authority = Self::global_aggregate_prepared_plan_authority::<E>(
                    catalog,
                    fallback_authority,
                )?;
                self.resolve_global_aggregate_prepared_plan_for_authority(
                    command, catalog, authority,
                )
            },
        )
    }

    // Execute one borrowed compiled global aggregate while reusing its
    // compiled-command resident shared plan when the schema fingerprint still
    // matches the accepted snapshot carried by this execution context.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_catalog<
        E,
    >(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let direct_count_target =
            self.resolve_compiled_direct_count_cardinality_target::<E>(compiled, command, catalog)?;

        self.execute_global_aggregate_after_direct_count_target::<E>(
            command,
            catalog,
            direct_count_target,
            |fallback_authority| {
                self.resolve_compiled_global_aggregate_prepared_plan::<E>(
                    compiled,
                    command,
                    catalog,
                    fallback_authority,
                )
            },
        )
    }

    #[cfg(test)]
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_read_admission_policy<
        E,
    >(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        policy: &QueryAdmissionPolicy,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let resolved = self.resolve_compiled_global_aggregate_prepared_plan::<E>(
            compiled, command, &catalog, None,
        )?;
        let (prepared_plan, cache_attribution) = resolved.into_parts();

        enforce_read_admission_policy(policy, &prepared_plan)?;

        let (result, _) = self.execute_global_aggregate_with_prepared_plan::<E>(
            command,
            &catalog,
            &prepared_plan,
            cache_attribution,
        )?;
        enforce_sql_read_response_byte_policy(policy, &result)?;

        Ok(result)
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_phase_attribution<
        E,
    >(
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
    >
    where
        E: PersistedRow<Canister = C>,
    {
        let (direct_count_target, direct_plan_compile_attribution) = self
            .resolve_compiled_direct_count_cardinality_target_with_phase_attribution::<E>(
                compiled, command, catalog,
            )?;

        self.execute_measured_global_aggregate_after_direct_count_target::<E>(
            command,
            catalog,
            direct_count_target,
            direct_plan_compile_attribution,
            |fallback_authority| {
                self.resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution::<E>(
                    compiled,
                    command,
                    catalog,
                    fallback_authority,
                )
            },
        )
    }
}
