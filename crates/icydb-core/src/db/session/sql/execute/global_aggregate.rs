//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        access::LoweredIndexPrefixCardinalitySpec,
        executor::{
            EntityAuthority, ScalarTerminalBoundaryRequest, SharedPreparedExecutionPlan,
            StructuralAggregateRequest, StructuralAggregateTerminal,
            StructuralAggregateTerminalKind,
        },
        query::plan::{AggregateKind, VisibleIndexes, expr::ProjectionSpec},
        schema::SchemaInfo,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlGlobalAggregateCountPlanCacheEntry,
                SqlProjectionContract, SqlStatementResult,
                projection::{
                    projection_contract_from_projection_spec,
                    sql_projection_statement_result_from_value_rows,
                },
            },
        },
        sql::lowering::{
            PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommand,
        },
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};
use std::sync::Arc;

#[cfg(feature = "diagnostics")]
use super::diagnostics::measure_execute_phase_with_physical_access;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::measure_sql_stage;
#[cfg(feature = "diagnostics")]
use crate::db::{
    executor::with_scalar_aggregate_terminal_attribution,
    session::{query::QueryPlanCompilePhaseAttribution, sql::SqlExecutePhaseAttribution},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlAggregateTerminalBuildError {
    UnsupportedStrategyDrift,
}

struct DirectCountCardinalityPlanProbe {
    authority: EntityAuthority,
    entry: Option<Arc<SqlGlobalAggregateCountPlanCacheEntry>>,
}

struct DirectCountCardinalityProbeResolution {
    result: Option<(SqlStatementResult, SqlCacheAttribution)>,
    fallback_authority: Option<EntityAuthority>,
}

struct PreparedAggregateRequestBundle {
    request: StructuralAggregateRequest,
    projection: SqlProjectionContract,
}

impl PreparedAggregateRequestBundle {
    fn from_global_command(
        command: &SqlGlobalAggregateCommand,
        schema_info: SchemaInfo,
    ) -> Result<Self, QueryError> {
        let projection = command.projection();
        let terminals = command
            .strategies()
            .iter()
            .cloned()
            .map(|strategy| {
                build_structural_aggregate_terminal_from_sql_strategy(strategy)
                    .map_err(|_err| QueryError::invariant())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = StructuralAggregateRequest::new(
            terminals,
            projection.clone(),
            command.having().cloned(),
            schema_info,
        );

        Ok(Self {
            request,
            projection: projection_contract_from_projection_spec(projection),
        })
    }

    fn into_parts(self) -> (StructuralAggregateRequest, SqlProjectionContract) {
        let Self {
            request,
            projection,
        } = self;

        (request, projection)
    }
}

// Convert one prepared SQL aggregate strategy into the executor terminal DTO at
// the session boundary so SQL lowering stays executor-neutral.
fn build_structural_aggregate_terminal_from_sql_strategy(
    strategy: PreparedSqlScalarAggregateStrategy,
) -> Result<StructuralAggregateTerminal, SqlAggregateTerminalBuildError> {
    let (descriptor, target_slot, input_expr, filter_expr, distinct_input) =
        strategy.into_structural_terminal_inputs();

    let kind = match descriptor {
        PreparedSqlScalarAggregatePlanFragment::CountRows => {
            StructuralAggregateTerminalKind::CountRows
        }
        PreparedSqlScalarAggregatePlanFragment::CountField => {
            StructuralAggregateTerminalKind::CountValues
        }
        PreparedSqlScalarAggregatePlanFragment::NumericField {
            kind: AggregateKind::Sum,
        } => StructuralAggregateTerminalKind::Sum,
        PreparedSqlScalarAggregatePlanFragment::NumericField {
            kind: AggregateKind::Avg,
        } => StructuralAggregateTerminalKind::Avg,
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Min,
        } => StructuralAggregateTerminalKind::Min,
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Max,
        } => StructuralAggregateTerminalKind::Max,
        PreparedSqlScalarAggregatePlanFragment::NumericField { .. }
        | PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField { .. } => {
            return Err(SqlAggregateTerminalBuildError::UnsupportedStrategyDrift);
        }
    };

    Ok(StructuralAggregateTerminal::new(
        kind,
        target_slot,
        input_expr,
        filter_expr,
        distinct_input,
    ))
}

fn direct_count_rows_statement_result(
    projection: &ProjectionSpec,
    value: Value,
    cache_attribution: SqlCacheAttribution,
) -> (SqlStatementResult, SqlCacheAttribution) {
    let (columns, fixed_scales) =
        projection_contract_from_projection_spec(projection).into_components();

    (
        sql_projection_statement_result_from_value_rows(
            columns,
            fixed_scales,
            vec![vec![value]],
            1,
        ),
        cache_attribution,
    )
}

fn direct_count_cardinality_plan_entry_from_prefix_specs(
    catalog: &AcceptedSchemaCatalogContext,
    prefix_specs: Option<Vec<LoweredIndexPrefixCardinalitySpec>>,
) -> Option<Arc<SqlGlobalAggregateCountPlanCacheEntry>> {
    let prefix_specs = prefix_specs?;
    if prefix_specs.is_empty() {
        return None;
    }

    Some(Arc::new(SqlGlobalAggregateCountPlanCacheEntry::new(
        catalog.fingerprint_method_version(),
        catalog.fingerprint(),
        Arc::from(prefix_specs),
    )))
}

fn direct_count_cardinality_probe_from_entry<E>(
    catalog: &AcceptedSchemaCatalogContext,
    entry: Arc<SqlGlobalAggregateCountPlanCacheEntry>,
) -> Result<DirectCountCardinalityPlanProbe, QueryError>
where
    E: PersistedRow + EntityValue,
{
    let authority = catalog
        .accepted_entity_authority_for::<E>()
        .map_err(QueryError::execute)?;

    Ok(DirectCountCardinalityPlanProbe {
        authority,
        entry: Some(entry),
    })
}

#[cfg(feature = "diagnostics")]
const fn planner_local_instructions_from_plan_compile_attribution(
    attribution: QueryPlanCompilePhaseAttribution,
) -> u64 {
    attribution
        .schema_info
        .saturating_add(attribution.prepare)
        .saturating_add(attribution.cache_key)
        .saturating_add(attribution.cache_lookup)
        .saturating_add(attribution.plan_build)
        .saturating_add(attribution.cache_insert)
}

#[cfg(feature = "diagnostics")]
const fn apply_plan_compile_attribution_to_execute_phase(
    phase_attribution: &mut SqlExecutePhaseAttribution,
    plan_compile_attribution: QueryPlanCompilePhaseAttribution,
) {
    phase_attribution.planner_local_instructions =
        planner_local_instructions_from_plan_compile_attribution(plan_compile_attribution);
    phase_attribution.planner_schema_info_local_instructions = plan_compile_attribution.schema_info;
    phase_attribution.planner_prepare_local_instructions = plan_compile_attribution.prepare;
    phase_attribution.planner_cache_key_local_instructions = plan_compile_attribution.cache_key;
    phase_attribution.planner_cache_lookup_local_instructions =
        plan_compile_attribution.cache_lookup;
    phase_attribution.planner_plan_build_local_instructions = plan_compile_attribution.plan_build;
    phase_attribution.planner_cache_insert_local_instructions =
        plan_compile_attribution.cache_insert;
}

#[cfg(feature = "diagnostics")]
const fn merge_plan_compile_attribution(
    attribution: &mut QueryPlanCompilePhaseAttribution,
    other: QueryPlanCompilePhaseAttribution,
) {
    attribution.schema_catalog = attribution
        .schema_catalog
        .saturating_add(other.schema_catalog);
    attribution.schema_info = attribution.schema_info.saturating_add(other.schema_info);
    attribution.prepare = attribution.prepare.saturating_add(other.prepare);
    attribution.cache_key = attribution.cache_key.saturating_add(other.cache_key);
    attribution.cache_lookup = attribution.cache_lookup.saturating_add(other.cache_lookup);
    attribution.plan_build = attribution.plan_build.saturating_add(other.plan_build);
    attribution.cache_insert = attribution.cache_insert.saturating_add(other.cache_insert);
}

impl<C: CanisterKind> DbSession<C> {
    fn execute_direct_count_rows_global_aggregate<E>(
        &self,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let count = self
            .with_metrics(|| {
                self.load_executor::<E>().execute_scalar_terminal_request(
                    prepared_plan.typed_clone::<E>(),
                    ScalarTerminalBoundaryRequest::Count,
                )
            })
            .map_err(QueryError::execute)?
            .into_count()
            .map_err(QueryError::execute)?;

        Ok(Value::Nat64(u64::from(count)))
    }

    fn execute_direct_count_cardinality_global_aggregate<E>(
        &self,
        authority: EntityAuthority,
        plan: &SqlGlobalAggregateCountPlanCacheEntry,
    ) -> Result<Option<Value>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let output = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_direct_count_index_prefix_cardinality_request(
                        authority,
                        None,
                        plan.prefix_specs(),
                    )
            })
            .map_err(QueryError::execute)?;
        let Some(output) = output else {
            return Ok(None);
        };
        let count = output.into_count().map_err(QueryError::execute)?;

        Ok(Some(Value::Nat64(u64::from(count))))
    }

    fn execute_direct_count_cardinality_probe<E>(
        &self,
        projection: &ProjectionSpec,
        probe: Option<DirectCountCardinalityPlanProbe>,
    ) -> Result<DirectCountCardinalityProbeResolution, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let Some(probe) = probe else {
            return Ok(DirectCountCardinalityProbeResolution {
                result: None,
                fallback_authority: None,
            });
        };
        let DirectCountCardinalityPlanProbe { authority, entry } = probe;
        if let Some(count_plan) = &entry
            && let Some(value) = self.execute_direct_count_cardinality_global_aggregate::<E>(
                authority.clone(),
                count_plan,
            )?
        {
            return Ok(DirectCountCardinalityProbeResolution {
                result: Some(direct_count_rows_statement_result(
                    projection,
                    value,
                    SqlCacheAttribution::none(),
                )),
                fallback_authority: None,
            });
        }

        Ok(DirectCountCardinalityProbeResolution {
            result: None,
            fallback_authority: Some(authority),
        })
    }

    fn direct_count_cardinality_plan_entry_for_accepted_authority(
        authority: &EntityAuthority,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        visible_indexes: &VisibleIndexes<'_>,
        schema_info: &SchemaInfo,
    ) -> Result<Option<Arc<SqlGlobalAggregateCountPlanCacheEntry>>, QueryError> {
        Ok(direct_count_cardinality_plan_entry_from_prefix_specs(
            catalog,
            Self::direct_count_cardinality_prefix_specs_for_accepted_authority(
                authority,
                command.query(),
                visible_indexes,
                schema_info,
            )?,
        ))
    }

    fn build_direct_count_cardinality_plan_probe<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<Option<DirectCountCardinalityPlanProbe>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok(None);
        }

        let authority = catalog
            .accepted_entity_authority_for::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info_for::<E>();
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let visible_indexes = Self::visible_indexes_for_accepted_schema(&schema_info, visibility);
        let entry = Self::direct_count_cardinality_plan_entry_for_accepted_authority(
            &authority,
            command,
            catalog,
            &visible_indexes,
            &schema_info,
        );

        Ok(Some(DirectCountCardinalityPlanProbe {
            authority,
            entry: entry?,
        }))
    }

    fn resolve_compiled_direct_count_cardinality_plan<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<Option<DirectCountCardinalityPlanProbe>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok(None);
        }
        if let Some(entry) = compiled.cached_global_aggregate_count_plan(
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
        ) {
            return direct_count_cardinality_probe_from_entry::<E>(catalog, entry).map(Some);
        }

        let Some(probe) = self.build_direct_count_cardinality_plan_probe::<E>(command, catalog)?
        else {
            return Ok(None);
        };
        if let Some(entry) = &probe.entry {
            compiled.set_cached_global_aggregate_count_plan(Arc::clone(entry));
        }

        Ok(Some(probe))
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_compiled_direct_count_cardinality_plan_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            Option<DirectCountCardinalityPlanProbe>,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let mut attribution = QueryPlanCompilePhaseAttribution::default();
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok((None, attribution));
        }

        let (cache_lookup, cached_plan) = measure_sql_stage(|| {
            compiled.cached_global_aggregate_count_plan(
                catalog.fingerprint_method_version(),
                catalog.fingerprint(),
            )
        });
        attribution.cache_lookup = attribution.cache_lookup.saturating_add(cache_lookup);
        if let Some(entry) = cached_plan {
            return Ok((
                Some(direct_count_cardinality_probe_from_entry::<E>(
                    catalog, entry,
                )?),
                attribution,
            ));
        }

        let authority = catalog
            .accepted_entity_authority_for::<E>()
            .map_err(QueryError::execute)?;
        let (schema_info_local, schema_info) =
            measure_sql_stage(|| catalog.accepted_schema_info_for::<E>());
        attribution.schema_info = attribution.schema_info.saturating_add(schema_info_local);
        let (schema_info_local, visibility) =
            measure_sql_stage(|| self.query_plan_visibility_for_store_path(authority.store_path()));
        attribution.schema_info = attribution.schema_info.saturating_add(schema_info_local);
        let visibility = visibility?;
        let (schema_info_local, visible_indexes) = measure_sql_stage(|| {
            Self::visible_indexes_for_accepted_schema(&schema_info, visibility)
        });
        attribution.schema_info = attribution.schema_info.saturating_add(schema_info_local);
        let (plan_build_local, entry) = measure_sql_stage(|| {
            Self::direct_count_cardinality_plan_entry_for_accepted_authority(
                &authority,
                command,
                catalog,
                &visible_indexes,
                &schema_info,
            )
        });
        attribution.plan_build = attribution.plan_build.saturating_add(plan_build_local);
        let entry = entry?;
        if let Some(entry) = &entry {
            let (cache_insert, ()) = measure_sql_stage(|| {
                compiled.set_cached_global_aggregate_count_plan(Arc::clone(entry));
            });
            attribution.cache_insert = attribution.cache_insert.saturating_add(cache_insert);
        }

        Ok((
            Some(DirectCountCardinalityPlanProbe { authority, entry }),
            attribution,
        ))
    }

    fn execute_global_aggregate_with_prepared_plan<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        prepared_plan: &SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let projection = command.projection();

        if command.facts().is_direct_count_rows() {
            let value = self.execute_direct_count_rows_global_aggregate::<E>(prepared_plan)?;

            return Ok(direct_count_rows_statement_result(
                projection,
                value,
                cache_attribution,
            ));
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
            sql_projection_statement_result_from_value_rows(columns, fixed_scales, rows, row_count),
            cache_attribution,
        ))
    }

    fn resolve_compiled_global_aggregate_prepared_plan<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> Result<(SharedPreparedExecutionPlan, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(prepared_plan) = compiled.cached_global_aggregate_plan(
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
        ) {
            return Ok((
                prepared_plan,
                SqlCacheAttribution::shared_query_plan_cache_hit(),
            ));
        }

        let authority = match authority {
            Some(authority) => authority,
            None => catalog
                .accepted_entity_authority_for::<E>()
                .map_err(QueryError::execute)?,
        };
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority,
                catalog.snapshot(),
                catalog.fingerprint(),
                command.query(),
            )?;
        compiled.set_cached_global_aggregate_plan(
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
            prepared_plan.clone(),
        );

        Ok((
            prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlCacheAttribution,
            QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(prepared_plan) = compiled.cached_global_aggregate_plan(
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
        ) {
            return Ok((
                prepared_plan,
                SqlCacheAttribution::shared_query_plan_cache_hit(),
                QueryPlanCompilePhaseAttribution::default(),
            ));
        }

        let authority = match authority {
            Some(authority) => authority,
            None => catalog
                .accepted_entity_authority_for::<E>()
                .map_err(QueryError::execute)?,
        };
        let (prepared_plan, cache_attribution, plan_compile_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint_and_compile_phase_attribution(
                authority,
                catalog.snapshot(),
                catalog.fingerprint(),
                command.query(),
            )?;
        compiled.set_cached_global_aggregate_plan(
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
            prepared_plan.clone(),
        );

        Ok((
            prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
            plan_compile_attribution,
        ))
    }

    // Execute one borrowed prepared SQL aggregate command through executor-owned
    // structural aggregate execution after resolving the accepted catalog.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_ref<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let direct_probe = self.build_direct_count_cardinality_plan_probe::<E>(command, catalog)?;
        let direct_resolution =
            self.execute_direct_count_cardinality_probe::<E>(command.projection(), direct_probe)?;
        if let Some(result) = direct_resolution.result {
            return Ok(result);
        }

        let authority = match direct_resolution.fallback_authority {
            Some(authority) => authority,
            None => catalog
                .accepted_entity_authority_for::<E>()
                .map_err(QueryError::execute)?,
        };
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority,
                catalog.snapshot(),
                catalog.fingerprint(),
                command.query(),
            )?;

        self.execute_global_aggregate_with_prepared_plan::<E>(
            command,
            catalog,
            &prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let direct_probe =
            self.resolve_compiled_direct_count_cardinality_plan::<E>(compiled, command, catalog)?;
        let direct_resolution =
            self.execute_direct_count_cardinality_probe::<E>(command.projection(), direct_probe)?;
        if let Some(result) = direct_resolution.result {
            return Ok(result);
        }

        let (prepared_plan, cache_attribution) = self
            .resolve_compiled_global_aggregate_prepared_plan::<E>(
                compiled,
                command,
                catalog,
                direct_resolution.fallback_authority,
            )?;

        self.execute_global_aggregate_with_prepared_plan::<E>(
            command,
            catalog,
            &prepared_plan,
            cache_attribution,
        )
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let mut direct_fallback_execute_local_instructions = 0_u64;
        let mut direct_fallback_store_local_instructions = 0_u64;
        let (direct_probe, direct_plan_compile_attribution) = self
            .resolve_compiled_direct_count_cardinality_plan_with_phase_attribution::<E>(
                compiled, command, catalog,
            )?;
        let direct_fallback_plan_compile_attribution = direct_plan_compile_attribution;
        let direct_authority = if let Some(probe) = direct_probe {
            if let Some(count_plan) = &probe.entry {
                let (
                    scalar_aggregate_terminal,
                    ((execute_local_instructions, store_local_instructions), result),
                ) = with_scalar_aggregate_terminal_attribution(|| {
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_direct_count_cardinality_global_aggregate::<E>(
                            probe.authority.clone(),
                            count_plan,
                        )
                    })
                });
                if let Some(value) = result? {
                    let result = direct_count_rows_statement_result(
                        command.projection(),
                        value,
                        SqlCacheAttribution::none(),
                    );
                    let mut phase_attribution =
                        SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                            execute_local_instructions,
                            store_local_instructions,
                        );
                    apply_plan_compile_attribution_to_execute_phase(
                        &mut phase_attribution,
                        direct_plan_compile_attribution,
                    );
                    phase_attribution.scalar_aggregate_terminal = scalar_aggregate_terminal;

                    return Ok((result.0, result.1, phase_attribution));
                }

                direct_fallback_execute_local_instructions = execute_local_instructions;
                direct_fallback_store_local_instructions = store_local_instructions;
            }

            Some(probe.authority)
        } else {
            None
        };

        let (prepared_plan, cache_attribution, mut plan_compile_attribution) = self
            .resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution::<E>(
                compiled,
                command,
                catalog,
                direct_authority,
            )?;
        merge_plan_compile_attribution(
            &mut plan_compile_attribution,
            direct_fallback_plan_compile_attribution,
        );
        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = with_scalar_aggregate_terminal_attribution(|| {
            measure_execute_phase_with_physical_access(|| {
                self.execute_global_aggregate_with_prepared_plan::<E>(
                    command,
                    catalog,
                    &prepared_plan,
                    cache_attribution,
                )
            })
        });
        let (result, cache_attribution) = result?;
        let mut phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
            execute_local_instructions.saturating_add(direct_fallback_execute_local_instructions),
            store_local_instructions.saturating_add(direct_fallback_store_local_instructions),
        );
        apply_plan_compile_attribution_to_execute_phase(
            &mut phase_attribution,
            plan_compile_attribution,
        );
        phase_attribution.scalar_aggregate_terminal = scalar_aggregate_terminal;

        Ok((result, cache_attribution, phase_attribution))
    }
}
