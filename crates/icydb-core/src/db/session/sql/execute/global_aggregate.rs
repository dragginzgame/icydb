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
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledSchemaFingerprint,
                SqlGlobalAggregateCountPlanCacheEntry, SqlProjectionContract, SqlStatementResult,
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

enum DirectCountCardinalityTarget {
    Disabled,
    FallbackOnly(EntityAuthority),
    CountPlan {
        authority: EntityAuthority,
        entry: Arc<SqlGlobalAggregateCountPlanCacheEntry>,
    },
}

enum DirectCountCardinalityOutcome {
    Direct(SqlStatementResult, SqlCacheAttribution),
    Fallback { authority: Option<EntityAuthority> },
}

#[cfg(feature = "diagnostics")]
enum MeasuredDirectCountCardinalityOutcome {
    Direct {
        result: SqlStatementResult,
        cache_attribution: SqlCacheAttribution,
        phase_attribution: Box<SqlExecutePhaseAttribution>,
    },
    Fallback {
        authority: Option<EntityAuthority>,
        execute_local_instructions: u64,
        store_local_instructions: u64,
    },
}

struct PreparedAggregateRequestBundle {
    request: StructuralAggregateRequest,
    projection: SqlProjectionContract,
}

struct DirectCountCardinalityPlanInput {
    authority: EntityAuthority,
    schema_info: SchemaInfo,
    visible_indexes: VisibleIndexes<'static>,
}

struct ResolvedGlobalAggregatePreparedPlan {
    prepared_plan: SharedPreparedExecutionPlan,
    cache_attribution: SqlCacheAttribution,
}

type PreparedAggregatePlanResolution = Result<ResolvedGlobalAggregatePreparedPlan, QueryError>;
#[cfg(feature = "diagnostics")]
type MeasuredPreparedAggregatePlanResolution = Result<
    (
        ResolvedGlobalAggregatePreparedPlan,
        QueryPlanCompilePhaseAttribution,
    ),
    QueryError,
>;

impl DirectCountCardinalityTarget {
    fn from_optional_entry(
        authority: EntityAuthority,
        entry: Option<Arc<SqlGlobalAggregateCountPlanCacheEntry>>,
    ) -> Self {
        match entry {
            Some(entry) => Self::CountPlan { authority, entry },
            None => Self::FallbackOnly(authority),
        }
    }

    const fn count_plan_entry(&self) -> Option<&Arc<SqlGlobalAggregateCountPlanCacheEntry>> {
        match self {
            Self::CountPlan { entry, .. } => Some(entry),
            Self::Disabled | Self::FallbackOnly(_) => None,
        }
    }
}

impl DirectCountCardinalityOutcome {
    const fn disabled() -> Self {
        Self::Fallback { authority: None }
    }

    const fn fallback(authority: EntityAuthority) -> Self {
        Self::Fallback {
            authority: Some(authority),
        }
    }

    fn from_direct_value(projection: &ProjectionSpec, value: Value) -> Self {
        let (result, cache_attribution) =
            direct_count_rows_statement_result(projection, value, SqlCacheAttribution::none());

        Self::Direct(result, cache_attribution)
    }
}

#[cfg(feature = "diagnostics")]
impl MeasuredDirectCountCardinalityOutcome {
    const fn disabled() -> Self {
        Self::Fallback {
            authority: None,
            execute_local_instructions: 0,
            store_local_instructions: 0,
        }
    }

    const fn fallback(
        authority: EntityAuthority,
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self::Fallback {
            authority: Some(authority),
            execute_local_instructions,
            store_local_instructions,
        }
    }
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

impl DirectCountCardinalityPlanInput {
    const fn new(
        authority: EntityAuthority,
        schema_info: SchemaInfo,
        visible_indexes: VisibleIndexes<'static>,
    ) -> Self {
        Self {
            authority,
            schema_info,
            visible_indexes,
        }
    }
}

impl ResolvedGlobalAggregatePreparedPlan {
    const fn new(
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        Self {
            prepared_plan,
            cache_attribution,
        }
    }

    const fn from_compiled_cache_hit(prepared_plan: SharedPreparedExecutionPlan) -> Self {
        Self::new(
            prepared_plan,
            SqlCacheAttribution::shared_query_plan_cache_hit(),
        )
    }

    const fn from_shared_query_plan_cache(
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: crate::db::session::query::QueryPlanCacheAttribution,
    ) -> Self {
        Self::new(
            prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        )
    }

    fn into_parts(self) -> (SharedPreparedExecutionPlan, SqlCacheAttribution) {
        (self.prepared_plan, self.cache_attribution)
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
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        Arc::from(prefix_specs),
    )))
}

fn direct_count_cardinality_target_from_entry<E>(
    catalog: &AcceptedSchemaCatalogContext,
    entry: Arc<SqlGlobalAggregateCountPlanCacheEntry>,
) -> Result<DirectCountCardinalityTarget, QueryError>
where
    E: PersistedRow + EntityValue,
{
    let authority = catalog
        .accepted_entity_authority_for::<E>()
        .map_err(QueryError::execute)?;

    Ok(DirectCountCardinalityTarget::CountPlan { authority, entry })
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

    fn execute_direct_count_cardinality_target<E>(
        &self,
        projection: &ProjectionSpec,
        target: DirectCountCardinalityTarget,
    ) -> Result<DirectCountCardinalityOutcome, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match target {
            DirectCountCardinalityTarget::Disabled => Ok(DirectCountCardinalityOutcome::disabled()),
            DirectCountCardinalityTarget::FallbackOnly(authority) => {
                Ok(DirectCountCardinalityOutcome::fallback(authority))
            }
            DirectCountCardinalityTarget::CountPlan { authority, entry } => {
                if let Some(value) = self.execute_direct_count_cardinality_global_aggregate::<E>(
                    authority.clone(),
                    &entry,
                )? {
                    return Ok(DirectCountCardinalityOutcome::from_direct_value(
                        projection, value,
                    ));
                }

                Ok(DirectCountCardinalityOutcome::fallback(authority))
            }
        }
    }

    #[cfg(feature = "diagnostics")]
    fn execute_measured_direct_count_cardinality_target<E>(
        &self,
        projection: &ProjectionSpec,
        target: DirectCountCardinalityTarget,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
    ) -> Result<MeasuredDirectCountCardinalityOutcome, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (authority, count_plan) = match target {
            DirectCountCardinalityTarget::Disabled => {
                return Ok(MeasuredDirectCountCardinalityOutcome::disabled());
            }
            DirectCountCardinalityTarget::FallbackOnly(authority) => {
                return Ok(MeasuredDirectCountCardinalityOutcome::fallback(
                    authority, 0, 0,
                ));
            }
            DirectCountCardinalityTarget::CountPlan { authority, entry } => (authority, entry),
        };
        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = with_scalar_aggregate_terminal_attribution(|| {
            measure_execute_phase_with_physical_access(|| {
                self.execute_direct_count_cardinality_global_aggregate::<E>(
                    authority.clone(),
                    &count_plan,
                )
            })
        });
        if let Some(value) = result? {
            let (result, cache_attribution) =
                direct_count_rows_statement_result(projection, value, SqlCacheAttribution::none());
            let phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                execute_local_instructions,
                store_local_instructions,
            )
            .with_query_plan_compile_attribution(
                plan_compile_attribution.planner_local_instructions(),
                plan_compile_attribution,
            )
            .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

            return Ok(MeasuredDirectCountCardinalityOutcome::Direct {
                result,
                cache_attribution,
                phase_attribution: Box::new(phase_attribution),
            });
        }

        Ok(MeasuredDirectCountCardinalityOutcome::fallback(
            authority,
            execute_local_instructions,
            store_local_instructions,
        ))
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

    fn direct_count_cardinality_authority<E>(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<EntityAuthority, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        catalog
            .accepted_entity_authority_for::<E>()
            .map_err(QueryError::execute)
    }

    fn direct_count_cardinality_plan_input_for_authority<E>(
        &self,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> Result<DirectCountCardinalityPlanInput, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema_info = catalog.accepted_schema_info_for::<E>();
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let visible_indexes = Self::visible_indexes_for_accepted_schema(&schema_info, visibility);

        Ok(DirectCountCardinalityPlanInput::new(
            authority,
            schema_info,
            visible_indexes,
        ))
    }

    fn direct_count_cardinality_target_from_plan_input(
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        input: DirectCountCardinalityPlanInput,
    ) -> Result<DirectCountCardinalityTarget, QueryError> {
        let entry = Self::direct_count_cardinality_plan_entry_for_accepted_authority(
            &input.authority,
            command,
            catalog,
            &input.visible_indexes,
            &input.schema_info,
        )?;

        Ok(DirectCountCardinalityTarget::from_optional_entry(
            input.authority,
            entry,
        ))
    }

    fn global_aggregate_prepared_plan_authority<E>(
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> Result<EntityAuthority, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        catalog
            .accepted_or_provided_entity_authority_for::<E>(authority.as_ref())
            .map_err(QueryError::execute)
    }

    fn resolve_global_aggregate_prepared_plan_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> PreparedAggregatePlanResolution {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority,
                catalog,
                command.query(),
            )?;

        Ok(
            ResolvedGlobalAggregatePreparedPlan::from_shared_query_plan_cache(
                prepared_plan,
                cache_attribution,
            ),
        )
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_global_aggregate_prepared_plan_for_authority_with_phase_attribution(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> MeasuredPreparedAggregatePlanResolution {
        let (prepared_plan, cache_attribution, plan_compile_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                authority,
                catalog,
                command.query(),
            )?;

        Ok((
            ResolvedGlobalAggregatePreparedPlan::from_shared_query_plan_cache(
                prepared_plan,
                cache_attribution,
            ),
            plan_compile_attribution,
        ))
    }

    fn build_direct_count_cardinality_target<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<DirectCountCardinalityTarget, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok(DirectCountCardinalityTarget::Disabled);
        }

        let authority = Self::direct_count_cardinality_authority::<E>(catalog)?;
        let input =
            self.direct_count_cardinality_plan_input_for_authority::<E>(catalog, authority)?;

        Self::direct_count_cardinality_target_from_plan_input(command, catalog, input)
    }

    fn resolve_compiled_direct_count_cardinality_target<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<DirectCountCardinalityTarget, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if !command
            .facts()
            .is_direct_count_cardinality_metadata_candidate()
        {
            return Ok(DirectCountCardinalityTarget::Disabled);
        }
        let compiled_schema_fingerprint = SqlCompiledSchemaFingerprint::from_catalog(catalog);
        if let Some(entry) =
            compiled.cached_global_aggregate_count_plan(compiled_schema_fingerprint)
        {
            return direct_count_cardinality_target_from_entry::<E>(catalog, entry);
        }

        let target = self.build_direct_count_cardinality_target::<E>(command, catalog)?;
        if let Some(entry) = target.count_plan_entry() {
            compiled.set_cached_global_aggregate_count_plan(Arc::clone(entry));
        }

        Ok(target)
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_compiled_direct_count_cardinality_target_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            DirectCountCardinalityTarget,
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
            return Ok((DirectCountCardinalityTarget::Disabled, attribution));
        }

        let (cache_lookup, cached_plan) = measure_sql_stage(|| {
            compiled.cached_global_aggregate_count_plan(SqlCompiledSchemaFingerprint::from_catalog(
                catalog,
            ))
        });
        attribution.cache_lookup = attribution.cache_lookup.saturating_add(cache_lookup);
        if let Some(entry) = cached_plan {
            return Ok((
                direct_count_cardinality_target_from_entry::<E>(catalog, entry)?,
                attribution,
            ));
        }

        let authority = Self::direct_count_cardinality_authority::<E>(catalog)?;
        let (schema_info_local, input) = measure_sql_stage(|| {
            self.direct_count_cardinality_plan_input_for_authority::<E>(catalog, authority)
        });
        attribution.schema_info = attribution.schema_info.saturating_add(schema_info_local);
        let input = input?;
        let (plan_build_local, target) = measure_sql_stage(|| {
            Self::direct_count_cardinality_target_from_plan_input(command, catalog, input)
        });
        attribution.plan_build = attribution.plan_build.saturating_add(plan_build_local);
        let target = target?;
        if let Some(entry) = target.count_plan_entry() {
            let (cache_insert, ()) = measure_sql_stage(|| {
                compiled.set_cached_global_aggregate_count_plan(Arc::clone(entry));
            });
            attribution.cache_insert = attribution.cache_insert.saturating_add(cache_insert);
        }

        Ok((target, attribution))
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
    ) -> PreparedAggregatePlanResolution
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled_schema_fingerprint = SqlCompiledSchemaFingerprint::from_catalog(catalog);
        if let Some(prepared_plan) =
            compiled.cached_global_aggregate_plan(compiled_schema_fingerprint)
        {
            return Ok(ResolvedGlobalAggregatePreparedPlan::from_compiled_cache_hit(prepared_plan));
        }

        let authority = Self::global_aggregate_prepared_plan_authority::<E>(catalog, authority)?;
        let resolved =
            self.resolve_global_aggregate_prepared_plan_for_authority(command, catalog, authority)?;
        compiled.set_cached_global_aggregate_plan(
            compiled_schema_fingerprint,
            resolved.prepared_plan.clone(),
        );

        Ok(resolved)
    }

    fn execute_global_aggregate_after_direct_count_target<E>(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        direct_count_target: DirectCountCardinalityTarget,
        resolve_prepared_plan: impl FnOnce(Option<EntityAuthority>) -> PreparedAggregatePlanResolution,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
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
        E: PersistedRow<Canister = C> + EntityValue,
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
        let phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
            execute_local_instructions.saturating_add(direct_execute_local_instructions),
            store_local_instructions.saturating_add(direct_store_local_instructions),
        )
        .with_query_plan_compile_attribution(
            plan_compile_attribution.planner_local_instructions(),
            plan_compile_attribution,
        )
        .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

        Ok((result, cache_attribution, phase_attribution))
    }

    #[cfg(feature = "diagnostics")]
    fn resolve_compiled_global_aggregate_prepared_plan_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: Option<EntityAuthority>,
    ) -> MeasuredPreparedAggregatePlanResolution
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled_schema_fingerprint = SqlCompiledSchemaFingerprint::from_catalog(catalog);
        if let Some(prepared_plan) =
            compiled.cached_global_aggregate_plan(compiled_schema_fingerprint)
        {
            return Ok((
                ResolvedGlobalAggregatePreparedPlan::from_compiled_cache_hit(prepared_plan),
                QueryPlanCompilePhaseAttribution::default(),
            ));
        }

        let authority = Self::global_aggregate_prepared_plan_authority::<E>(catalog, authority)?;
        let (resolved, plan_compile_attribution) = self
            .resolve_global_aggregate_prepared_plan_for_authority_with_phase_attribution(
                command, catalog, authority,
            )?;
        compiled.set_cached_global_aggregate_plan(
            compiled_schema_fingerprint,
            resolved.prepared_plan.clone(),
        );

        Ok((resolved, plan_compile_attribution))
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
        E: PersistedRow<Canister = C> + EntityValue,
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
