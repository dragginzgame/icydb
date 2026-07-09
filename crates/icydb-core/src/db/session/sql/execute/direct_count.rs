//! Module: db::session::sql::execute::direct_count
//! Responsibility: direct SQL COUNT(*) row and index-prefix cardinality execution.
//! Does not own: global aggregate orchestration or non-cardinality aggregate execution.
//! Boundary: exposes target/outcome contracts consumed by the global aggregate adapter.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        access::{
            LoweredIndexPrefixCardinalitySpec, lower_access,
            lower_exact_index_prefix_cardinality_specs_for_prefix_access,
        },
        executor::{
            EntityAuthority, ScalarTerminalBoundaryRequest, SharedPreparedExecutionPlan,
            exact_count_cardinality_prefixes_for_plan,
            lowered_index_prefix_cardinality_specs_from_plan,
        },
        query::{
            intent::StructuralQuery,
            plan::{AccessPlannedQuery, VisibleIndexes, expr::ProjectionSpec},
        },
        schema::SchemaInfo,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledSchemaFingerprint,
                SqlGlobalAggregateCountPlanCacheEntry, SqlStatementResult,
                projection::{
                    projection_contract_from_projection_spec,
                    sql_projection_statement_result_from_value_rows,
                },
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};
use std::rc::Rc;

#[cfg(feature = "diagnostics")]
use super::diagnostics::measure_scalar_aggregate_execute_phase_with_physical_access;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::measure_sql_stage;
#[cfg(feature = "diagnostics")]
use crate::db::session::{
    query::QueryPlanCompilePhaseAttribution, sql::SqlExecutePhaseAttribution,
};

pub(super) enum DirectCountCardinalityTarget {
    Disabled,
    FallbackOnly(EntityAuthority),
    CountPlan {
        authority: EntityAuthority,
        entry: Rc<SqlGlobalAggregateCountPlanCacheEntry>,
    },
}

pub(super) enum DirectCountCardinalityOutcome {
    Direct(SqlStatementResult, SqlCacheAttribution),
    Fallback { authority: Option<EntityAuthority> },
}

#[cfg(feature = "diagnostics")]
pub(super) enum MeasuredDirectCountCardinalityOutcome {
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

struct DirectCountCardinalityPlanInput {
    authority: EntityAuthority,
    schema_info: SchemaInfo,
    visible_indexes: VisibleIndexes<'static>,
}

pub(super) fn direct_count_rows_statement_result(
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

impl DirectCountCardinalityTarget {
    fn from_optional_entry(
        authority: EntityAuthority,
        entry: Option<Rc<SqlGlobalAggregateCountPlanCacheEntry>>,
    ) -> Self {
        match entry {
            Some(entry) => Self::CountPlan { authority, entry },
            None => Self::FallbackOnly(authority),
        }
    }

    const fn count_plan_entry(&self) -> Option<&Rc<SqlGlobalAggregateCountPlanCacheEntry>> {
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

fn direct_count_cardinality_plan_entry_from_prefix_specs(
    catalog: &AcceptedSchemaCatalogContext,
    prefix_specs: Option<Vec<LoweredIndexPrefixCardinalitySpec>>,
) -> Option<Rc<SqlGlobalAggregateCountPlanCacheEntry>> {
    let prefix_specs = prefix_specs?;
    if prefix_specs.is_empty() {
        return None;
    }

    Some(Rc::new(SqlGlobalAggregateCountPlanCacheEntry::new(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        Rc::from(prefix_specs),
    )))
}

pub(in crate::db::session::sql::execute) fn direct_count_cardinality_prefix_specs_for_accepted_authority(
    authority: &EntityAuthority,
    query: &StructuralQuery,
    visible_indexes: &VisibleIndexes<'_>,
    schema_info: &SchemaInfo,
) -> Result<Option<Vec<LoweredIndexPrefixCardinalitySpec>>, QueryError> {
    if let Some(access) = query
        .try_build_count_cardinality_prefix_access_with_schema_info(visible_indexes, schema_info)?
    {
        let prefix_specs = lower_exact_index_prefix_cardinality_specs_for_prefix_access(
            authority.entity_tag(),
            &access,
        )
        .map_err(|_err| QueryError::invariant())?;
        if !prefix_specs.is_empty() {
            return Ok(Some(prefix_specs));
        }
    }

    let plan = query.build_plan_with_visible_indexes(visible_indexes)?;

    direct_count_cardinality_prefix_specs_from_planned_query(authority, &plan)
}

fn direct_count_cardinality_prefix_specs_from_planned_query(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<Option<Vec<LoweredIndexPrefixCardinalitySpec>>, QueryError> {
    let lowered_access = lower_access(authority.entity_tag(), &plan.access)
        .map_err(|_err| QueryError::invariant())?;
    let Some(prefix_plan) = exact_count_cardinality_prefixes_for_plan(
        authority.entity_tag(),
        plan,
        lowered_access.index_prefix_specs(),
        true,
    ) else {
        return Ok(None);
    };

    Ok(lowered_index_prefix_cardinality_specs_from_plan(
        prefix_plan,
    ))
}

fn direct_count_cardinality_target_from_entry<E>(
    catalog: &AcceptedSchemaCatalogContext,
    entry: Rc<SqlGlobalAggregateCountPlanCacheEntry>,
) -> Result<DirectCountCardinalityTarget, QueryError>
where
    E: PersistedRow + EntityValue,
{
    let authority = catalog
        .accepted_entity_authority_for::<E>()
        .map_err(QueryError::execute)?;

    Ok(DirectCountCardinalityTarget::CountPlan { authority, entry })
}

fn cached_compiled_direct_count_cardinality_entry(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
) -> Option<Rc<SqlGlobalAggregateCountPlanCacheEntry>> {
    compiled.cached_global_aggregate_count_plan(SqlCompiledSchemaFingerprint::from_catalog(catalog))
}

fn cache_compiled_direct_count_cardinality_target(
    compiled: &CompiledSqlCommand,
    target: &DirectCountCardinalityTarget,
) {
    if let Some(entry) = target.count_plan_entry() {
        compiled.set_cached_global_aggregate_count_plan(Rc::clone(entry));
    }
}

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn execute_direct_count_rows_global_aggregate<E>(
        &self,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> Result<Value, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let count = self
            .with_metrics(|| {
                self.load_executor::<E>().execute_scalar_terminal_request(
                    prepared_plan.typed_clone::<E>()?,
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

    pub(super) fn execute_direct_count_cardinality_target<E>(
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
    pub(super) fn execute_measured_direct_count_cardinality_target<E>(
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
        ) = measure_scalar_aggregate_execute_phase_with_physical_access(|| {
            self.execute_direct_count_cardinality_global_aggregate::<E>(
                authority.clone(),
                &count_plan,
            )
        });
        if let Some(value) = result? {
            let (result, cache_attribution) =
                direct_count_rows_statement_result(projection, value, SqlCacheAttribution::none());
            let phase_attribution =
                SqlExecutePhaseAttribution::from_query_plan_execute_total_and_store_total(
                    plan_compile_attribution.planner_local_instructions(),
                    plan_compile_attribution,
                    execute_local_instructions,
                    store_local_instructions,
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
    ) -> Result<Option<Rc<SqlGlobalAggregateCountPlanCacheEntry>>, QueryError> {
        Ok(direct_count_cardinality_plan_entry_from_prefix_specs(
            catalog,
            direct_count_cardinality_prefix_specs_for_accepted_authority(
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

    pub(super) fn build_direct_count_cardinality_target<E>(
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

    pub(super) fn resolve_compiled_direct_count_cardinality_target<E>(
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
        if let Some(entry) = cached_compiled_direct_count_cardinality_entry(compiled, catalog) {
            return direct_count_cardinality_target_from_entry::<E>(catalog, entry);
        }

        let target = self.build_direct_count_cardinality_target::<E>(command, catalog)?;
        cache_compiled_direct_count_cardinality_target(compiled, &target);

        Ok(target)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn resolve_compiled_direct_count_cardinality_target_with_phase_attribution<E>(
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

        let (cache_lookup, cached_plan) =
            measure_sql_stage(|| cached_compiled_direct_count_cardinality_entry(compiled, catalog));
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
        if target.count_plan_entry().is_some() {
            let (cache_insert, ()) = measure_sql_stage(|| {
                cache_compiled_direct_count_cardinality_target(compiled, &target);
            });
            attribution.cache_insert = attribution.cache_insert.saturating_add(cache_insert);
        }

        Ok((target, attribution))
    }
}
