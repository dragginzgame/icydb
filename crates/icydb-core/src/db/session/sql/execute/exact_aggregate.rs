//! Module: db::session::sql::execute::exact_aggregate
//! Responsibility: exact SQL global-aggregate metadata selection and execution.
//! Does not own: global aggregate orchestration or prepared aggregate execution.
//! Boundary: exposes target/outcome contracts consumed by the global aggregate adapter.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            exact_count_cardinality_prefixes_for_plan, execute_exact_cardinality_for_canister,
            execute_exact_indexed_numeric_aggregate_for_canister,
            user_index_prefix_cardinality_keys_from_plan,
        },
        index::{IndexId, IndexKey, RawIndexStoreKey, UserIndexPrefixCardinalityKey},
        query::plan::{exact_first_component_metadata_index, expr::ProjectionSpec},
        schema::AcceptedFieldKind,
        session::{
            AcceptedSchemaCatalogContext,
            query::{
                StructuralProjectionContract,
                exact_count_cardinality_prefix_keys_for_accepted_authority,
            },
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledSchemaFingerprint,
                SqlGlobalAggregateCachedPlan, SqlGlobalAggregatePlanCacheEntry, SqlStatementResult,
                projection::sql_projection_statement_result_from_value_rows,
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;
use std::{ops::Bound, rc::Rc};

#[cfg(feature = "diagnostics")]
use super::diagnostics::measure_scalar_aggregate_execute_phase_with_physical_access;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::measure_sql_stage;
#[cfg(feature = "diagnostics")]
use crate::db::session::{
    query::QueryPlanCompilePhaseAttribution, sql::SqlExecutePhaseAttribution,
};

pub(super) enum ExactCountTarget {
    Disabled,
    FallbackOnly(EntityAuthority),
    PreparedPlan(Rc<SqlGlobalAggregatePlanCacheEntry>),
    CountPlan {
        authority: EntityAuthority,
        entry: Rc<SqlGlobalAggregatePlanCacheEntry>,
        cache_attribution: SqlCacheAttribution,
    },
}

pub(super) enum ExactCountOutcome {
    Direct {
        result: SqlStatementResult,
        cache_attribution: SqlCacheAttribution,
        #[cfg(feature = "diagnostics")]
        phase_attribution: Option<Box<SqlExecutePhaseAttribution>>,
    },
    Prepared {
        prepared_plan: SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    },
    Fallback {
        authority: Option<EntityAuthority>,
        #[cfg(feature = "diagnostics")]
        execute_local_instructions: u64,
        #[cfg(feature = "diagnostics")]
        store_local_instructions: u64,
    },
}

fn exact_aggregate_statement_result(
    catalog: &AcceptedSchemaCatalogContext,
    projection: &ProjectionSpec,
    row: Vec<Value>,
    cache_attribution: SqlCacheAttribution,
) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
    let (columns, fixed_scales) =
        StructuralProjectionContract::from_projection_spec(projection).into_components();

    Ok((
        sql_projection_statement_result_from_value_rows(
            catalog.enum_catalog(),
            columns,
            fixed_scales,
            std::iter::once(row),
            1,
        )?,
        cache_attribution,
    ))
}

impl ExactCountTarget {
    fn from_optional_entry(
        authority: EntityAuthority,
        entry: Option<Rc<SqlGlobalAggregatePlanCacheEntry>>,
        cache_attribution: SqlCacheAttribution,
    ) -> Self {
        match entry {
            Some(entry) => Self::CountPlan {
                authority,
                entry,
                cache_attribution,
            },
            None => Self::FallbackOnly(authority),
        }
    }

    const fn count_plan_entry(&self) -> Option<&Rc<SqlGlobalAggregatePlanCacheEntry>> {
        match self {
            Self::CountPlan { entry, .. } => Some(entry),
            Self::Disabled | Self::FallbackOnly(_) | Self::PreparedPlan(_) => None,
        }
    }
}

impl ExactCountOutcome {
    const fn disabled() -> Self {
        Self::Fallback {
            authority: None,
            #[cfg(feature = "diagnostics")]
            execute_local_instructions: 0,
            #[cfg(feature = "diagnostics")]
            store_local_instructions: 0,
        }
    }

    const fn fallback(authority: EntityAuthority) -> Self {
        Self::Fallback {
            authority: Some(authority),
            #[cfg(feature = "diagnostics")]
            execute_local_instructions: 0,
            #[cfg(feature = "diagnostics")]
            store_local_instructions: 0,
        }
    }

    fn from_direct_row(
        catalog: &AcceptedSchemaCatalogContext,
        projection: &ProjectionSpec,
        row: Vec<Value>,
        cache_attribution: SqlCacheAttribution,
    ) -> Result<Self, QueryError> {
        let (result, cache_attribution) =
            exact_aggregate_statement_result(catalog, projection, row, cache_attribution)?;

        Ok(Self::Direct {
            result,
            cache_attribution,
            #[cfg(feature = "diagnostics")]
            phase_attribution: None,
        })
    }

    #[cfg(feature = "diagnostics")]
    const fn measured_fallback(
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

    #[cfg(feature = "diagnostics")]
    const fn measured_direct(
        result: SqlStatementResult,
        cache_attribution: SqlCacheAttribution,
        phase_attribution: Box<SqlExecutePhaseAttribution>,
    ) -> Self {
        Self::Direct {
            result,
            cache_attribution,
            phase_attribution: Some(phase_attribution),
        }
    }
}

fn direct_count_cardinality_plan_entry_from_prefix_keys(
    catalog: &AcceptedSchemaCatalogContext,
    prefix_keys: Option<Vec<UserIndexPrefixCardinalityKey>>,
) -> Option<Rc<SqlGlobalAggregatePlanCacheEntry>> {
    let prefix_keys = prefix_keys?;
    if prefix_keys.is_empty() {
        return None;
    }

    Some(Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        SqlGlobalAggregateCachedPlan::exact_user_index_prefixes(Rc::from(prefix_keys)),
    )))
}

fn direct_count_cardinality_entity_plan_entry(
    catalog: &AcceptedSchemaCatalogContext,
) -> Rc<SqlGlobalAggregatePlanCacheEntry> {
    Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        SqlGlobalAggregateCachedPlan::exact_entity_cardinality(),
    ))
}

fn exact_first_component_plan_entry(
    catalog: &AcceptedSchemaCatalogContext,
    index_id: IndexId,
    numeric: bool,
) -> Rc<SqlGlobalAggregatePlanCacheEntry> {
    let plan = if numeric {
        SqlGlobalAggregateCachedPlan::ExactUserIndexFirstComponentNumeric(index_id)
    } else {
        SqlGlobalAggregateCachedPlan::exact_user_index_first_component_distinct(index_id)
    };
    Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
        SqlCompiledSchemaFingerprint::from_catalog(catalog),
        plan,
    ))
}

fn direct_count_cardinality_prefix_keys_from_planned_query(
    prepared_plan: &SharedPreparedExecutionPlan,
) -> Option<Vec<UserIndexPrefixCardinalityKey>> {
    let plan = prepared_plan.logical_plan();
    let prefix_plan = exact_count_cardinality_prefixes_for_plan(
        prepared_plan.authority_ref().entity_tag(),
        plan,
        prepared_plan.index_prefix_specs(),
        true,
    )?;

    user_index_prefix_cardinality_keys_from_plan(prefix_plan)
}

fn direct_count_cardinality_range_from_planned_query(
    prepared_plan: &SharedPreparedExecutionPlan,
) -> Option<SqlGlobalAggregateCachedPlan> {
    let plan = prepared_plan.logical_plan();
    if plan.has_any_residual_filter() {
        return None;
    }
    let semantic = plan.access.as_index_range_path()?;
    let selected = semantic.index();
    if !semantic.prefix_values().is_empty() || selected.is_filtered() {
        return None;
    }
    let [lowered] = prepared_plan.index_range_specs() else {
        return None;
    };

    let authority = prepared_plan.authority_ref();
    let schema = authority.accepted_schema_info()?;
    let accepted = schema
        .field_path_indexes()
        .iter()
        .find(|index| index.ordinal() == selected.ordinal())?;
    let first = accepted.fields().first()?;
    if semantic.field_slots() != [0]
        || first.persisted_kind() != Some(&AcceptedFieldKind::Int32)
        || accepted.fields().iter().any(|field| {
            field.path().len() != 1
                || schema.accepted_field_is_nullable(field.field_name()) != Some(false)
        })
    {
        return None;
    }

    let index_id = IndexId::new_with_generation(
        authority.entity_tag(),
        selected.ordinal(),
        selected.physical_generation(),
    );
    let lower = encoded_component_bound(semantic.lower(), lowered.lower(), index_id)?;
    let upper = encoded_component_bound(semantic.upper(), lowered.upper(), index_id)?;

    Some(
        SqlGlobalAggregateCachedPlan::ExactUserIndexFirstComponentRange {
            index_id,
            lower,
            upper,
        },
    )
}

fn encoded_component_bound(
    semantic: &Bound<Value>,
    raw: &Bound<RawIndexStoreKey>,
    index_id: IndexId,
) -> Option<Bound<Vec<u8>>> {
    let included = match semantic {
        Bound::Unbounded => return Some(Bound::Unbounded),
        Bound::Included(_) => true,
        Bound::Excluded(_) => false,
    };
    let raw = match raw {
        Bound::Included(raw) | Bound::Excluded(raw) => raw,
        Bound::Unbounded => return None,
    };
    let key = IndexKey::try_from_raw(raw).ok()?;
    (*key.index_id() == index_id).then_some(())?;
    let component = key.component(0)?.to_vec();

    Some(if included {
        Bound::Included(component)
    } else {
        Bound::Excluded(component)
    })
}

fn exact_count_target_from_cached_entry(
    catalog: &AcceptedSchemaCatalogContext,
    entry: Rc<SqlGlobalAggregatePlanCacheEntry>,
) -> ExactCountTarget {
    if entry.prepared_plan().is_some() {
        return ExactCountTarget::PreparedPlan(entry);
    }
    let authority = catalog.accepted_entity_authority();

    ExactCountTarget::CountPlan {
        authority,
        entry,
        cache_attribution: SqlCacheAttribution::shared_query_plan_cache_hit(),
    }
}

fn cached_compiled_global_aggregate_plan_entry(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
) -> Option<Rc<SqlGlobalAggregatePlanCacheEntry>> {
    compiled.cached_global_aggregate_plan(SqlCompiledSchemaFingerprint::from_catalog(catalog))
}

fn cache_compiled_exact_count_target(compiled: &CompiledSqlCommand, target: &ExactCountTarget) {
    if let Some(entry) = target.count_plan_entry() {
        compiled.set_cached_global_aggregate_plan(Rc::clone(entry));
    }
}

fn exact_count_metadata_candidate(command: &SqlGlobalAggregateCommand) -> bool {
    command
        .facts()
        .is_direct_count_cardinality_metadata_candidate()
        || command.exact_distinct_cardinality_target().is_some()
        || command.exact_indexed_numeric_target().is_some()
}

impl<C: CanisterKind> DbSession<C> {
    fn execute_exact_global_aggregate(
        &self,
        command: &SqlGlobalAggregateCommand,
        authority: EntityAuthority,
        entry: &SqlGlobalAggregatePlanCacheEntry,
    ) -> Result<Option<Vec<Value>>, QueryError> {
        if let Some(target) = entry.exact_cardinality_target() {
            let count = self
                .with_metrics(|| {
                    execute_exact_cardinality_for_canister(
                        &self.db,
                        authority,
                        DiagnosticExecutionLane::TrustedRead,
                        target,
                    )
                })
                .map_err(QueryError::execute)?;

            return Ok(count.map(|count| vec![Value::Nat64(count)]));
        }
        let Some(index_id) = entry.exact_indexed_numeric_target() else {
            return Err(QueryError::invariant());
        };
        let output_kinds = command
            .exact_indexed_numeric_output_kinds()
            .ok_or_else(QueryError::invariant)?;

        self.with_metrics(|| {
            execute_exact_indexed_numeric_aggregate_for_canister(
                &self.db,
                authority,
                DiagnosticExecutionLane::TrustedRead,
                index_id,
                &output_kinds,
            )
        })
        .map_err(QueryError::execute)
    }

    pub(super) fn execute_exact_count_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        target: ExactCountTarget,
    ) -> Result<ExactCountOutcome, QueryError> {
        match target {
            ExactCountTarget::Disabled => Ok(ExactCountOutcome::disabled()),
            ExactCountTarget::FallbackOnly(authority) => Ok(ExactCountOutcome::fallback(authority)),
            ExactCountTarget::PreparedPlan(entry) => {
                let Some(prepared_plan) = entry.prepared_plan() else {
                    return Err(QueryError::invariant());
                };

                Ok(ExactCountOutcome::Prepared {
                    prepared_plan,
                    cache_attribution: SqlCacheAttribution::shared_query_plan_cache_hit(),
                })
            }
            ExactCountTarget::CountPlan {
                authority,
                entry,
                cache_attribution,
            } => {
                if let Some(row) =
                    self.execute_exact_global_aggregate(command, authority.clone(), &entry)?
                {
                    return ExactCountOutcome::from_direct_row(
                        catalog,
                        command.projection(),
                        row,
                        cache_attribution,
                    );
                }

                Ok(ExactCountOutcome::fallback(authority))
            }
        }
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn execute_measured_exact_count_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        target: ExactCountTarget,
        plan_compile_attribution: QueryPlanCompilePhaseAttribution,
    ) -> Result<ExactCountOutcome, QueryError> {
        let (authority, count_plan, cache_attribution) = match target {
            ExactCountTarget::Disabled => {
                return Ok(ExactCountOutcome::disabled());
            }
            ExactCountTarget::FallbackOnly(authority) => {
                return Ok(ExactCountOutcome::fallback(authority));
            }
            ExactCountTarget::PreparedPlan(entry) => {
                let Some(prepared_plan) = entry.prepared_plan() else {
                    return Err(QueryError::invariant());
                };

                return Ok(ExactCountOutcome::Prepared {
                    prepared_plan,
                    cache_attribution: SqlCacheAttribution::shared_query_plan_cache_hit(),
                });
            }
            ExactCountTarget::CountPlan {
                authority,
                entry,
                cache_attribution,
            } => (authority, entry, cache_attribution),
        };
        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = measure_scalar_aggregate_execute_phase_with_physical_access(|| {
            self.execute_exact_global_aggregate(command, authority.clone(), &count_plan)
        });
        if let Some(row) = result? {
            let (result, cache_attribution) = exact_aggregate_statement_result(
                catalog,
                command.projection(),
                row,
                cache_attribution,
            )?;
            let phase_attribution =
                SqlExecutePhaseAttribution::from_query_plan_execute_total_and_store_total(
                    plan_compile_attribution.planner_local_instructions(),
                    plan_compile_attribution,
                    execute_local_instructions,
                    store_local_instructions,
                )
                .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

            return Ok(ExactCountOutcome::measured_direct(
                result,
                cache_attribution,
                Box::new(phase_attribution),
            ));
        }

        Ok(ExactCountOutcome::measured_fallback(
            authority,
            execute_local_instructions,
            store_local_instructions,
        ))
    }

    fn exact_count_shortcut_target_for_authority(
        &self,
        authority: &EntityAuthority,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactCountTarget, QueryError> {
        let Some(schema_info) = authority.accepted_schema_info() else {
            return Err(QueryError::invariant());
        };
        let exact_numeric = command.exact_indexed_numeric_target().is_some();
        if exact_numeric || command.exact_distinct_cardinality_target().is_some() {
            let target = (if exact_numeric {
                command.exact_indexed_numeric_target()
            } else {
                command.exact_distinct_cardinality_target()
            })
            .map(crate::db::query::plan::FieldSlot::field)
            .ok_or_else(QueryError::invariant)?;
            let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
            let visible_indexes =
                Self::visible_indexes_for_accepted_schema(schema_info, visibility);
            let entry = exact_first_component_metadata_index(&visible_indexes, schema_info, target)
                .map(|index| {
                    let index_id = IndexId::new_with_generation(
                        authority.entity_tag(),
                        index.ordinal(),
                        index.physical_generation(),
                    );
                    exact_first_component_plan_entry(catalog, index_id, exact_numeric)
                });

            return Ok(ExactCountTarget::from_optional_entry(
                authority.clone(),
                entry,
                SqlCacheAttribution::none(),
            ));
        }
        if command.query().direct_count_cardinality_entity_candidate() {
            return Ok(ExactCountTarget::from_optional_entry(
                authority.clone(),
                Some(direct_count_cardinality_entity_plan_entry(catalog)),
                SqlCacheAttribution::none(),
            ));
        }
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let visible_indexes = Self::visible_indexes_for_accepted_schema(schema_info, visibility);
        let entry = direct_count_cardinality_plan_entry_from_prefix_keys(
            catalog,
            exact_count_cardinality_prefix_keys_for_accepted_authority(
                authority,
                command.query(),
                &visible_indexes,
                schema_info,
            )?,
        );

        Ok(ExactCountTarget::from_optional_entry(
            authority.clone(),
            entry,
            SqlCacheAttribution::none(),
        ))
    }

    fn exact_count_target_from_cached_shared_plan(
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
        prepared_plan: &SharedPreparedExecutionPlan,
        cache_attribution: SqlCacheAttribution,
    ) -> ExactCountTarget {
        let entry = direct_count_cardinality_plan_entry_from_prefix_keys(
            catalog,
            direct_count_cardinality_prefix_keys_from_planned_query(prepared_plan),
        )
        .or_else(|| {
            direct_count_cardinality_range_from_planned_query(prepared_plan).map(|plan| {
                Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
                    SqlCompiledSchemaFingerprint::from_catalog(catalog),
                    plan,
                ))
            })
        });

        ExactCountTarget::from_optional_entry(authority, entry, cache_attribution)
    }

    fn exact_count_target_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> Result<ExactCountTarget, QueryError> {
        let shortcut =
            self.exact_count_shortcut_target_for_authority(&authority, command, catalog)?;
        if shortcut.count_plan_entry().is_some() {
            return Ok(shortcut);
        }

        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                command.query(),
                DiagnosticExecutionLane::TrustedRead,
            )?;

        Ok(Self::exact_count_target_from_cached_shared_plan(
            catalog,
            authority,
            &prepared_plan,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
    }

    fn build_exact_count_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactCountTarget, QueryError> {
        if !exact_count_metadata_candidate(command) {
            return Ok(ExactCountTarget::Disabled);
        }

        let authority = catalog.accepted_entity_authority();
        self.exact_count_target_for_authority(command, catalog, authority)
    }

    pub(super) fn resolve_compiled_exact_count_target(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactCountTarget, QueryError> {
        if let Some(entry) = cached_compiled_global_aggregate_plan_entry(compiled, catalog) {
            return Ok(exact_count_target_from_cached_entry(catalog, entry));
        }
        if !exact_count_metadata_candidate(command) {
            return Ok(ExactCountTarget::Disabled);
        }

        let target = self.build_exact_count_target(command, catalog)?;
        cache_compiled_exact_count_target(compiled, &target);

        Ok(target)
    }

    #[cfg(feature = "diagnostics")]
    pub(super) fn resolve_compiled_exact_count_target_with_phase_attribution(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(ExactCountTarget, QueryPlanCompilePhaseAttribution), QueryError> {
        let mut attribution = QueryPlanCompilePhaseAttribution::default();
        let (cache_lookup, cached_plan) =
            measure_sql_stage(|| cached_compiled_global_aggregate_plan_entry(compiled, catalog));
        attribution.cache_lookup = attribution.cache_lookup.saturating_add(cache_lookup);
        if let Some(plan) = cached_plan {
            return Ok((
                exact_count_target_from_cached_entry(catalog, plan),
                attribution,
            ));
        }
        if !exact_count_metadata_candidate(command) {
            return Ok((ExactCountTarget::Disabled, attribution));
        }

        let authority = catalog.accepted_entity_authority();
        let (schema_info_local, shortcut) = measure_sql_stage(|| {
            self.exact_count_shortcut_target_for_authority(&authority, command, catalog)
        });
        attribution.schema_info = attribution.schema_info.saturating_add(schema_info_local);
        let shortcut = shortcut?;
        let target = if shortcut.count_plan_entry().is_some() {
            shortcut
        } else {
            let (prepared_plan, cache_attribution, compile_attribution) = self
                .cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                    authority.clone(),
                    catalog,
                    command.query(),
                    DiagnosticExecutionLane::TrustedRead,
                )?;
            attribution.merge(compile_attribution);

            Self::exact_count_target_from_cached_shared_plan(
                catalog,
                authority,
                &prepared_plan,
                SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
            )
        };
        if target.count_plan_entry().is_some() {
            let (cache_insert, ()) = measure_sql_stage(|| {
                cache_compiled_exact_count_target(compiled, &target);
            });
            attribution.cache_insert = attribution.cache_insert.saturating_add(cache_insert);
        }

        Ok((target, attribution))
    }
}
