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
                CompiledSqlCommand, SqlCompiledSchemaFingerprint, SqlGlobalAggregateCachedPlan,
                SqlGlobalAggregatePlanCacheEntry, SqlStatementResult,
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

pub(super) enum ExactTarget {
    Disabled,
    FallbackOnly(EntityAuthority),
    PreparedPlan(Rc<SqlGlobalAggregatePlanCacheEntry>),
    ExactPlan {
        authority: EntityAuthority,
        entry: Rc<SqlGlobalAggregatePlanCacheEntry>,
    },
}

pub(super) enum ExactOutcome {
    Direct(SqlStatementResult),
    Prepared(SharedPreparedExecutionPlan),
    Fallback { authority: Option<EntityAuthority> },
}

fn exact_aggregate_statement_result(
    catalog: &AcceptedSchemaCatalogContext,
    projection: &ProjectionSpec,
    row: Vec<Value>,
) -> Result<SqlStatementResult, QueryError> {
    let (columns, fixed_scales) =
        StructuralProjectionContract::from_projection_spec(projection).into_components();

    sql_projection_statement_result_from_value_rows(
        catalog.enum_catalog(),
        columns,
        fixed_scales,
        std::iter::once(row),
        1,
    )
}

impl ExactTarget {
    fn from_optional_entry(
        authority: EntityAuthority,
        entry: Option<Rc<SqlGlobalAggregatePlanCacheEntry>>,
    ) -> Self {
        match entry {
            Some(entry) => Self::ExactPlan { authority, entry },
            None => Self::FallbackOnly(authority),
        }
    }

    const fn exact_plan_entry(&self) -> Option<&Rc<SqlGlobalAggregatePlanCacheEntry>> {
        match self {
            Self::ExactPlan { entry, .. } => Some(entry),
            Self::Disabled | Self::FallbackOnly(_) | Self::PreparedPlan(_) => None,
        }
    }
}

impl ExactOutcome {
    const fn disabled() -> Self {
        Self::Fallback { authority: None }
    }

    const fn fallback(authority: EntityAuthority) -> Self {
        Self::Fallback {
            authority: Some(authority),
        }
    }

    fn from_direct_row(
        catalog: &AcceptedSchemaCatalogContext,
        projection: &ProjectionSpec,
        row: Vec<Value>,
    ) -> Result<Self, QueryError> {
        let result = exact_aggregate_statement_result(catalog, projection, row)?;

        Ok(Self::Direct(result))
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

fn exact_target_from_cached_entry(
    catalog: &AcceptedSchemaCatalogContext,
    entry: Rc<SqlGlobalAggregatePlanCacheEntry>,
) -> ExactTarget {
    if entry.prepared_plan().is_some() {
        return ExactTarget::PreparedPlan(entry);
    }
    let authority = catalog.accepted_entity_authority();

    ExactTarget::ExactPlan { authority, entry }
}

fn cached_compiled_global_aggregate_plan_entry(
    compiled: &CompiledSqlCommand,
    catalog: &AcceptedSchemaCatalogContext,
) -> Option<Rc<SqlGlobalAggregatePlanCacheEntry>> {
    compiled.cached_global_aggregate_plan(SqlCompiledSchemaFingerprint::from_catalog(catalog))
}

fn cache_compiled_exact_target(compiled: &CompiledSqlCommand, target: &ExactTarget) {
    if let Some(entry) = target.exact_plan_entry() {
        compiled.set_cached_global_aggregate_plan(Rc::clone(entry));
    }
}

fn exact_metadata_candidate(command: &SqlGlobalAggregateCommand) -> bool {
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
            let count = execute_exact_cardinality_for_canister(
                &self.db,
                authority,
                DiagnosticExecutionLane::TrustedRead,
                target,
            )
            .map_err(QueryError::execute)?;

            return Ok(count.map(|count| vec![Value::Nat64(count)]));
        }
        let Some(index_id) = entry.exact_indexed_numeric_target() else {
            return Err(QueryError::invariant());
        };
        let output_kinds = command
            .exact_indexed_numeric_output_kinds()
            .ok_or_else(QueryError::invariant)?;

        execute_exact_indexed_numeric_aggregate_for_canister(
            &self.db,
            authority,
            DiagnosticExecutionLane::TrustedRead,
            index_id,
            &output_kinds,
        )
        .map_err(QueryError::execute)
    }

    pub(super) fn execute_exact_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        target: ExactTarget,
    ) -> Result<ExactOutcome, QueryError> {
        match target {
            ExactTarget::Disabled => Ok(ExactOutcome::disabled()),
            ExactTarget::FallbackOnly(authority) => Ok(ExactOutcome::fallback(authority)),
            ExactTarget::PreparedPlan(entry) => {
                let Some(prepared_plan) = entry.prepared_plan() else {
                    return Err(QueryError::invariant());
                };

                Ok(ExactOutcome::Prepared(prepared_plan))
            }
            ExactTarget::ExactPlan { authority, entry } => {
                if let Some(row) =
                    self.execute_exact_global_aggregate(command, authority.clone(), &entry)?
                {
                    return ExactOutcome::from_direct_row(catalog, command.projection(), row);
                }

                Ok(ExactOutcome::fallback(authority))
            }
        }
    }

    fn exact_shortcut_target_for_authority(
        &self,
        authority: &EntityAuthority,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactTarget, QueryError> {
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

            return Ok(ExactTarget::from_optional_entry(authority.clone(), entry));
        }
        if command.query().direct_count_cardinality_entity_candidate() {
            return Ok(ExactTarget::from_optional_entry(
                authority.clone(),
                Some(direct_count_cardinality_entity_plan_entry(catalog)),
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

        Ok(ExactTarget::from_optional_entry(authority.clone(), entry))
    }

    fn exact_target_from_cached_shared_plan(
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> ExactTarget {
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

        ExactTarget::from_optional_entry(authority, entry)
    }

    fn exact_target_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> Result<ExactTarget, QueryError> {
        let shortcut = self.exact_shortcut_target_for_authority(&authority, command, catalog)?;
        if shortcut.exact_plan_entry().is_some() {
            return Ok(shortcut);
        }

        let (prepared_plan, _) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                command.query(),
                DiagnosticExecutionLane::TrustedRead,
            )?;

        Ok(Self::exact_target_from_cached_shared_plan(
            catalog,
            authority,
            &prepared_plan,
        ))
    }

    fn build_exact_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactTarget, QueryError> {
        if !exact_metadata_candidate(command) {
            return Ok(ExactTarget::Disabled);
        }

        let authority = catalog.accepted_entity_authority();
        self.exact_target_for_authority(command, catalog, authority)
    }

    pub(super) fn resolve_compiled_exact_target(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactTarget, QueryError> {
        if let Some(entry) = cached_compiled_global_aggregate_plan_entry(compiled, catalog) {
            return Ok(exact_target_from_cached_entry(catalog, entry));
        }
        if !exact_metadata_candidate(command) {
            return Ok(ExactTarget::Disabled);
        }

        let target = self.build_exact_target(command, catalog)?;
        cache_compiled_exact_target(compiled, &target);

        Ok(target)
    }
}
