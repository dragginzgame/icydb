//! Module: db::session::sql::execute::exact_aggregate
//! Responsibility: exact SQL global-aggregate metadata selection and execution.
//! Does not own: global aggregate orchestration or prepared aggregate execution.
//! Boundary: exposes target/outcome contracts consumed by the global aggregate adapter.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, ExactCardinalityTarget, SharedPreparedExecutionPlan,
            exact_count_cardinality_prefixes_for_plan, execute_exact_cardinality_for_canister,
            execute_exact_indexed_numeric_aggregate_for_canister,
            user_index_prefix_cardinality_keys_from_plan,
        },
        index::{IndexId, IndexKey, RawIndexStoreKey, UserIndexPrefixCardinalityKey},
        query::plan::{exact_first_component_metadata_index, expr::ProjectionSpec},
        schema::AcceptedFieldKind,
        session::{
            AcceptedSchemaCatalogContext,
            query::StructuralProjectionContract,
            sql::{
                SqlStatementResult, projection::sql_projection_statement_result_from_value_rows,
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
    Fallback,
    ExactPlan {
        authority: EntityAuthority,
        entry: Rc<SqlExactAggregatePlan>,
    },
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
        entry: Option<Rc<SqlExactAggregatePlan>>,
    ) -> Self {
        match entry {
            Some(entry) => Self::ExactPlan { authority, entry },
            None => Self::Fallback,
        }
    }

    const fn exact_plan_entry(&self) -> Option<&Rc<SqlExactAggregatePlan>> {
        match self {
            Self::ExactPlan { entry, .. } => Some(entry),
            Self::Fallback => None,
        }
    }
}

fn direct_count_cardinality_plan_entry_from_prefix_keys(
    prefix_keys: Option<Vec<UserIndexPrefixCardinalityKey>>,
) -> Option<Rc<SqlExactAggregatePlan>> {
    let prefix_keys = prefix_keys?;
    if prefix_keys.is_empty() {
        return None;
    }

    Some(Rc::new(SqlExactAggregatePlan::exact_user_index_prefixes(
        Rc::from(prefix_keys),
    )))
}

fn direct_count_cardinality_entity_plan_entry() -> Rc<SqlExactAggregatePlan> {
    Rc::new(SqlExactAggregatePlan::exact_entity_cardinality())
}

fn exact_first_component_plan_entry(index_id: IndexId, numeric: bool) -> Rc<SqlExactAggregatePlan> {
    let plan = if numeric {
        SqlExactAggregatePlan::UserIndexFirstComponentNumeric(index_id)
    } else {
        SqlExactAggregatePlan::exact_user_index_first_component_distinct(index_id)
    };
    Rc::new(plan)
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
) -> Option<SqlExactAggregatePlan> {
    let plan = prepared_plan.logical_plan();
    if plan.has_any_residual_filter().ok()? {
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
    let schema = authority.accepted_schema_info();
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

    Some(SqlExactAggregatePlan::UserIndexFirstComponentRange {
        index_id,
        lower,
        upper,
    })
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
        entry: &SqlExactAggregatePlan,
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
    ) -> Result<Option<SqlStatementResult>, QueryError> {
        match target {
            ExactTarget::Fallback => Ok(None),
            ExactTarget::ExactPlan { authority, entry } => {
                if let Some(row) =
                    self.execute_exact_global_aggregate(command, authority, &entry)?
                {
                    return exact_aggregate_statement_result(catalog, command.projection(), row)
                        .map(Some);
                }

                Ok(None)
            }
        }
    }

    fn exact_shortcut_target_for_authority(
        &self,
        authority: &EntityAuthority,
        command: &SqlGlobalAggregateCommand,
    ) -> Result<ExactTarget, QueryError> {
        let schema_info = authority.accepted_schema_info();
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
                Self::visible_indexes_for_accepted_schema(schema_info, visibility)?;
            let entry = exact_first_component_metadata_index(&visible_indexes, schema_info, target)
                .map(|index| {
                    let index_id = IndexId::new_with_generation(
                        authority.entity_tag(),
                        index.ordinal(),
                        index.physical_generation(),
                    );
                    exact_first_component_plan_entry(index_id, exact_numeric)
                });

            return Ok(ExactTarget::from_optional_entry(authority.clone(), entry));
        }
        if command.query().direct_count_cardinality_entity_candidate() {
            return Ok(ExactTarget::from_optional_entry(
                authority.clone(),
                Some(direct_count_cardinality_entity_plan_entry()),
            ));
        }
        let visibility = self.query_plan_visibility_for_store_path(authority.store_path())?;
        let visible_indexes = Self::visible_indexes_for_accepted_schema(schema_info, visibility)?;
        let entry = direct_count_cardinality_plan_entry_from_prefix_keys(
            self.exact_count_cardinality_prefix_keys_for_accepted_authority(
                authority,
                command.query(),
                &visible_indexes,
                schema_info,
                DiagnosticExecutionLane::TrustedRead,
            )?,
        );

        Ok(ExactTarget::from_optional_entry(authority.clone(), entry))
    }

    fn exact_target_from_cached_shared_plan(
        authority: EntityAuthority,
        prepared_plan: &SharedPreparedExecutionPlan,
    ) -> ExactTarget {
        let entry = direct_count_cardinality_plan_entry_from_prefix_keys(
            direct_count_cardinality_prefix_keys_from_planned_query(prepared_plan),
        )
        .or_else(|| direct_count_cardinality_range_from_planned_query(prepared_plan).map(Rc::new));

        ExactTarget::from_optional_entry(authority, entry)
    }

    fn exact_target_for_authority(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        authority: EntityAuthority,
    ) -> Result<ExactTarget, QueryError> {
        let shortcut = self.exact_shortcut_target_for_authority(&authority, command)?;
        if shortcut.exact_plan_entry().is_some() {
            return Ok(shortcut);
        }

        let prepared_plan = self.cached_shared_query_plan_for_accepted_authority_with_catalog(
            authority.clone(),
            catalog,
            command.query(),
            DiagnosticExecutionLane::TrustedRead,
        )?;

        Ok(Self::exact_target_from_cached_shared_plan(
            authority,
            &prepared_plan,
        ))
    }

    pub(super) fn resolve_compiled_exact_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<ExactTarget, QueryError> {
        if !exact_metadata_candidate(command) {
            return Ok(ExactTarget::Fallback);
        }

        let target =
            self.exact_target_for_authority(command, catalog, catalog.accepted_entity_authority())?;

        Ok(target)
    }
}

// Exact targets are request-local; only the shared weighted owner retains plans.
#[derive(Clone, Debug)]
pub(super) enum SqlExactAggregatePlan {
    EntityCardinality,
    UserIndexFirstComponentDistinct(IndexId),
    UserIndexFirstComponentNumeric(IndexId),
    UserIndexFirstComponentRange {
        index_id: IndexId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    },
    UserIndexPrefixes(Rc<[UserIndexPrefixCardinalityKey]>),
}

impl SqlExactAggregatePlan {
    #[must_use]
    const fn exact_entity_cardinality() -> Self {
        Self::EntityCardinality
    }

    #[must_use]
    const fn exact_user_index_prefixes(prefix_keys: Rc<[UserIndexPrefixCardinalityKey]>) -> Self {
        Self::UserIndexPrefixes(prefix_keys)
    }

    #[must_use]
    const fn exact_user_index_first_component_distinct(index_id: IndexId) -> Self {
        Self::UserIndexFirstComponentDistinct(index_id)
    }

    #[must_use]
    fn exact_cardinality_target(&self) -> Option<ExactCardinalityTarget<'_>> {
        match self {
            Self::EntityCardinality => Some(ExactCardinalityTarget::Entity),
            Self::UserIndexFirstComponentDistinct(index_id) => Some(
                ExactCardinalityTarget::UserIndexFirstComponentDistinct(*index_id),
            ),
            Self::UserIndexFirstComponentRange {
                index_id,
                lower,
                upper,
            } => Some(ExactCardinalityTarget::UserIndexFirstComponentRange {
                index_id: *index_id,
                lower,
                upper,
            }),
            Self::UserIndexPrefixes(prefix_keys) => Some(
                ExactCardinalityTarget::UserIndexPrefixes(prefix_keys.as_ref()),
            ),
            Self::UserIndexFirstComponentNumeric(_) => None,
        }
    }

    #[must_use]
    const fn exact_indexed_numeric_target(&self) -> Option<IndexId> {
        match self {
            Self::UserIndexFirstComponentNumeric(index_id) => Some(*index_id),
            Self::EntityCardinality
            | Self::UserIndexFirstComponentDistinct(_)
            | Self::UserIndexFirstComponentRange { .. }
            | Self::UserIndexPrefixes(_) => None,
        }
    }
}
