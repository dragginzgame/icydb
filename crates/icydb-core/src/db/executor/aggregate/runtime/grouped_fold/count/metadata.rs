//! Grouped COUNT(*) ingestion from bounded, exact index-prefix metadata.
//! Reuses the selected access proof and ordinary grouped page finalization.

use crate::{
    db::{
        access::{LoweredIndexPrefixSpec, MAX_INDEX_BRANCH_SET_VALUES},
        data::DataStore,
        executor::{
            aggregate::{
                GroupError, capability::accepted_field_kind_has_identity_group_canonical_form,
                runtime::grouped_fold::count::finalize::finalize_grouped_count_page,
            },
            budget::{ExecutionConstructionBudget, charge_current_execution_budget},
            group::{GroupKey, grouped_execution_context_from_planner_config},
            index_prefix_cardinality::exact_count_cardinality_prefixes_for_plan,
            pipeline::contracts::{GroupedCursorPage, GroupedRouteStage},
        },
        index::IndexKeyKind,
        query::construction::ConstructionBudget,
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Read one count per selected prefix, without opening the token row stream.
/// Missing metadata leaves the ordinary grouped executor authoritative.
pub(in crate::db::executor) fn try_execute_grouped_count_metadata(
    store: StoreHandle,
    entity_tag: EntityTag,
    route: &GroupedRouteStage,
) -> Result<Option<GroupedCursorPage>, InternalError> {
    let Some(values) = grouped_count_prefix_values(route) else {
        return Ok(None);
    };
    let Some(prefixes) = exact_count_cardinality_prefixes_for_plan(
        entity_tag,
        route.plan(),
        route.index_prefix_specs(),
        true,
    ) else {
        return Ok(None);
    };
    if prefixes.prefix_len() != 1 || prefixes.specs().len() != values.len() {
        return Ok(None);
    }
    charge_current_execution_budget(Resource::KeyIndexEntriesVisited, values.len() as u64)?;
    let budget: &dyn ConstructionBudget = &ExecutionConstructionBudget;
    // The storage batch retains prefix references and one count per prefix.
    budget.charge(
        Resource::TemporaryBytes,
        (values.len() as u64).saturating_mul((size_of::<&[Vec<u8>]>() + size_of::<u64>()) as u64),
    )?;
    let Some(counts) = store.exact_user_index_prefix_counts(
        store.with_data(DataStore::generation),
        IndexKeyKind::User,
        prefixes.index_id(),
        prefixes
            .specs()
            .iter()
            .map(LoweredIndexPrefixSpec::prefix_components),
    ) else {
        return Ok(None);
    };
    let mut context =
        grouped_execution_context_from_planner_config(Some(route.grouped_execution()));
    let mut groups = budget.vec_with_capacity(values.len())?;
    // Lowering preserves the selected path's value/spec order. Membership
    // normalization has already removed duplicate values before this boundary.
    for (value, count) in values.iter().zip(counts) {
        if count == 0 {
            continue;
        }
        let key = GroupKey::from_single_canonical_group_value(budget.copy_value(value)?)?;
        context
            .record_new_group(groups.len(), groups.capacity(), &key)
            .map_err(GroupError::into_internal_error)?;
        // Preserve the maintained dedicated count fold's saturating Nat32 state.
        groups.push((key, u32::try_from(count).unwrap_or(u32::MAX)));
    }
    let projection = route.plan().frozen_projection_spec()?;
    let (rows, next_cursor) = finalize_grouped_count_page(route, projection, groups)?;
    Ok(Some(GroupedCursorPage { rows, next_cursor }))
}

// Only identity-canonical direct grouping can recover output keys from access
// literals. Residual predicates and incomplete indexes remain planner-owned.
fn grouped_count_prefix_values(route: &GroupedRouteStage) -> Option<&[Value]> {
    if !route
        .grouped_execution_route()
        .uses_count_rows_dedicated_fold()
    {
        return None;
    }
    let [field] = route.group_fields().as_direct()? else {
        return None;
    };
    if !field
        .accepted_kind()
        .is_some_and(accepted_field_kind_has_identity_group_canonical_form)
    {
        return None;
    }
    let path = route.plan().access.as_path()?;
    let (index, values) = path
        .as_index_multi_lookup_contract()
        .or_else(|| path.as_index_prefix_contract())?;
    if index.key_field_at(0) != Some(field.field())
        || index.is_filtered()
        || values.is_empty()
        || values.len() > MAX_INDEX_BRANCH_SET_VALUES
    {
        return None;
    }
    Some(values)
}
