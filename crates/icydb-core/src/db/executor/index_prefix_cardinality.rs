//! Module: executor::index_prefix_cardinality
//! Responsibility: executor-local probes over lowered index-prefix metadata.
//! Does not own: count planning or index metadata maintenance.
//! Boundary: fail-open helpers for runtime branch pruning.

use crate::db::{
    access::{ExecutionPathPayload, LoweredAccess, LoweredIndexPrefixSpec},
    data::DataStore,
    index::{IndexId, IndexKey, IndexKeyKind, IndexStore},
    query::plan::AccessPlannedQuery,
    registry::StoreHandle,
};
use crate::value::Value;
use std::ops::Bound;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::executor) struct LoweredIndexPrefixCardinalityKey {
    index_id: IndexId,
    prefix_components: Vec<Vec<u8>>,
}

impl LoweredIndexPrefixCardinalityKey {
    #[must_use]
    pub(in crate::db::executor) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db::executor) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
    }
}

/// Return one fail-open empty-prefix bitmap for a lowered branch-prefix set.
#[must_use]
pub(in crate::db::executor) fn lowered_index_prefix_empty_bitmap(
    store: StoreHandle,
    specs: &[LoweredIndexPrefixSpec],
) -> Vec<bool> {
    let data_generation = store.with_data(DataStore::generation);
    store.with_index(|index_store| {
        specs
            .iter()
            .map(|spec| lowered_index_prefix_is_proven_empty(index_store, data_generation, spec))
            .collect()
    })
}

fn lowered_index_prefix_is_proven_empty(
    index_store: &IndexStore,
    data_generation: u64,
    spec: &LoweredIndexPrefixSpec,
) -> bool {
    let Some(cardinality_key) =
        lowered_index_prefix_cardinality_key(spec, spec.prefix_components().len())
    else {
        return false;
    };

    index_store
        .exact_prefix_cardinality(
            data_generation,
            IndexKeyKind::User,
            cardinality_key.index_id(),
            cardinality_key.prefix_components(),
        )
        .is_some_and(|count| count == 0)
}

pub(in crate::db::executor) fn lowered_index_prefix_cardinality_key(
    spec: &LoweredIndexPrefixSpec,
    prefix_len: usize,
) -> Option<LoweredIndexPrefixCardinalityKey> {
    if prefix_len == 0 {
        return None;
    }

    let index_id = exact_cardinality_index_id_from_lowered_spec(spec)?;
    let prefix_components = spec.prefix_components().get(..prefix_len)?.to_vec();

    Some(LoweredIndexPrefixCardinalityKey {
        index_id,
        prefix_components,
    })
}

pub(in crate::db::executor) fn exact_count_cardinality_prefixes_for_plan(
    plan: &AccessPlannedQuery,
    lowered_access: &LoweredAccess<'_, Value>,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
) -> Option<Vec<LoweredIndexPrefixCardinalityKey>> {
    if !plan.has_no_distinct()
        || plan.scalar_plan().order.is_some()
        || plan.has_residual_filter_expr()
        || plan.has_residual_filter_predicate()
    {
        return None;
    }

    let path = lowered_access.executable().as_path()?;
    let index = path.index_prefix_details()?;
    let prefix_len = index.slot_arity();
    let expected_prefix_specs = match path {
        ExecutionPathPayload::IndexBranchSet { branch_count, .. } => *branch_count,
        ExecutionPathPayload::IndexPrefix { .. } => 1,
        ExecutionPathPayload::ByKey(_)
        | ExecutionPathPayload::ByKeys(_)
        | ExecutionPathPayload::KeyRange { .. }
        | ExecutionPathPayload::IndexMultiLookup { .. }
        | ExecutionPathPayload::IndexRange { .. }
        | ExecutionPathPayload::FullScan => return None,
    };

    exact_cardinality_prefixes_from_lowered_specs(
        index_prefix_specs,
        expected_prefix_specs,
        prefix_len,
    )
}

fn exact_cardinality_prefixes_from_lowered_specs(
    specs: &[LoweredIndexPrefixSpec],
    expected_prefix_specs: usize,
    prefix_len: usize,
) -> Option<Vec<LoweredIndexPrefixCardinalityKey>> {
    if prefix_len == 0 || specs.len() != expected_prefix_specs {
        return None;
    }

    let mut prefixes = Vec::with_capacity(specs.len());
    for spec in specs {
        prefixes.push(lowered_index_prefix_cardinality_key(spec, prefix_len)?);
    }
    prefixes.sort();
    prefixes.dedup();

    Some(prefixes)
}

fn exact_cardinality_index_id_from_lowered_spec(spec: &LoweredIndexPrefixSpec) -> Option<IndexId> {
    if spec.prefix_components().is_empty() {
        return None;
    }

    let Bound::Included(raw_key) = spec.lower() else {
        return None;
    };
    let key = IndexKey::try_from_raw(raw_key).ok()?;
    if key.key_kind() != IndexKeyKind::User
        || key.component_count() < spec.prefix_components().len()
    {
        return None;
    }

    Some(*key.index_id())
}
