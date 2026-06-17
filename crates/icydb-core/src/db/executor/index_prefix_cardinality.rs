//! Module: executor::index_prefix_cardinality
//! Responsibility: executor-local probes over lowered index-prefix metadata.
//! Does not own: count planning or index metadata maintenance.
//! Boundary: fail-open helpers for runtime branch pruning.

use crate::db::{
    access::LoweredIndexPrefixSpec,
    data::DataStore,
    index::{IndexId, IndexKey, IndexKeyKind, IndexStore},
    registry::StoreHandle,
};
use std::ops::Bound;

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
    let Some(index_id) = exact_cardinality_index_id_from_lowered_spec(spec) else {
        return false;
    };

    index_store
        .exact_prefix_cardinality(
            data_generation,
            IndexKeyKind::User,
            index_id,
            spec.prefix_components(),
        )
        .is_some_and(|count| count == 0)
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
