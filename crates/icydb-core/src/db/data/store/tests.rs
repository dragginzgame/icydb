use super::*;
use crate::{
    db::{
        data::DecodedDataStoreKey,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    },
    testing::test_memory,
};
use ic_memory::stable_structures::{
    Memory, VectorMemory,
    memory_manager::{MemoryId, MemoryManager},
};
use std::ops::Bound;

const SINGLE_MEMORY_MANAGER_BUCKET_PAGES: u64 = 1 + 128;

fn raw_key(entity: u64, id: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new(
        EntityTag::new(entity),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
    )
    .to_raw()
    .expect("test data key should encode")
}

fn raw_row(value: u8) -> RawRow {
    RawRow::try_new(vec![value]).expect("test raw row should be bounded")
}

fn seed_store(memory_id: u8, entries: &[(u64, u64, u8)]) -> DataStore {
    let mut store = DataStore::init_journaled(test_memory(memory_id));
    for (entity, id, row) in entries {
        store.insert_raw_for_test(raw_key(*entity, *id), raw_row(*row));
    }
    store
}

fn seed_heap_store(entries: &[(u64, u64, u8)]) -> DataStore {
    let mut store = DataStore::init_heap();
    for (entity, id, row) in entries {
        store.insert_raw_for_test(raw_key(*entity, *id), raw_row(*row));
    }
    store
}

#[test]
fn data_store_tiny_fold_stays_within_one_memory_manager_bucket() {
    let memory = VectorMemory::default();
    let manager = MemoryManager::init(memory.clone());
    let mut store = DataStore::init_journaled(manager.get(MemoryId::new(23)));

    store
        .fold_recovered_journal_put(raw_key(1, 1), raw_row(7))
        .expect("tiny folded row should persist");

    assert!(
        memory.size() <= SINGLE_MEMORY_MANAGER_BUCKET_PAGES,
        "tiny data fold should not allocate extra MemoryManager buckets; pages={}",
        memory.size()
    );
}

fn collect_keys(store: &DataStore) -> Vec<RawDataStoreKey> {
    let mut keys = Vec::new();
    let _: Result<(), Infallible> = store.visit_entries(|key, _row| {
        keys.push(key.clone());
        Ok(StoreVisit::Continue)
    });
    keys
}

#[test]
fn data_store_visit_entries_preserves_storage_key_order() {
    let store = seed_store(221, &[(2, 1, 21), (1, 3, 13), (1, 1, 11), (1, 2, 12)]);

    let mut expected = vec![raw_key(2, 1), raw_key(1, 3), raw_key(1, 1), raw_key(1, 2)];
    expected.sort();

    assert_eq!(collect_keys(&store), expected);
}

#[test]
fn data_store_visit_range_preserves_raw_key_bounds() {
    let store = seed_store(222, &[(1, 1, 11), (1, 2, 12), (1, 3, 13), (1, 4, 14)]);

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_range(
        (
            Bound::Included(raw_key(1, 2)),
            Bound::Excluded(raw_key(1, 4)),
        ),
        |key, row| {
            visited.push((key.clone(), row.as_bytes()[0]));
            Ok(StoreVisit::Continue)
        },
    );

    assert_eq!(visited, vec![(raw_key(1, 2), 12), (raw_key(1, 3), 13)]);
}

#[test]
fn data_store_visit_entity_preserves_compact_entity_prefix_bounds() {
    let store = seed_store(223, &[(2, 1, 21), (1, 2, 12), (2, 3, 23), (1, 1, 11)]);

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_entity(EntityTag::new(2), |key, row| {
        visited.push((key.clone(), row.as_bytes()[0]));
        Ok(StoreVisit::Continue)
    });

    assert_eq!(visited, vec![(raw_key(2, 1), 21), (raw_key(2, 3), 23)]);
}

#[test]
fn data_store_entity_cardinality_tracks_heap_writes() {
    let mut store = seed_heap_store(&[(1, 1, 11), (1, 2, 12), (2, 1, 21)]);

    assert_eq!(store.exact_entity_count(EntityTag::new(1)), Some(2));
    assert_eq!(store.exact_entity_count(EntityTag::new(2)), Some(1));
    assert_eq!(store.exact_entity_count(EntityTag::new(3)), Some(0));

    store.insert_raw_for_test(raw_key(1, 2), raw_row(22));
    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        Some(2),
        "replacing a row must not change entity cardinality",
    );

    store.insert_raw_for_test(raw_key(1, 3), raw_row(13));
    assert_eq!(store.exact_entity_count(EntityTag::new(1)), Some(3));

    let removed = store.remove(&raw_key(1, 1));
    assert!(removed.is_some());
    assert_eq!(store.exact_entity_count(EntityTag::new(1)), Some(2));

    let removed = store.remove(&raw_key(1, 99));
    assert!(removed.is_none());
    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        Some(2),
        "removing a missing row must not change entity cardinality",
    );
}

#[test]
fn data_store_entity_cardinality_tracks_journaled_overlay_writes() {
    let mut store = DataStore::init_journaled(test_memory(226));
    store
        .fold_recovered_journal_put(raw_key(1, 1), raw_row(11))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 3), raw_row(13))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(2, 1), raw_row(21))
        .expect("canonical seed should fold");
    assert_eq!(store.exact_entity_count(EntityTag::new(1)), Some(2));
    assert_eq!(store.exact_entity_count(EntityTag::new(2)), Some(1));

    store
        .apply_recovered_journal_put(raw_key(1, 0), raw_row(10))
        .expect("live put should apply");
    store
        .apply_recovered_journal_put(raw_key(1, 3), raw_row(33))
        .expect("live replacement should apply");
    store
        .apply_recovered_journal_delete(&raw_key(1, 1))
        .expect("live delete should apply");
    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        Some(2),
        "one insert, one replacement, and one delete should leave two entity rows",
    );

    store
        .reset_journaled_live_projection()
        .expect("projection reset should succeed");
    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        Some(2),
        "projection reset should rebuild counts from canonical rows",
    );
}

#[test]
fn data_store_entity_cardinality_ignores_fold_updates_hidden_by_live_overlay() {
    let mut store = DataStore::init_journaled(test_memory(227));
    store
        .fold_recovered_journal_put(raw_key(1, 1), raw_row(11))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 2), raw_row(12))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 3), raw_row(13))
        .expect("canonical seed should fold");

    store
        .apply_recovered_journal_put(raw_key(1, 2), raw_row(22))
        .expect("live replacement should apply");
    store
        .apply_recovered_journal_delete(&raw_key(1, 3))
        .expect("live delete should apply");
    assert_eq!(store.exact_entity_count(EntityTag::new(1)), Some(2));

    store
        .fold_recovered_journal_delete(&raw_key(1, 2))
        .expect("hidden canonical delete should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 3), raw_row(33))
        .expect("hidden canonical put should fold");

    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        Some(2),
        "folding canonical rows hidden by live rows or tombstones must not change visible counts",
    );
}

#[test]
fn data_store_entity_cardinality_fails_closed_for_malformed_raw_keys() {
    let mut store = seed_heap_store(&[(1, 1, 11)]);
    store.insert_raw_for_test(
        RawDataStoreKey::from_persisted_bytes(vec![1, 2, 3]),
        raw_row(99),
    );

    assert_eq!(
        store.exact_entity_count(EntityTag::new(1)),
        None,
        "malformed raw keys should disable entity-cardinality metadata",
    );
}

#[test]
fn data_store_visit_entries_can_stop_without_error() {
    let store = seed_store(224, &[(1, 1, 11), (1, 2, 12), (1, 3, 13)]);

    let mut visited = Vec::new();
    let _: Result<(), Infallible> = store.visit_entries(|key, _row| {
        visited.push(key.clone());
        Ok(if visited.len() == 2 {
            StoreVisit::Stop
        } else {
            StoreVisit::Continue
        })
    });

    assert_eq!(visited, vec![raw_key(1, 1), raw_key(1, 2)]);
}

#[test]
fn heap_data_store_preserves_order_bounds_and_early_stop() {
    let store = seed_heap_store(&[(2, 1, 21), (1, 3, 13), (1, 1, 11), (1, 2, 12)]);

    let mut expected = vec![raw_key(2, 1), raw_key(1, 3), raw_key(1, 1), raw_key(1, 2)];
    expected.sort();
    assert_eq!(collect_keys(&store), expected);

    let mut ranged = Vec::new();
    let _: Result<(), Infallible> = store.visit_range(
        (
            Bound::Included(raw_key(1, 1)),
            Bound::Excluded(raw_key(1, 3)),
        ),
        |key, row| {
            ranged.push((key.clone(), row.as_bytes()[0]));
            Ok(StoreVisit::Continue)
        },
    );
    assert_eq!(ranged, vec![(raw_key(1, 1), 11), (raw_key(1, 2), 12)]);

    let mut entity = Vec::new();
    let _: Result<(), Infallible> = store.visit_entity(EntityTag::new(2), |key, row| {
        entity.push((key.clone(), row.as_bytes()[0]));
        Ok(StoreVisit::Continue)
    });
    assert_eq!(entity, vec![(raw_key(2, 1), 21)]);

    let mut stopped = Vec::new();
    let _: Result<(), Infallible> = store.visit_entries(|key, _| {
        stopped.push(key.clone());
        Ok(if stopped.len() == 2 {
            StoreVisit::Stop
        } else {
            StoreVisit::Continue
        })
    });
    assert_eq!(stopped, vec![raw_key(1, 1), raw_key(1, 2)]);
}

#[test]
fn journaled_mixed_data_range_traversal_streams_without_snapshot() {
    let mut store = DataStore::init_journaled(test_memory(225));
    store
        .fold_recovered_journal_put(raw_key(1, 1), raw_row(11))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 3), raw_row(13))
        .expect("canonical seed should fold");
    store
        .fold_recovered_journal_put(raw_key(1, 5), raw_row(15))
        .expect("canonical seed should fold");
    store
        .apply_recovered_journal_put(raw_key(1, 0), raw_row(10))
        .expect("live put should apply");
    store
        .apply_recovered_journal_put(raw_key(1, 4), raw_row(14))
        .expect("live put should apply");
    store
        .apply_recovered_journal_put(raw_key(1, 5), raw_row(55))
        .expect("live override should apply");
    store
        .apply_recovered_journal_delete(&raw_key(1, 1))
        .expect("live delete should apply");

    let mut asc = Vec::new();
    let _: Result<(), Infallible> = store.visit_range(
        (
            Bound::Included(raw_key(1, 0)),
            Bound::Included(raw_key(1, 5)),
        ),
        |key, row| {
            asc.push((key.clone(), row.as_bytes()[0]));
            Ok(if asc.len() == 2 {
                StoreVisit::Stop
            } else {
                StoreVisit::Continue
            })
        },
    );
    assert_eq!(asc, vec![(raw_key(1, 0), 10), (raw_key(1, 3), 13)]);

    let mut desc = Vec::new();
    let _: Result<(), Infallible> = store.visit_range_rev(
        (
            Bound::Included(raw_key(1, 0)),
            Bound::Included(raw_key(1, 5)),
        ),
        |key, row| {
            desc.push((key.clone(), row.as_bytes()[0]));
            Ok(if desc.len() == 2 {
                StoreVisit::Stop
            } else {
                StoreVisit::Continue
            })
        },
    );
    assert_eq!(desc, vec![(raw_key(1, 5), 55), (raw_key(1, 4), 14)]);
}
