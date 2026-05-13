use super::{
    FusedSecondaryCoveringAuthority, read_row_presence_with_consistency_from_data_store,
    read_row_presence_with_consistency_from_store, with_row_check_metrics,
};

use crate::{
    db::{
        data::{DataKey, DataStore, RawRow},
        index::IndexStore,
        predicate::MissingRowPolicy,
        registry::StoreHandle,
        schema::SchemaStore,
    },
    testing::test_memory,
    types::EntityTag,
    value::StorageKey,
};

use std::cell::RefCell;

thread_local! {
    static TEST_RUNTIME_CONTEXT_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(171)));
    static TEST_RUNTIME_CONTEXT_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(172)));
    static TEST_RUNTIME_CONTEXT_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init(test_memory(173)));
}

fn test_key() -> DataKey {
    DataKey::new(EntityTag::new(17), StorageKey::Nat(41))
}

fn reset_test_store() {
    let raw_key = test_key().to_raw().expect("test key should encode");
    let raw_row = RawRow::try_new(vec![0xAA]).expect("test raw row should encode");

    TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow_mut(|store| {
        store.clear();
        let _ = store.insert_raw_for_test(raw_key, raw_row);
    });
}

fn test_store_handle() -> StoreHandle {
    StoreHandle::new(
        &TEST_RUNTIME_CONTEXT_DATA_STORE,
        &TEST_RUNTIME_CONTEXT_INDEX_STORE,
        &TEST_RUNTIME_CONTEXT_SCHEMA_STORE,
    )
}

#[test]
fn row_check_metrics_distinguish_borrowed_data_store_probes() {
    reset_test_store();
    let key = test_key();

    let (row_exists, metrics) = with_row_check_metrics(|| {
        TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
            read_row_presence_with_consistency_from_data_store(store, &key, MissingRowPolicy::Error)
                .expect("borrowed row-presence probe should succeed")
        })
    });

    assert!(row_exists, "borrowed probe should find the inserted row");
    assert_eq!(metrics.row_presence_probe_count, 1);
    assert_eq!(metrics.row_presence_probe_hits, 1);
    assert_eq!(metrics.row_presence_probe_misses, 0);
    assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 1);
    assert_eq!(metrics.row_presence_probe_store_handle_count, 0);
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}

#[test]
fn row_check_metrics_distinguish_store_handle_probes() {
    reset_test_store();
    let key = test_key();

    let (row_exists, metrics) = with_row_check_metrics(|| {
        read_row_presence_with_consistency_from_store(
            test_store_handle(),
            &key,
            MissingRowPolicy::Error,
        )
        .expect("store-handle row-presence probe should succeed")
    });

    assert!(
        row_exists,
        "store-handle probe should find the inserted row"
    );
    assert_eq!(metrics.row_presence_probe_count, 1);
    assert_eq!(metrics.row_presence_probe_hits, 1);
    assert_eq!(metrics.row_presence_probe_misses, 0);
    assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 0);
    assert_eq!(metrics.row_presence_probe_store_handle_count, 1);
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}

#[test]
fn fused_secondary_covering_authority_tracks_candidate_and_probe_metrics() {
    reset_test_store();

    let (row_exists, metrics) = with_row_check_metrics(|| {
        TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
            FusedSecondaryCoveringAuthority::new(store, EntityTag::new(17), MissingRowPolicy::Error)
                .admits_storage_key(StorageKey::Nat(41))
                .expect("fused secondary covering probe should succeed")
        })
    });

    assert!(
        row_exists,
        "fused secondary covering probe should find the inserted row"
    );
    assert_eq!(metrics.row_check_covering_candidates_seen, 1);
    assert_eq!(metrics.row_presence_probe_count, 1);
    assert_eq!(metrics.row_presence_probe_hits, 1);
    assert_eq!(metrics.row_presence_probe_misses, 0);
    assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 1);
    assert_eq!(metrics.row_presence_probe_store_handle_count, 0);
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}
