use super::{
    FusedSecondaryCoveringAuthority, read_row_presence_with_consistency_from_data_store,
    with_row_check_metrics,
};

use crate::{
    db::{
        data::{DataStore, DecodedDataStoreKey, RawRow},
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        predicate::MissingRowPolicy,
    },
    testing::test_memory,
    types::EntityTag,
};

use std::cell::RefCell;

thread_local! {
    static TEST_RUNTIME_CONTEXT_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(171)));
}

fn test_key() -> DecodedDataStoreKey {
    DecodedDataStoreKey::new(
        EntityTag::new(17),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(41)),
    )
}

fn composite_test_key() -> DecodedDataStoreKey {
    let composite = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(41),
        PrimaryKeyComponent::Nat64(7),
    ])
    .expect("test composite key should encode");
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(17),
        &PrimaryKeyValue::Composite(composite),
    )
}

fn reset_test_store() {
    let raw_key = test_key().to_raw().expect("test key should encode");
    let raw_row = RawRow::try_new(vec![0xAA]).expect("test raw row should encode");

    TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow_mut(|store| {
        store.clear();
        let _ = store.insert_raw_for_test(raw_key, raw_row);
    });
}

#[test]
fn row_check_metrics_track_authoritative_data_store_probes() {
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
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}

#[test]
fn fused_secondary_covering_authority_tracks_candidate_and_probe_metrics() {
    reset_test_store();

    let (row_exists, metrics) = with_row_check_metrics(|| {
        TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
            let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(41));
            FusedSecondaryCoveringAuthority::new(store, EntityTag::new(17), MissingRowPolicy::Error)
                .admits_primary_key_value(&primary_key)
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
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}

#[test]
fn fused_secondary_covering_authority_accepts_composite_primary_key_values() {
    let key = composite_test_key();
    let raw_key = key.to_raw().expect("test composite key should encode");
    let raw_row = RawRow::try_new(vec![0xBB]).expect("test raw row should encode");

    TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow_mut(|store| {
        store.clear();
        let _ = store.insert_raw_for_test(raw_key, raw_row);
    });

    let primary_key = key.primary_key_value();
    let (row_exists, metrics) = with_row_check_metrics(|| {
        TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
            FusedSecondaryCoveringAuthority::new(store, EntityTag::new(17), MissingRowPolicy::Error)
                .admits_primary_key_value(&primary_key)
                .expect("fused secondary covering composite probe should succeed")
        })
    });

    assert!(
        row_exists,
        "fused secondary covering probe should find the inserted composite-key row"
    );
    assert_eq!(metrics.row_check_covering_candidates_seen, 1);
    assert_eq!(metrics.row_presence_probe_count, 1);
    assert_eq!(metrics.row_presence_probe_hits, 1);
    assert_eq!(metrics.row_presence_probe_misses, 0);
    assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
}
