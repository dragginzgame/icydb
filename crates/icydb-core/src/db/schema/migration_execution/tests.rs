use super::*;
use crate::{
    db::{
        data::{DataStore, DecodedDataStoreKey},
        index::IndexStore,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreAllocationIdentities, StoreRuntimeStorageCapabilities},
        schema::SchemaStore,
    },
    types::EntityTag,
};
use icydb_schema::SchemaMigrationPlanDigest;
use std::cell::RefCell;

#[test]
fn migration_row_preflight_requires_present_source_rows() {
    thread_local! {
        static DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    }
    let store = StoreHandle::new(
        &DATA,
        &INDEX,
        &SCHEMA,
        StoreAllocationIdentities::absent(),
        StoreRuntimeStorageCapabilities::heap(),
    );
    let key = DecodedDataStoreKey::new(
        EntityTag::new(7),
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(11)),
    )
    .to_raw()
    .unwrap();
    let records = [JournalRecord::schema_migration_row_put(
        "test::Store",
        key.clone(),
        vec![1],
        [0; 16],
        SchemaMigrationPlanDigest::from_bytes([7; 32]),
    )
    .unwrap()];
    for present in [false, true, false] {
        DATA.with_borrow_mut(|data| {
            if present {
                data.insert_raw_for_test(key.clone(), RawRow::try_new(vec![2]).unwrap())
                    .expect("fixture storage operation should succeed");
            } else {
                data.remove(&key)
                    .expect("fixture storage operation should succeed");
            }
        });
        let result = preflight_migration_effects("test::Store", store, &records);
        if present {
            result.unwrap();
        } else {
            assert_eq!(
                result.unwrap_err().diagnostic(),
                InternalError::store_corruption().diagnostic(),
            );
        }
    }
}

#[test]
fn migration_page_limits_qualify_every_indivisible_rewrite_resource() {
    let capacity = [
        MAX_MIGRATION_REWRITE_ROW_BYTES_PER_PAGE.min(MAX_MIGRATION_FINAL_VALIDATION_BYTES_PER_PAGE),
        MAX_MIGRATION_REWRITE_INDEX_BYTES_PER_PAGE,
        MAX_JOURNAL_BATCH_RECORDS,
        MAX_MIGRATION_REWRITE_JOURNAL_BYTES_PER_PAGE,
    ];
    let fits = |[rows, indexes, effects, journal]: [usize; 4]| {
        migration_rewrite_row_fits_full_page(rows, indexes, effects, journal)
    };
    assert!(fits(capacity));
    for resource in 0..capacity.len() {
        let mut usage = capacity;
        usage[resource] += 1;
        assert!(!fits(usage), "resource={resource}");
        usage[resource] = usize::MAX;
        assert!(!fits(usage), "resource={resource}");
    }
}
