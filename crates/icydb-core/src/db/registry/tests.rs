use crate::{
    db::{data::DataStore, index::IndexStore, registry::StoreRegistry, schema::SchemaStore},
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
};
use std::{cell::RefCell, ptr};

const STORE_PATH: &str = "store_registry_tests::Store";
const ALIAS_STORE_PATH: &str = "store_registry_tests::StoreAlias";

thread_local! {
    static TEST_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(151)));
    static TEST_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(152)));
    static TEST_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init(test_memory(153)));
}

fn test_registry() -> StoreRegistry {
    let mut registry = StoreRegistry::new();
    registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
        )
        .expect("test store registration should succeed");
    registry
}

#[test]
fn register_store_binds_data_index_and_schema_handles() {
    let registry = test_registry();
    let handle = registry
        .try_get_store(STORE_PATH)
        .expect("registered store path should resolve");

    assert!(
        ptr::eq(handle.data_store(), &TEST_DATA_STORE),
        "store handle should expose the registered data store accessor"
    );
    assert!(
        ptr::eq(handle.index_store(), &TEST_INDEX_STORE),
        "store handle should expose the registered index store accessor"
    );
    assert!(
        ptr::eq(handle.schema_store(), &TEST_SCHEMA_STORE),
        "store handle should expose the registered schema store accessor"
    );

    let data_rows = handle.with_data(DataStore::len);
    let index_rows = handle.with_index(IndexStore::len);
    assert_eq!(data_rows, 0, "fresh test data store should be empty");
    assert_eq!(index_rows, 0, "fresh test index store should be empty");
}

#[test]
fn missing_store_path_rejected_before_access() {
    let registry = StoreRegistry::new();
    let err = registry
        .try_get_store("store_registry_tests::Missing")
        .expect_err("missing path should fail lookup");

    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("store 'store_registry_tests::Missing' not found"),
        "missing store lookup should include the missing path"
    );
}

#[test]
fn duplicate_store_registration_is_rejected() {
    let mut registry = StoreRegistry::new();
    registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
        )
        .expect("initial store registration should succeed");

    let err = registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
        )
        .expect_err("duplicate registration should fail");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("store 'store_registry_tests::Store' already registered"),
        "duplicate registration should include the conflicting path"
    );
}

#[test]
fn alias_store_registration_reusing_same_store_triplet_is_rejected() {
    let mut registry = StoreRegistry::new();
    registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
        )
        .expect("initial store registration should succeed");

    let err = registry
        .register_store(
            ALIAS_STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
        )
        .expect_err("alias registration reusing the same store triplet should fail");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains(
            "store 'store_registry_tests::StoreAlias' reuses the same row/index/schema store triplet"
        ),
        "alias registration should include conflicting alias path"
    );
    assert!(
        err.message
            .contains("registered as 'store_registry_tests::Store'"),
        "alias registration should include original path"
    );
}
