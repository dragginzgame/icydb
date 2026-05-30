use crate::{
    db::{
        data::DataStore,
        index::IndexStore,
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
            StoreCommitParticipation, StoreDurability, StoreRecoveryCapability, StoreRegistry,
            StoreRuntimeStorageCapabilities, StoreRuntimeStorageMode,
            StoreSchemaMetadataCapability,
        },
        schema::SchemaStore,
    },
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
    static TEST_HEAP_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static TEST_HEAP_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static TEST_HEAP_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
}

fn test_registry() -> StoreRegistry {
    let mut registry = StoreRegistry::new();
    registry
        .register_store(
            STORE_PATH,
            &TEST_HEAP_DATA_STORE,
            &TEST_HEAP_INDEX_STORE,
            &TEST_HEAP_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("test store registration without allocation identities should succeed");
    registry
}

#[test]
fn register_store_with_absent_allocation_identities_binds_store_handles() {
    let registry = test_registry();
    let handle = registry
        .try_get_store(STORE_PATH)
        .expect("registered store path should resolve");

    assert!(
        ptr::eq(handle.data_store(), &TEST_HEAP_DATA_STORE),
        "store handle should expose the registered data store accessor"
    );
    assert!(
        ptr::eq(handle.index_store(), &TEST_HEAP_INDEX_STORE),
        "store handle should expose the registered index store accessor"
    );
    assert!(
        ptr::eq(handle.schema_store(), &TEST_HEAP_SCHEMA_STORE),
        "store handle should expose the registered schema store accessor"
    );

    let data_rows = handle.with_data(DataStore::len);
    let index_rows = handle.with_index(IndexStore::len);
    assert_eq!(data_rows, 0, "fresh test data store should be empty");
    assert_eq!(index_rows, 0, "fresh test index store should be empty");
    assert!(
        handle.data_allocation().is_none(),
        "registration without allocation identities should keep data allocation absent"
    );
    assert!(
        handle.index_allocation().is_none(),
        "registration without allocation identities should keep index allocation absent"
    );
    assert!(
        handle.schema_allocation().is_none(),
        "registration without allocation identities should keep schema allocation absent"
    );
    let capabilities = handle.storage_capabilities();
    assert_eq!(capabilities.storage_mode(), StoreRuntimeStorageMode::Heap);
    assert_eq!(
        capabilities.allocation_identity(),
        StoreAllocationIdentityCapability::Absent
    );
    assert_eq!(capabilities.durability(), StoreDurability::Volatile);
    assert_eq!(
        capabilities.commit_participation(),
        StoreCommitParticipation::LiveOnly
    );
    assert_eq!(capabilities.recovery(), StoreRecoveryCapability::None);
    assert_eq!(
        capabilities.schema_metadata(),
        StoreSchemaMetadataCapability::LiveRebuiltMetadata
    );
}

#[test]
fn register_store_with_stable_allocation_identities_binds_metadata() {
    let mut registry = StoreRegistry::new();
    registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            StoreAllocationIdentities::new(
                StoreAllocationIdentity::new(151, "icydb.test.store.data.v1"),
                StoreAllocationIdentity::new(152, "icydb.test.store.index.v1"),
                StoreAllocationIdentity::new(153, "icydb.test.store.schema.v1"),
            ),
            StoreRuntimeStorageCapabilities::stable(),
        )
        .expect("test store registration with allocation identities should succeed");

    let handle = registry
        .try_get_store(STORE_PATH)
        .expect("registered store path should resolve");

    assert_eq!(
        handle.data_allocation(),
        Some(StoreAllocationIdentity::new(
            151,
            "icydb.test.store.data.v1"
        ))
    );
    assert_eq!(
        handle.index_allocation(),
        Some(StoreAllocationIdentity::new(
            152,
            "icydb.test.store.index.v1"
        ))
    );
    assert_eq!(
        handle.schema_allocation(),
        Some(StoreAllocationIdentity::new(
            153,
            "icydb.test.store.schema.v1"
        ))
    );
    let capabilities = handle.storage_capabilities();
    assert_eq!(capabilities.storage_mode(), StoreRuntimeStorageMode::Stable);
    assert_eq!(
        capabilities.allocation_identity(),
        StoreAllocationIdentityCapability::Present
    );
    assert_eq!(capabilities.durability(), StoreDurability::Durable);
    assert_eq!(
        capabilities.commit_participation(),
        StoreCommitParticipation::Durable
    );
    assert_eq!(
        capabilities.recovery(),
        StoreRecoveryCapability::StableCommitReplay
    );
    assert_eq!(
        capabilities.schema_metadata(),
        StoreSchemaMetadataCapability::DurableAcceptedHistory
    );
}

#[test]
fn register_store_rejects_allocation_capability_mismatch() {
    let mut registry = StoreRegistry::new();
    let err = registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::stable(),
        )
        .expect_err("stable capabilities require explicit allocation identities");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("allocation identities do not match storage capabilities"),
        "allocation/capability mismatch should be diagnosed"
    );
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
            &TEST_HEAP_DATA_STORE,
            &TEST_HEAP_INDEX_STORE,
            &TEST_HEAP_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("initial store registration should succeed");

    let err = registry
        .register_store(
            STORE_PATH,
            &TEST_HEAP_DATA_STORE,
            &TEST_HEAP_INDEX_STORE,
            &TEST_HEAP_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
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
            &TEST_HEAP_DATA_STORE,
            &TEST_HEAP_INDEX_STORE,
            &TEST_HEAP_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("initial store registration should succeed");

    let err = registry
        .register_store(
            ALIAS_STORE_PATH,
            &TEST_HEAP_DATA_STORE,
            &TEST_HEAP_INDEX_STORE,
            &TEST_HEAP_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
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
