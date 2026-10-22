//! Module: db::registry::tests
//! Responsibility: registry registration, lookup, and capability fixture coverage.
//! Does not own: storage engine behavior outside registered handle wiring.
//! Boundary: exercises registry-local invariants and converted error classification.

use crate::{
    db::{
        data::DataStore,
        index::IndexStore,
        journal::JournalTailStore,
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
    static TEST_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(151)));
    static TEST_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(152)));
    static TEST_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(153)));
    static TEST_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(154)));
    static TEST_HEAP_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static TEST_HEAP_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static TEST_HEAP_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static OTHER_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(155)));
    static OTHER_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(156)));
    static OTHER_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(157)));
    static OTHER_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(158)));
    static OTHER_HEAP_DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static OTHER_HEAP_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static OTHER_HEAP_SCHEMA_STORE: RefCell<SchemaStore> =
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
fn register_store_with_journaled_allocation_identities_binds_four_role_metadata() {
    let mut registry = StoreRegistry::new();
    registry
        .register_journaled_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            &TEST_JOURNAL_STORE,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(151, "icydb.test.store.data.v1"),
                StoreAllocationIdentity::new(152, "icydb.test.store.index.v1"),
                StoreAllocationIdentity::new(153, "icydb.test.store.schema.v1"),
                StoreAllocationIdentity::new(154, "icydb.test.store.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("test journaled store registration should succeed");

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
    assert_eq!(
        handle.journal_allocation(),
        Some(StoreAllocationIdentity::new(
            154,
            "icydb.test.store.journal.v1"
        ))
    );
    assert!(
        ptr::eq(
            handle
                .journal_tail_store()
                .expect("journaled handle should expose journal tail store"),
            &TEST_JOURNAL_STORE,
        ),
        "journaled registration should bind the journal tail store accessor",
    );
    let capabilities = handle.storage_capabilities();
    assert_eq!(
        capabilities.storage_mode(),
        StoreRuntimeStorageMode::Journaled
    );
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
        StoreRecoveryCapability::StableBasePlusJournalReplay
    );
    assert_eq!(
        capabilities.schema_metadata(),
        StoreSchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail
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
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect_err("journaled capabilities require explicit allocation identities");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "allocation/capability mismatch should stay store-invariant classified",
    );
}

#[test]
fn register_store_rejects_journaled_capabilities_without_journal_allocation_identity() {
    let mut registry = StoreRegistry::new();
    let err = registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect_err("journaled capabilities require explicit journal allocation identity");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "journal allocation/capability mismatch should stay store-invariant classified",
    );
}

#[test]
fn register_store_rejects_journaled_capabilities_without_journal_tail_store() {
    let mut registry = StoreRegistry::new();
    let err = registry
        .register_store(
            STORE_PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(151, "icydb.test.store.data.v1"),
                StoreAllocationIdentity::new(152, "icydb.test.store.index.v1"),
                StoreAllocationIdentity::new(153, "icydb.test.store.schema.v1"),
                StoreAllocationIdentity::new(154, "icydb.test.store.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect_err("journaled capabilities require explicit journal tail store");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn missing_store_path_rejected_before_access() {
    let registry = StoreRegistry::new();
    let err = registry
        .try_get_store("store_registry_tests::Missing")
        .expect_err("missing path should fail lookup");

    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
        "missing store lookup should stay internal classified",
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
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "duplicate registration should stay store-invariant classified",
    );
}

#[test]
fn heap_store_registration_rejects_every_partial_handle_alias() {
    for shared_roles in 0..8 {
        let mut registry = test_registry();
        let data = if shared_roles & 1 != 0 {
            &TEST_HEAP_DATA_STORE
        } else {
            &OTHER_HEAP_DATA_STORE
        };
        let index = if shared_roles & 2 != 0 {
            &TEST_HEAP_INDEX_STORE
        } else {
            &OTHER_HEAP_INDEX_STORE
        };
        let schema = if shared_roles & 4 != 0 {
            &TEST_HEAP_SCHEMA_STORE
        } else {
            &OTHER_HEAP_SCHEMA_STORE
        };
        let result = registry.register_store(
            ALIAS_STORE_PATH,
            data,
            index,
            schema,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        );
        if shared_roles == 0 {
            result.expect("independent heap handles must remain registerable");
        } else {
            let error = result.expect_err("sharing any physical heap role must reject");
            assert_eq!(error.class(), ErrorClass::InvariantViolation);
            assert_eq!(error.origin(), ErrorOrigin::Store);
            assert_eq!(
                error.diagnostic_code(),
                icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation
            );
            assert_eq!(registry.iter().count(), 1);
            assert!(registry.try_get_store(ALIAS_STORE_PATH).is_err());
            registry
                .register_store(
                    ALIAS_STORE_PATH,
                    &OTHER_HEAP_DATA_STORE,
                    &OTHER_HEAP_INDEX_STORE,
                    &OTHER_HEAP_SCHEMA_STORE,
                    StoreAllocationIdentities::absent(),
                    StoreRuntimeStorageCapabilities::heap(),
                )
                .expect("refusal must leave the path available for independent handles");
        }
        assert_eq!(registry.iter().count(), 2);
        let alias = registry.try_get_store(ALIAS_STORE_PATH).unwrap();
        assert!(ptr::eq(alias.data_store(), &OTHER_HEAP_DATA_STORE));
        assert!(ptr::eq(alias.index_store(), &OTHER_HEAP_INDEX_STORE));
        assert!(ptr::eq(alias.schema_store(), &OTHER_HEAP_SCHEMA_STORE));
        let original = registry.try_get_store(STORE_PATH).unwrap();
        assert!(ptr::eq(original.data_store(), &TEST_HEAP_DATA_STORE));
        assert!(ptr::eq(original.index_store(), &TEST_HEAP_INDEX_STORE));
        assert!(ptr::eq(original.schema_store(), &TEST_HEAP_SCHEMA_STORE));
        assert!(original.journal_tail_store().is_none());
    }
}

#[test]
fn journaled_store_registration_rejects_every_partial_handle_alias() {
    let original_allocations = StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(151, "icydb.test.store.data.v1"),
        StoreAllocationIdentity::new(152, "icydb.test.store.index.v1"),
        StoreAllocationIdentity::new(153, "icydb.test.store.schema.v1"),
        StoreAllocationIdentity::new(154, "icydb.test.store.journal.v1"),
    );
    let other_allocations = StoreAllocationIdentities::new_journaled(
        StoreAllocationIdentity::new(155, "icydb.test.other.data.v1"),
        StoreAllocationIdentity::new(156, "icydb.test.other.index.v1"),
        StoreAllocationIdentity::new(157, "icydb.test.other.schema.v1"),
        StoreAllocationIdentity::new(158, "icydb.test.other.journal.v1"),
    );
    for shared_roles in 0..16 {
        let mut registry = StoreRegistry::new();
        registry
            .register_journaled_store(
                STORE_PATH,
                &TEST_DATA_STORE,
                &TEST_INDEX_STORE,
                &TEST_SCHEMA_STORE,
                &TEST_JOURNAL_STORE,
                original_allocations,
                StoreRuntimeStorageCapabilities::journaled(),
            )
            .unwrap();
        let data = if shared_roles & 1 != 0 {
            &TEST_DATA_STORE
        } else {
            &OTHER_DATA_STORE
        };
        let index = if shared_roles & 2 != 0 {
            &TEST_INDEX_STORE
        } else {
            &OTHER_INDEX_STORE
        };
        let schema = if shared_roles & 4 != 0 {
            &TEST_SCHEMA_STORE
        } else {
            &OTHER_SCHEMA_STORE
        };
        let journal = if shared_roles & 8 != 0 {
            &TEST_JOURNAL_STORE
        } else {
            &OTHER_JOURNAL_STORE
        };
        let result = registry.register_journaled_store(
            ALIAS_STORE_PATH,
            data,
            index,
            schema,
            journal,
            other_allocations,
            StoreRuntimeStorageCapabilities::journaled(),
        );
        if shared_roles == 0 {
            result.expect("independent journaled handles must remain registerable");
        } else {
            let error = result.expect_err("sharing any journaled role must reject");
            assert_eq!(error.class(), ErrorClass::InvariantViolation);
            assert_eq!(error.origin(), ErrorOrigin::Store);
            assert_eq!(
                error.diagnostic_code(),
                icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation
            );
            assert_eq!(registry.iter().count(), 1);
            assert!(registry.try_get_store(ALIAS_STORE_PATH).is_err());
            registry
                .register_journaled_store(
                    ALIAS_STORE_PATH,
                    &OTHER_DATA_STORE,
                    &OTHER_INDEX_STORE,
                    &OTHER_SCHEMA_STORE,
                    &OTHER_JOURNAL_STORE,
                    other_allocations,
                    StoreRuntimeStorageCapabilities::journaled(),
                )
                .expect("refusal must leave the path available for independent handles");
        }
        assert_eq!(registry.iter().count(), 2);
        let alias = registry.try_get_store(ALIAS_STORE_PATH).unwrap();
        assert!(ptr::eq(alias.data_store(), &OTHER_DATA_STORE));
        assert!(ptr::eq(alias.index_store(), &OTHER_INDEX_STORE));
        assert!(ptr::eq(alias.schema_store(), &OTHER_SCHEMA_STORE));
        assert!(ptr::eq(
            alias.journal_tail_store().unwrap(),
            &OTHER_JOURNAL_STORE
        ));
        assert_eq!(alias.allocation_identities(), other_allocations);
        let original = registry.try_get_store(STORE_PATH).unwrap();
        assert!(ptr::eq(original.data_store(), &TEST_DATA_STORE));
        assert!(ptr::eq(original.index_store(), &TEST_INDEX_STORE));
        assert!(ptr::eq(original.schema_store(), &TEST_SCHEMA_STORE));
        assert!(ptr::eq(
            original.journal_tail_store().unwrap(),
            &TEST_JOURNAL_STORE
        ));
        assert_eq!(original.allocation_identities(), original_allocations);
    }
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
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
        "alias registration should report a compact store invariant",
    );
}

#[test]
fn committed_key_open_refuses_drifted_ids_without_changing_storage() {
    use ic_memory::Memory;

    // One default runtime on an isolated thread; ordinary store fixtures use
    // their own explicit snapshots without changing this default runtime.
    std::thread::spawn(|| {
        const OWNER: &str = "icydb.core_identity";
        const KEY: &str = "icydb.core_identity.control.v1";
        let requests = ic_memory::SealedDeclarationSnapshot::new(vec![
            ic_memory::MemoryRequest::new(OWNER, KEY).unwrap(),
        ])
        .unwrap();
        let pool = ic_memory::MemoryAllocationPool::new(
            vec![ic_memory::MemoryAuthority::new(OWNER, "icydb.core_identity.").unwrap()],
            vec![],
        )
        .unwrap();
        let mut runtime =
            ic_memory::MemoryRuntime::new(ic_memory::DefaultMemoryImpl::default()).unwrap();
        runtime
            .bootstrap(&requests, &pool, &ic_memory::GenericAllocationPolicy)
            .unwrap();
        let runtime = std::rc::Rc::new(runtime);
        crate::db::bind_memory_runtime(std::rc::Rc::clone(&runtime)).unwrap();
        let id = runtime.memory_id(KEY).unwrap();
        let memory = crate::db::registry::open_committed_memory(KEY, id).unwrap();
        memory.grow(1).unwrap();
        memory.write(0, b"retained");
        let commitment = runtime.committed_allocations().unwrap();
        let before = runtime.memory_allocations().unwrap();
        assert!(crate::db::registry::open_committed_memory(KEY, id + 1).is_err());
        assert!(
            crate::db::registry::open_committed_memory("icydb.core_identity.absent.v1", id)
                .is_err()
        );
        assert_eq!(runtime.committed_allocations().unwrap(), commitment);
        assert_eq!(runtime.memory_allocations().unwrap(), before);
        // Reopening the committed key requires releasing its exclusive handle.
        drop(memory);
        let memory = crate::db::registry::open_committed_memory(KEY, id).unwrap();
        let mut bytes = [0; 8];
        memory.read(0, &mut bytes);
        assert_eq!(&bytes, b"retained");
    })
    .join()
    .unwrap();
}
