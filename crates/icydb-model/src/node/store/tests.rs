use crate::{
    build::schema_write,
    node::{Canister, SchemaNode},
};

use super::*;

fn insert_canister(path_module: &'static str, ident: &'static str) {
    schema_write().insert_node(SchemaNode::Canister(Canister::new(
        Def::new(path_module, ident),
        "test_db",
        None,
    )));
}

#[test]
fn store_allocations_default_to_absent_schema_metadata() {
    let store = Store::new_journaled(
        Def::new("demo::rpg", "CharacterStore"),
        "demo::rpg::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );

    for allocation in [
        store.stable_data_allocation("demo_rpg"),
        store.stable_index_allocation("demo_rpg"),
        store.stable_schema_allocation("demo_rpg"),
        store.journal_allocation("demo_rpg"),
    ] {
        assert_eq!(allocation.schema_version(), None);
        assert_eq!(allocation.schema_fingerprint_method_version(), None);
        assert_eq!(allocation.schema_fingerprint(), None);
        assert_eq!(
            allocation.schema_metadata(),
            &StableMemoryAllocationMetadata::absent()
        );
    }
}

#[test]
fn allocation_metadata_is_role_specific_and_diagnostic_only() {
    let store = Store::new_journaled(
        Def::new("demo::rpg", "CharacterStore"),
        "demo::rpg::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let data = store.stable_data_allocation_with_schema_metadata(
        "demo_rpg",
        StableMemoryAllocationMetadata::from_accepted_schema_contract(
            7,
            1,
            "data-row-layout".to_string(),
        ),
    );
    let index = store.stable_index_allocation_with_schema_metadata(
        "demo_rpg",
        StableMemoryAllocationMetadata::from_accepted_schema_contract(
            8,
            1,
            "index-catalog".to_string(),
        ),
    );
    let schema = store.stable_schema_allocation_with_schema_metadata(
        "demo_rpg",
        StableMemoryAllocationMetadata::from_accepted_schema_contract(
            10,
            1,
            "schema-catalog".to_string(),
        ),
    );
    let data_after_reconcile = store.stable_data_allocation_with_schema_metadata(
        "demo_rpg",
        StableMemoryAllocationMetadata::from_accepted_schema_contract(
            9,
            1,
            "data-row-layout-changed".to_string(),
        ),
    );

    assert_eq!(data.schema_version(), Some(7));
    assert_eq!(data.schema_fingerprint_method_version(), Some(1));
    assert_eq!(data.schema_fingerprint(), Some("data-row-layout"));
    assert_eq!(index.schema_version(), Some(8));
    assert_eq!(index.schema_fingerprint_method_version(), Some(1));
    assert_eq!(index.schema_fingerprint(), Some("index-catalog"));
    assert_eq!(schema.schema_version(), Some(10));
    assert_eq!(schema.schema_fingerprint_method_version(), Some(1));
    assert_eq!(schema.schema_fingerprint(), Some("schema-catalog"));
    assert!(data.same_identity_as(&data_after_reconcile));
    assert!(!data.same_identity_as(&index));
    assert!(!data.same_identity_as(&schema));
}

#[test]
fn store_owns_explicit_heap_storage_config() {
    insert_canister("store_heap_config", "Canister");
    let store = Store::new_heap(
        Def::new("store_heap_config", "Store"),
        "store_heap_config::Canister",
        StoreHeapConfig::new(),
    );

    assert!(store.is_heap_storage());
    assert!(store.validate().is_ok());
}

#[test]
fn heap_store_storage_capabilities_describe_volatile_contract() {
    let store = Store::new_heap(
        Def::new("store_heap_capabilities", "Store"),
        "store_heap_capabilities::Canister",
        StoreHeapConfig::new(),
    );
    let capabilities = store.storage_capabilities();

    assert_eq!(capabilities.storage_mode(), StoreStorageMode::Heap);
    assert_eq!(
        capabilities.allocation_identity(),
        AllocationIdentityCapability::Absent,
    );
    assert_eq!(capabilities.durability(), StoreDurability::Volatile);
    assert_eq!(capabilities.recovery(), StoreRecoveryCapability::None);
    assert_eq!(
        capabilities.commit_participation(),
        CommitParticipation::LiveOnly,
    );
    assert_eq!(
        capabilities.schema_metadata(),
        SchemaMetadataCapability::LiveRebuiltMetadata,
    );
    assert_eq!(
        capabilities.relation_source(),
        RelationSourceCapability::LiveSource,
    );
    assert_eq!(
        capabilities.relation_target(),
        RelationTargetCapability::VolatileTarget,
    );
    assert_eq!(
        capabilities.live_validation(),
        LiveValidationCapability::Supported,
    );
    assert!(!capabilities.has_allocation_identity());
    assert!(!capabilities.participates_in_durable_commit());
    assert!(capabilities.is_volatile());
}

#[test]
fn store_owns_explicit_journaled_storage_config() {
    insert_canister("store_journaled_config", "Canister");
    let store = Store::new_journaled(
        Def::new("store_journaled_config", "Store"),
        "store_journaled_config::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );

    assert!(store.is_journaled_storage());
    assert!(!store.is_heap_storage());
    let journaled = store
        .journaled_memory_config()
        .expect("journaled model stores four-role config explicitly");
    assert_eq!(journaled.key(), "store_110");
    assert_eq!(store.stable_store_key(), "store_110");
    assert!(store.validate().is_ok());
}

#[test]
fn store_keys_validate_segments_and_complete_role_key_lengths() {
    let canister = Canister::new(Def::new("bounded_key", "Canister"), "app", None);
    // The journal role is longest: prefix + store key + suffix = 27 + key bytes.
    let admitted = "a".repeat(101);
    let oversized = "a".repeat(102);
    for (key, accepted) in [
        ("transfers", true),
        ("", false),
        ("Transfers", false),
        ("1store", false),
        ("store.name", false),
        (admitted.as_str(), true),
        (oversized.as_str(), false),
    ] {
        let config = StoreJournaledMemoryConfig::new(Box::leak(key.to_string().into_boxed_str()));
        let store = Store::new_journaled(
            Def::new("bounded_key", "Store"),
            "bounded_key::Canister",
            config,
        );
        let mut errors = ErrorTree::new();
        validate_journaled_memory_config(&mut errors, &store, config, &canister);
        assert_eq!(errors.result().is_ok(), accepted);
    }
}

#[test]
fn journaled_store_storage_capabilities_describe_cached_stable_contract() {
    let store = Store::new_journaled(
        Def::new("store_journaled_capabilities", "Store"),
        "store_journaled_capabilities::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let capabilities = store.storage_capabilities();

    assert_eq!(capabilities.storage_mode(), StoreStorageMode::Journaled);
    assert_eq!(
        capabilities.allocation_identity(),
        AllocationIdentityCapability::Present,
    );
    assert_eq!(capabilities.durability(), StoreDurability::Durable);
    assert_eq!(
        capabilities.recovery(),
        StoreRecoveryCapability::StableBasePlusJournalReplay,
    );
    assert_eq!(
        capabilities.commit_participation(),
        CommitParticipation::Durable,
    );
    assert_eq!(
        capabilities.schema_metadata(),
        SchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail,
    );
    assert_eq!(
        capabilities.relation_source(),
        RelationSourceCapability::DurableSource,
    );
    assert_eq!(
        capabilities.relation_target(),
        RelationTargetCapability::DurableTarget,
    );
    assert_eq!(
        capabilities.live_validation(),
        LiveValidationCapability::Supported,
    );
    assert!(capabilities.has_allocation_identity());
    assert!(capabilities.participates_in_durable_commit());
    assert!(!capabilities.is_volatile());
}

#[test]
fn journaled_store_allocations_use_role_named_stable_keys() {
    let store = Store::new_journaled(
        Def::new("demo::rpg", "CharacterStore"),
        "demo::rpg::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );

    assert_eq!(
        store.stable_data_allocation("demo_rpg").stable_key(),
        "icydb.demo_rpg.store.store_110.data.v1",
    );
    assert_eq!(
        store.stable_index_allocation("demo_rpg").stable_key(),
        "icydb.demo_rpg.store.store_110.index.v1",
    );
    assert_eq!(
        store.stable_schema_allocation("demo_rpg").stable_key(),
        "icydb.demo_rpg.store.store_110.schema.v1",
    );
    assert_eq!(
        store.journal_allocation("demo_rpg").stable_key(),
        "icydb.demo_rpg.store.store_110.journal.v1",
    );

    // Rust names locate generated code; only the explicit key names memory.
    let renamed = Store::new_journaled(
        Def::new("renamed::module", "RenamedStore"),
        "renamed::module::RenamedCanister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    for (before, after) in [
        (
            store.stable_data_allocation("demo_rpg"),
            renamed.stable_data_allocation("demo_rpg"),
        ),
        (
            store.stable_index_allocation("demo_rpg"),
            renamed.stable_index_allocation("demo_rpg"),
        ),
        (
            store.stable_schema_allocation("demo_rpg"),
            renamed.stable_schema_allocation("demo_rpg"),
        ),
        (
            store.journal_allocation("demo_rpg"),
            renamed.journal_allocation("demo_rpg"),
        ),
    ] {
        assert_eq!(before.stable_key(), after.stable_key());
    }
}

#[test]
fn storage_capabilities_are_not_allocation_identity() {
    let store_a = Store::new_journaled(
        Def::new("demo::rpg", "CharacterStore"),
        "demo::rpg::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let store_b = Store::new_journaled(
        Def::new("demo::rpg", "InventoryStore"),
        "demo::rpg::Canister",
        StoreJournaledMemoryConfig::new("store_120"),
    );

    assert_eq!(
        store_a.storage_capabilities(),
        store_b.storage_capabilities()
    );
    assert_ne!(
        store_a.stable_data_allocation("demo_rpg"),
        store_b.stable_data_allocation("demo_rpg"),
        "stable allocation identity must remain separate from capabilities",
    );
}

#[test]
fn capability_consumers_use_axes_not_storage_mode() {
    const fn commit_label(capabilities: StoreStorageCapabilities) -> &'static str {
        match capabilities.commit_participation() {
            CommitParticipation::Durable => "durable",
            CommitParticipation::LiveOnly => "live-only",
        }
    }

    let future_durable_heap_mode = StoreStorageCapabilities {
        storage_mode: StoreStorageMode::Heap,
        allocation_identity: AllocationIdentityCapability::Present,
        durability: StoreDurability::Durable,
        recovery: StoreRecoveryCapability::StableBasePlusJournalReplay,
        commit_participation: CommitParticipation::Durable,
        schema_metadata: SchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail,
        relation_source: RelationSourceCapability::DurableSource,
        relation_target: RelationTargetCapability::DurableTarget,
        live_validation: LiveValidationCapability::Supported,
    };

    assert_eq!(commit_label(future_durable_heap_mode), "durable");
    assert!(future_durable_heap_mode.participates_in_durable_commit());
    assert_eq!(
        future_durable_heap_mode.storage_mode(),
        StoreStorageMode::Heap,
        "the diagnostic storage mode must not drive commit policy",
    );
}
