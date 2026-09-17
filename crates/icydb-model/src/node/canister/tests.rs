use crate::build::schema_write;

use super::*;

#[test]
fn memory_profiles_have_one_default_and_fixed_bucket_sizes() {
    let canister = Canister::new(Def::new("profile", "Canister"), "profile", None);
    assert_eq!(canister.memory_profile(), CanisterMemoryProfile::General);
    for (profile, pages) in [
        (CanisterMemoryProfile::Compact, 4),
        (CanisterMemoryProfile::General, 16),
        (CanisterMemoryProfile::HighHeadroom, 128),
    ] {
        let configured = canister.clone().with_memory_profile(profile);
        assert_eq!(configured.memory_profile().bucket_size_pages(), pages);
        assert_eq!(configured.commit_stable_key(), canister.commit_stable_key());
    }
}

fn insert_canister(path_module: &'static str, ident: &'static str) -> Canister {
    let canister = Canister::new(Def::new(path_module, ident), "test_db", None);
    schema_write().insert_node(SchemaNode::Canister(canister.clone()));

    canister
}

fn insert_store(
    path_module: &'static str,
    ident: &'static str,
    canister_path: &'static str,
    config: StoreJournaledMemoryConfig,
) {
    schema_write().insert_node(SchemaNode::Store(Store::new_journaled(
        Def::new(path_module, ident),
        canister_path,
        config,
    )));
}

#[test]
fn validate_rejects_duplicate_store_keys() {
    let canister = insert_canister("schema_store_collision", "Canister");
    let canister_path = "schema_store_collision::Canister";

    insert_store(
        "schema_store_collision",
        "StoreA",
        canister_path,
        StoreJournaledMemoryConfig::new("store_110"),
    );
    insert_store(
        "schema_store_collision",
        "StoreB",
        canister_path,
        StoreJournaledMemoryConfig::new("store_110"),
    ); // collision

    assert!(canister.validate().is_err());
}

#[test]
fn validate_accepts_unique_store_keys() {
    let canister = insert_canister("schema_store_unique", "Canister");
    let canister_path = "schema_store_unique::Canister";

    insert_store(
        "schema_store_unique",
        "StoreA",
        canister_path,
        StoreJournaledMemoryConfig::new("store_130"),
    );
    insert_store(
        "schema_store_unique",
        "StoreB",
        canister_path,
        StoreJournaledMemoryConfig::new("store_133"),
    );

    canister.validate().expect("unique store keys should pass");
}

#[test]
fn integrity_progress_allocation_has_one_canonical_identity() {
    let canister = Canister::new(
        Def::new("schema_integrity_progress_identity", "Canister"),
        "test_db",
        None,
    );
    assert_eq!(
        canister.integrity_progress_stable_key(),
        "icydb.test_db.integrity.progress.v1",
    );
}

#[test]
fn startup_allocation_has_one_canonical_identity() {
    let canister = Canister::new(
        Def::new("schema_startup_identity", "Canister"),
        "test_db",
        None,
    );
    assert_eq!(
        canister.startup_stable_key(),
        "icydb.test_db.startup.control.v1",
    );
}

#[test]
fn store_allocation_identity_is_independent_of_schema_order() {
    let first = Store::new_journaled(
        Def::new("schema_allocation_order", "Users"),
        "schema_allocation_order::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let reordered = Store::new_journaled(
        Def::new("schema_allocation_order", "Users"),
        "schema_allocation_order::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );

    assert!(
        first
            .stable_data_allocation("test_db")
            .same_identity_as(&reordered.stable_data_allocation("test_db"))
    );
    assert!(
        first
            .stable_index_allocation("test_db")
            .same_identity_as(&reordered.stable_index_allocation("test_db"))
    );
    assert!(
        first
            .stable_schema_allocation("test_db")
            .same_identity_as(&reordered.stable_schema_allocation("test_db"))
    );
}

#[test]
fn adding_store_does_not_change_existing_store_allocation() {
    let existing = Store::new_journaled(
        Def::new("schema_allocation_add", "Users"),
        "schema_allocation_add::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let _new_store = Store::new_journaled(
        Def::new("schema_allocation_add", "AuditEvents"),
        "schema_allocation_add::Canister",
        StoreJournaledMemoryConfig::new("store_120"),
    );
    assert_eq!(
        existing.stable_data_allocation("test_db").stable_key(),
        "icydb.test_db.store.store_110.data.v1"
    );
}

#[test]
fn rust_store_rename_preserves_allocation_identity() {
    let original = Store::new_journaled(
        Def::new("schema_store_rename", "Users"),
        "schema_store_rename::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );
    let renamed = Store::new_journaled(
        Def::new("schema_store_rename", "Accounts"),
        "schema_store_rename::Canister",
        StoreJournaledMemoryConfig::new("store_110"),
    );

    assert!(
        original
            .stable_data_allocation("test_db")
            .same_identity_as(&renamed.stable_data_allocation("test_db"))
    );
    assert!(
        original
            .journal_allocation("test_db")
            .same_identity_as(&renamed.journal_allocation("test_db"))
    );
}

#[test]
fn stable_memory_identity_ignores_schema_metadata() {
    let left = StableMemoryAllocation::with_schema_metadata(
        "icydb.test_db.memory_110.data.v1".to_string(),
        StableMemoryAllocationMetadata::from_accepted_schema_contract(1, 1, "aaa".to_string()),
    );
    let right = StableMemoryAllocation::with_schema_metadata(
        "icydb.test_db.memory_110.data.v1".to_string(),
        StableMemoryAllocationMetadata::from_accepted_schema_contract(2, 1, "bbb".to_string()),
    );

    assert!(left.same_identity_as(&right));
}

#[test]
fn stable_keys_reject_canic_prefix() {
    assert!(!stable_key_is_canonical("canic.test.users.data.v1"));
}
