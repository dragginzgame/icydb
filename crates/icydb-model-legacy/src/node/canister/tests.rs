use crate::build::schema_write;

use super::*;

fn insert_canister(path_module: &'static str, ident: &'static str) -> Canister {
    let canister = Canister::new(Def::new(path_module, ident), "test_db", 100, 254, 254, 253);
    schema_write().insert_node(SchemaNode::Canister(canister.clone()));

    canister
}

fn insert_store(
    path_module: &'static str,
    ident: &'static str,
    store_name: &'static str,
    canister_path: &'static str,
    config: StoreJournaledMemoryConfig,
) {
    schema_write().insert_node(SchemaNode::Store(Store::new_journaled(
        Def::new(path_module, ident),
        ident,
        store_name,
        canister_path,
        config,
    )));
}

#[test]
fn validate_rejects_memory_id_collision_between_stores() {
    let canister = insert_canister("schema_store_collision", "Canister");
    let canister_path = "schema_store_collision::Canister";

    insert_store(
        "schema_store_collision",
        "StoreA",
        "store_a",
        canister_path,
        StoreJournaledMemoryConfig::new(110, 111, 112, 115),
    );
    insert_store(
        "schema_store_collision",
        "StoreB",
        "store_b",
        canister_path,
        StoreJournaledMemoryConfig::new(113, 110, 114, 116),
    ); // collision

    let err = canister
        .validate()
        .expect_err("memory-id collision must fail");

    let rendered = err.to_string();
    assert!(
        rendered.contains("duplicate memory_id `110`"),
        "expected duplicate memory-id error, got: {rendered}"
    );
}

#[test]
fn validate_accepts_unique_memory_ids() {
    let canister = insert_canister("schema_store_unique", "Canister");
    let canister_path = "schema_store_unique::Canister";

    insert_store(
        "schema_store_unique",
        "StoreA",
        "store_a",
        canister_path,
        StoreJournaledMemoryConfig::new(130, 131, 132, 136),
    );
    insert_store(
        "schema_store_unique",
        "StoreB",
        "store_b",
        canister_path,
        StoreJournaledMemoryConfig::new(133, 134, 135, 137),
    );

    canister.validate().expect("unique memory IDs should pass");
}

#[test]
fn validate_rejects_reserved_commit_memory_id() {
    let canister = Canister::new(
        Def::new("schema_reserved_commit", "Canister"),
        "test_db",
        100,
        254,
        255,
        253,
    );
    schema_write().insert_node(SchemaNode::Canister(canister.clone()));

    let err = canister
        .validate()
        .expect_err("reserved commit memory id must fail");

    let rendered = err.to_string();
    assert!(
        rendered.contains("reserved for stable-structures internals"),
        "expected reserved-id error, got: {rendered}"
    );
}

#[test]
fn validate_rejects_integrity_progress_memory_collision() {
    let canister = Canister::new(
        Def::new("schema_integrity_progress_collision", "Canister"),
        "test_db",
        100,
        254,
        253,
        253,
    );
    schema_write().insert_node(SchemaNode::Canister(canister.clone()));

    let err = canister
        .validate()
        .expect_err("progress and commit memory IDs must not alias");
    assert!(
        err.to_string().contains("duplicate memory_id `253`"),
        "expected progress allocation collision, got: {err}"
    );
}

#[test]
fn integrity_progress_allocation_has_one_canonical_identity() {
    let canister = Canister::new(
        Def::new("schema_integrity_progress_identity", "Canister"),
        "test_db",
        100,
        254,
        254,
        253,
    );

    assert_eq!(canister.integrity_progress_memory_id(), 253);
    assert_eq!(
        canister.integrity_progress_stable_key(),
        "icydb.test_db.integrity.progress.v1",
    );
}

#[test]
fn store_allocation_identity_is_independent_of_schema_order() {
    let first = Store::new_journaled(
        Def::new("schema_allocation_order", "Users"),
        "USERS",
        "users",
        "schema_allocation_order::Canister",
        StoreJournaledMemoryConfig::new(110, 111, 112, 113),
    );
    let reordered = Store::new_journaled(
        Def::new("schema_allocation_order", "Users"),
        "USERS",
        "users",
        "schema_allocation_order::Canister",
        StoreJournaledMemoryConfig::new(110, 111, 112, 113),
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
        "USERS",
        "users",
        "schema_allocation_add::Canister",
        StoreJournaledMemoryConfig::new(110, 111, 112, 113),
    );
    let _new_store = Store::new_journaled(
        Def::new("schema_allocation_add", "AuditEvents"),
        "AUDIT_EVENTS",
        "audit_events",
        "schema_allocation_add::Canister",
        StoreJournaledMemoryConfig::new(120, 121, 122, 123),
    );

    assert_eq!(existing.stable_data_allocation("test_db").memory_id(), 110);
    assert_eq!(
        existing.stable_data_allocation("test_db").stable_key(),
        "icydb.test_db.users.data.v1"
    );
}

#[test]
fn validate_rejects_same_stable_key_with_different_memory_id() {
    let canister = insert_canister("schema_store_key_collision", "Canister");
    let canister_path = "schema_store_key_collision::Canister";

    insert_store(
        "schema_store_key_collision",
        "StoreA",
        "users",
        canister_path,
        StoreJournaledMemoryConfig::new(110, 111, 112, 113),
    );
    insert_store(
        "schema_store_key_collision",
        "StoreB",
        "users",
        canister_path,
        StoreJournaledMemoryConfig::new(120, 121, 122, 123),
    );

    let err = canister
        .validate()
        .expect_err("stable-key collision must fail");

    let rendered = err.to_string();
    assert!(
        rendered.contains("duplicate stable_key `icydb.test_db.users.data.v1`"),
        "expected duplicate stable-key error, got: {rendered}"
    );
}

#[test]
fn stable_memory_identity_ignores_schema_metadata() {
    let left = StableMemoryAllocation::with_schema_metadata(
        110,
        "icydb.test_db.users.data.v1".to_string(),
        StableMemoryAllocationMetadata::from_accepted_schema_contract(1, 1, "aaa".to_string()),
    );
    let right = StableMemoryAllocation::with_schema_metadata(
        110,
        "icydb.test_db.users.data.v1".to_string(),
        StableMemoryAllocationMetadata::from_accepted_schema_contract(2, 1, "bbb".to_string()),
    );

    assert!(left.same_identity_as(&right));
}

#[test]
fn validate_rejects_app_memory_id_below_canic_reserved_range() {
    let canister = Canister::new(
        Def::new("schema_reserved_app_range", "Canister"),
        "test_db",
        99,
        110,
        99,
        100,
    );

    let err = canister
        .validate()
        .expect_err("app memory id below 100 must fail");

    let rendered = err.to_string();
    assert!(
        rendered.contains("outside of application memory range 100-254"),
        "expected app memory range error, got: {rendered}"
    );
}

#[test]
fn stable_keys_reject_canic_prefix() {
    assert!(!stable_key_is_canonical("canic.test.users.data.v1"));
}
