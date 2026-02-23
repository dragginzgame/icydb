use crate::{
    db::{
        Db, EntityRuntimeHooks,
        commit::{
            CommitMarker, CommitRowOp, begin_commit, commit_marker_present,
            ensure_recovered_for_write, finish_commit, init_commit_store_for_tests,
            prepare_row_commit_for_entity, store,
        },
        data::{DataKey, DataStore, RawDataKey, RawRow, StorageKey},
        index::{IndexKey, IndexStore, RawIndexEntry},
        registry::{StoreHandle, StoreRegistry},
        relation::validate_delete_strong_relations_for_source,
    },
    error::{ErrorClass, ErrorOrigin},
    model::{field::FieldKind, index::IndexModel},
    serialize::serialize,
    test_support::test_memory,
    traits::{EntityIdentity, EntitySchema, FieldValue, Path},
    types::Ulid,
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::BTreeSet};

//
// RecoveryTestCanister
//

crate::test_canister! {
    ident = RecoveryTestCanister,
}

//
// RecoveryTestDataStore
//

crate::test_store! {
    ident = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

///
/// RecoveryTestEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct RecoveryTestEntity {
    id: Ulid,
}

crate::test_entity_schema! {
    ident = RecoveryTestEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryTestEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct RecoveryIndexedEntity {
    id: Ulid,
    group: u32,
}

static RECOVERY_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_INDEX_FIELDS,
    false,
)];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS: [&str; 1] = ["missing_group"];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL: IndexModel = IndexModel::new(
    "missing_group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS,
    false,
);

crate::test_entity_schema! {
    ident = RecoveryIndexedEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryIndexedEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("group", FieldKind::Uint)],
    indexes = [&RECOVERY_INDEXED_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

static ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_NAME,
        RecoveryTestEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_NAME,
        RecoveryIndexedEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(19)));
    static INDEX_STORE: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(20)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(RecoveryTestDataStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("test store registration should succeed");
        reg
    };
}

static DB: Db<RecoveryTestCanister> = Db::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS);

// Intentionally miswired runtime hooks used only to verify dispatch invariants.
static MISWIRED_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] =
    &[EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_NAME,
        RecoveryTestEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    )];

static MISWIRED_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, MISWIRED_ENTITY_RUNTIME_HOOKS);

static DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_NAME,
        RecoveryTestEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_NAME,
        RecoveryIndexedEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_NAME_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS);

static DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_NAME,
        RecoveryTestEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_NAME,
        RecoveryTestEntity::PATH,
        prepare_row_commit_for_entity::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_PATH_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS);

fn with_recovery_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(RecoveryTestDataStore::PATH).map(f))
        .expect("recovery test store access should succeed")
}

// Reset marker + data store to isolate recovery tests.
fn reset_recovery_state() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker reset should succeed");

    with_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

fn row_bytes_for(key: &RawDataKey) -> Option<Vec<u8>> {
    with_recovery_store(|store| {
        store.with_data(|data_store| data_store.get(key).map(|row| row.as_bytes().to_vec()))
    })
}

fn indexed_ids_for(entity: &RecoveryIndexedEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryIndexedEntity::INDEXES[0];
    let index_key = IndexKey::new(entity, index)
        .expect("index key build should succeed")
        .expect("index key should exist")
        .to_raw();

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                entry
                    .try_decode::<RecoveryIndexedEntity>()
                    .expect("index entry decode should succeed")
                    .iter_ids()
                    .collect::<BTreeSet<_>>()
            })
        })
    })
}

fn index_key_bytes_snapshot() -> Vec<Vec<u8>> {
    let mut keys = with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store
                .entries()
                .into_iter()
                .map(|(raw_key, _)| raw_key.as_bytes().to_vec())
                .collect::<Vec<_>>()
        })
    });
    keys.sort();
    keys
}

#[test]
fn index_key_new_rejects_missing_index_field_on_entity_model() {
    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(9901),
        group: 7,
    };

    let err = IndexKey::new(&entity, &RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL)
        .expect_err("index fields missing from the entity model must fail as invariants");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn commit_marker_round_trip_clears_after_finish() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(Vec::new()).expect("commit marker creation should succeed");

    let guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present after begin_commit"
    );

    finish_commit(guard, |_| Ok(())).expect("finish_commit should clear marker");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after finish_commit"
    );
}

#[test]
fn recovery_replay_is_idempotent() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(901),
    };
    let raw_key = DataKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = serialize(&entity).expect("entity serialization should succeed");
    let marker = CommitMarker::new(vec![CommitRowOp::new(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes.clone()),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );

    // First replay applies marker operations and clears the marker.
    ensure_recovered_for_write(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first = row_bytes_for(&raw_key);
    assert_eq!(first, Some(row_bytes));

    // Second replay is a no-op on already recovered state.
    ensure_recovered_for_write(&DB).expect("second recovery replay should be a no-op");
    let second = row_bytes_for(&raw_key);
    assert_eq!(second, first);
}

#[test]
fn recovery_rejects_corrupt_marker_data_key_decode() {
    reset_recovery_state();

    let row_bytes = serialize(&RecoveryTestEntity {
        id: Ulid::from_u128(902),
    })
    .expect("entity serialization should succeed");
    let marker = CommitMarker::new(vec![CommitRowOp::new(
        RecoveryTestEntity::PATH,
        vec![0u8; DataKey::STORED_SIZE_USIZE.saturating_sub(1)],
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err =
        ensure_recovered_for_write(&DB).expect_err("recovery should reject corrupt marker bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery prevalidation fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_unsupported_entity_path_without_fallback() {
    reset_recovery_state();

    let raw_key = DataKey::try_new::<RecoveryTestEntity>(Ulid::from_u128(911))
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = serialize(&RecoveryTestEntity {
        id: Ulid::from_u128(911),
    })
    .expect("entity serialization should succeed");
    let unsupported_path = "commit_tests::UnknownEntity";
    let marker = CommitMarker::new(vec![CommitRowOp::new(
        unsupported_path,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered_for_write(&DB)
        .expect_err("recovery should reject unsupported entity path markers");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("unsupported entity path"),
        "unsupported entity diagnostics should include dispatch context: {err:?}"
    );
    assert!(
        err.message.contains(unsupported_path),
        "unsupported entity diagnostics should include the unknown path: {err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery dispatch fails"
    );
    assert_eq!(
        row_bytes_for(&raw_key),
        None,
        "recovery must not partially apply rows when dispatch fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-unsupported marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_miswired_hook_entity_path_mismatch_as_corruption() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(912),
    };
    let raw_key = DataKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = serialize(&entity).expect("entity serialization should succeed");
    let marker = CommitMarker::new(vec![CommitRowOp::new(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered_for_write(&MISWIRED_DB)
        .expect_err("miswired hook dispatch should fail with path mismatch corruption");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message.contains("commit marker entity path mismatch"),
        "dispatch corruption should include mismatch context: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryIndexedEntity::PATH),
        "dispatch corruption should include the hook-expected entity path: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryTestEntity::PATH),
        "dispatch corruption should include the marker entity path: {err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery dispatch detects hook mismatch"
    );
    assert_eq!(
        row_bytes_for(&raw_key),
        None,
        "recovery must not partially apply rows when hook/entity dispatch mismatches"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn runtime_hook_lookup_rejects_duplicate_entity_names() {
    let Err(err) = DUPLICATE_NAME_DB.runtime_hook_for_entity_name(RecoveryTestEntity::ENTITY_NAME)
    else {
        panic!("duplicate entity names must fail runtime-hook lookup")
    };
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("duplicate runtime hooks for entity name"),
        "duplicate-name runtime-hook lookup should include invariant context: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryTestEntity::ENTITY_NAME),
        "duplicate-name runtime-hook lookup should include conflicting name: {err:?}"
    );
}

#[test]
fn prepare_row_commit_rejects_duplicate_entity_paths() {
    let op = CommitRowOp::new(RecoveryTestEntity::PATH, vec![0xAA], None, None);
    let Err(err) = DUPLICATE_PATH_DB.prepare_row_commit_op(&op) else {
        panic!("duplicate entity paths must fail prepare dispatch")
    };
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("duplicate runtime hooks for entity path"),
        "duplicate-path prepare dispatch should include invariant context: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryTestEntity::PATH),
        "duplicate-path prepare dispatch should include conflicting path: {err:?}"
    );
}

#[test]
fn recovery_replay_merges_multi_row_shared_index_key() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(903),
        group: 7,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(904),
        group: 7,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = serialize(&first).expect("first entity serialization should succeed");
    let second_row = serialize(&second).expect("second entity serialization should succeed");

    let marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered_for_write(&DB).expect("recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), Some(second_row));

    let indexed_ids_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_ids_second = indexed_ids_for(&second).expect("second index entry should exist");
    assert_eq!(
        indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replays_interrupted_atomic_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(907),
        group: 9,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(908),
        group: 9,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = serialize(&first).expect("first entity serialization should succeed");
    let second_row = serialize(&second).expect("second entity serialization should succeed");

    // Simulate an interrupted atomic batch by persisting the marker without apply.
    let marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert!(
        indexed_ids_for(&first).is_none(),
        "interrupted batch index state must not be visible before recovery replay"
    );

    // First replay applies marker row ops and clears the marker.
    ensure_recovered_for_write(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(first_row));
    assert_eq!(second_after, Some(second_row));

    let indexed_after_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_after_second = indexed_ids_for(&second).expect("second index entry should exist");
    let expected_first = std::iter::once(first.id).collect::<BTreeSet<_>>();
    let expected_second = std::iter::once(second.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_after_first, expected_first);
    assert_eq!(indexed_after_second, expected_second);

    // Second replay is a no-op on already recovered state.
    ensure_recovered_for_write(&DB).expect("second recovery replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    let indexed_second_first =
        indexed_ids_for(&first).expect("first index entry should remain after idempotent replay");
    let indexed_second_second =
        indexed_ids_for(&second).expect("second index entry should remain after idempotent replay");
    assert_eq!(indexed_second_first, expected_first);
    assert_eq!(indexed_second_second, expected_second);
}

#[expect(clippy::too_many_lines)]
#[test]
fn recovery_replays_interrupted_atomic_update_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let old_first = RecoveryIndexedEntity {
        id: Ulid::from_u128(909),
        group: 10,
    };
    let old_second = RecoveryIndexedEntity {
        id: Ulid::from_u128(910),
        group: 10,
    };
    let new_first = RecoveryIndexedEntity {
        id: old_first.id,
        group: 11,
    };
    let new_second = RecoveryIndexedEntity {
        id: old_second.id,
        group: 11,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(old_first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(old_second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");

    let old_first_row = serialize(&old_first).expect("old first serialization should succeed");
    let old_second_row = serialize(&old_second).expect("old second serialization should succeed");
    let new_first_row = serialize(&new_first).expect("new first serialization should succeed");
    let new_second_row = serialize(&new_second).expect("new second serialization should succeed");

    // Phase 1: establish the pre-update durable state (group=10).
    let seed_marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(old_first_row.clone()),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(old_second_row.clone()),
        ),
    ])
    .expect("seed marker creation should succeed");
    begin_commit(seed_marker).expect("seed begin_commit should persist marker");
    ensure_recovered_for_write(&DB).expect("seed replay should succeed");

    let old_indexed_ids_first =
        indexed_ids_for(&old_first).expect("old first index entry should exist after seed replay");
    let old_indexed_ids_second = indexed_ids_for(&old_second)
        .expect("old second index entry should exist after seed replay");
    assert_eq!(
        old_indexed_ids_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        old_indexed_ids_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(row_bytes_for(&first_key), Some(old_first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(old_second_row.clone()));

    // Phase 2: simulate an interrupted atomic update marker (group=10 -> group=11).
    let update_marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            Some(old_first_row),
            Some(new_first_row.clone()),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            Some(old_second_row),
            Some(new_second_row.clone()),
        ),
    ])
    .expect("update marker creation should succeed");
    begin_commit(update_marker).expect("update begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        Some(serialize(&old_first).expect("old first serialization should succeed")),
        "pre-recovery row bytes should still reflect old update state"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        Some(serialize(&old_second).expect("old second serialization should succeed")),
        "pre-recovery row bytes should still reflect old update state"
    );
    let pre_update_old_indexed_first =
        indexed_ids_for(&old_first).expect("old first index entry should still exist pre-recovery");
    let pre_update_old_indexed_second = indexed_ids_for(&old_second)
        .expect("old second index entry should still exist pre-recovery");
    assert_eq!(
        pre_update_old_indexed_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        pre_update_old_indexed_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert!(
        indexed_ids_for(&new_first).is_none(),
        "new index entry must not be visible before update replay"
    );

    // First replay applies update row ops and clears the marker.
    ensure_recovered_for_write(&DB).expect("update replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be cleared after replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(new_first_row));
    assert_eq!(second_after, Some(new_second_row));
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should be removed after update replay"
    );
    let new_indexed_ids_first = indexed_ids_for(&new_first)
        .expect("new first index entry should exist after update replay");
    let new_indexed_ids_second = indexed_ids_for(&new_second)
        .expect("new second index entry should exist after update replay");
    assert_eq!(
        new_indexed_ids_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_ids_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );

    // Second replay is a no-op on already recovered state.
    ensure_recovered_for_write(&DB).expect("second update replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should remain absent after idempotent replay"
    );
    let new_indexed_second_first = indexed_ids_for(&new_first)
        .expect("new first index entry should remain after idempotent replay");
    let new_indexed_second_second = indexed_ids_for(&new_second)
        .expect("new second index entry should remain after idempotent replay");
    assert_eq!(
        new_indexed_second_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_second_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replay_mixed_save_save_delete_sequence_preserves_final_index_state() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(905),
        group: 8,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(906),
        group: 8,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = serialize(&first).expect("first entity serialization should succeed");
    let second_row = serialize(&second).expect("second entity serialization should succeed");

    // Phase 1: replay two inserts sharing the same index key.
    let save_marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(save_marker).expect("begin_commit should persist marker");

    ensure_recovered_for_write(&DB).expect("recovery replay should succeed");
    assert_eq!(row_bytes_for(&first_key), Some(first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(second_row.clone()));

    let inserted_indexed_ids_first =
        indexed_ids_for(&first).expect("first index entry should exist after insert replay");
    let inserted_indexed_ids_second =
        indexed_ids_for(&second).expect("second index entry should exist after insert replay");
    assert_eq!(
        inserted_indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        inserted_indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );

    // Phase 2: replay a delete that removes one of the inserted rows.
    let delete_marker = CommitMarker::new(vec![CommitRowOp::new(
        RecoveryIndexedEntity::PATH,
        second_key.as_bytes().to_vec(),
        Some(second_row),
        None,
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_marker).expect("delete begin_commit should persist marker");

    ensure_recovered_for_write(&DB).expect("delete recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), None);

    let indexed_ids = indexed_ids_for(&first).expect("index entry should exist after replay");
    let expected_ids = std::iter::once(first.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_ids, expected_ids);
}

#[test]
fn recovery_replay_preserves_index_key_raw_bytes_across_reloads() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(913),
        group: 20,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(914),
        group: 21,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = serialize(&first).expect("first entity serialization should succeed");
    let second_row = serialize(&second).expect("second entity serialization should succeed");

    let index = RecoveryIndexedEntity::INDEXES[0];
    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    let marker = CommitMarker::new(vec![
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        CommitRowOp::new(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered_for_write(&DB).expect("first recovery replay should succeed");
    let first_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        first_snapshot, expected,
        "index key bytes after replay should match precomputed canonical bytes"
    );

    ensure_recovered_for_write(&DB).expect("second recovery replay should be no-op");
    let second_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        second_snapshot, expected,
        "index key bytes should remain stable after subsequent replay"
    );
    assert_eq!(second_snapshot, first_snapshot);
}

#[test]
fn recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(920),
        group: 30,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(921),
        group: 31,
    };
    let stale = RecoveryIndexedEntity {
        id: Ulid::from_u128(999),
        group: 99,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = serialize(&first).expect("first row serialization should succeed");
    let second_row = serialize(&second).expect("second row serialization should succeed");

    let index = RecoveryIndexedEntity::INDEXES[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale key build should succeed")
        .expect("stale key should exist")
        .to_raw();
    let stale_storage_key =
        StorageKey::try_from_value(&stale.id.to_value()).expect("stale storage key should encode");
    let stale_entry = RawIndexEntry::try_from_keys(vec![stale_storage_key])
        .expect("stale index entry should encode");

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert(
                first_key,
                RawRow::try_new(first_row).expect("first row raw construction should succeed"),
            );
            data_store.insert(
                second_key,
                RawRow::try_new(second_row).expect("second row raw construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered_for_write(&DB).expect("recovery should rebuild indexes from data rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery"
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale index entries and recreate canonical entries from rows"
    );
    assert_eq!(
        indexed_ids_for(&first).expect("first index entry should exist"),
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_for(&second).expect("second index entry should exist"),
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_startup_rebuild_fail_closed_restores_previous_index_state_on_corrupt_row() {
    reset_recovery_state();

    let sentinel = RecoveryIndexedEntity {
        id: Ulid::from_u128(922),
        group: 77,
    };
    let index = RecoveryIndexedEntity::INDEXES[0];
    let sentinel_key = IndexKey::new(&sentinel, index)
        .expect("sentinel key build should succeed")
        .expect("sentinel key should exist")
        .to_raw();
    let sentinel_storage_key = StorageKey::try_from_value(&sentinel.id.to_value())
        .expect("sentinel storage key should encode");
    let sentinel_entry = RawIndexEntry::try_from_keys(vec![sentinel_storage_key])
        .expect("sentinel entry should encode");

    with_recovery_store(|store| {
        store.with_index_mut(|index_store| {
            index_store.insert(sentinel_key.clone(), sentinel_entry);
        });
    });
    let before_snapshot = index_key_bytes_snapshot();

    let bad_key = DataKey::try_new::<RecoveryIndexedEntity>(Ulid::from_u128(923))
        .expect("bad data key should build")
        .to_raw()
        .expect("bad data key should encode");
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert(
                bad_key,
                RawRow::try_new(vec![0xFF, 0x00, 0xAA]).expect("bad row raw construction"),
            );
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered_for_write(&DB)
        .expect_err("startup rebuild should reject corrupted row bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Serialize);

    let after_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        after_snapshot, before_snapshot,
        "failed startup rebuild must restore the prior index snapshot"
    );
}
