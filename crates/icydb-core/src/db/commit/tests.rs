use crate::{
    db::{
        Db,
        commit::{
            CommitDataOp, CommitKind, CommitMarker, begin_commit, commit_marker_present,
            ensure_recovered_for_write, finish_commit, init_commit_store_for_tests, store,
        },
        index::IndexStoreRegistry,
        store::{DataKey, DataStore, DataStoreRegistry, RawDataKey},
    },
    error::{ErrorClass, ErrorOrigin},
    traits::{CanisterKind, DataStoreKind, Path},
};
use canic_cdk::structures::{
    DefaultMemoryImpl,
    memory::{MemoryId, MemoryManager, VirtualMemory},
};
use std::cell::RefCell;

///
/// RecoveryTestCanister
///

struct RecoveryTestCanister;

impl Path for RecoveryTestCanister {
    const PATH: &'static str = "commit_tests::RecoveryTestCanister";
}

impl CanisterKind for RecoveryTestCanister {}

///
/// RecoveryTestDataStore
///

struct RecoveryTestDataStore;

impl Path for RecoveryTestDataStore {
    const PATH: &'static str = "commit_tests::RecoveryTestDataStore";
}

impl DataStoreKind for RecoveryTestDataStore {
    type Canister = RecoveryTestCanister;
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(19)));
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(RecoveryTestDataStore::PATH, &DATA_STORE);
        reg
    };
    static INDEX_REGISTRY: IndexStoreRegistry = IndexStoreRegistry::new();
}

static DB: Db<RecoveryTestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY);

// Test-only stable memory allocation for in-memory stores.
fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());
    manager.get(MemoryId::new(id))
}

// Reset marker + data store to isolate recovery tests.
fn reset_recovery_state() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker reset should succeed");

    DB.with_data(|reg| reg.with_store_mut(RecoveryTestDataStore::PATH, DataStore::clear))
        .expect("data store reset should succeed");
}

fn row_bytes_for(key: &RawDataKey) -> Option<Vec<u8>> {
    DB.with_data(|reg| {
        reg.with_store(RecoveryTestDataStore::PATH, |store| {
            store.get(key).map(|row| row.as_bytes().to_vec())
        })
    })
    .expect("data store read should succeed")
}

#[test]
fn commit_marker_round_trip_clears_after_finish() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(CommitKind::Save, Vec::new(), Vec::new())
        .expect("commit marker creation should succeed");

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

    let raw_key = DataKey::max_storable()
        .to_raw()
        .expect("data key should encode");
    let row_bytes = vec![1u8, 2, 3, 4];
    let marker = CommitMarker::new(
        CommitKind::Save,
        Vec::new(),
        vec![CommitDataOp {
            store: RecoveryTestDataStore::PATH.to_string(),
            key: raw_key.as_bytes().to_vec(),
            value: Some(row_bytes.clone()),
        }],
    )
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

    let marker = CommitMarker::new(
        CommitKind::Save,
        Vec::new(),
        vec![CommitDataOp {
            store: RecoveryTestDataStore::PATH.to_string(),
            key: vec![0u8; DataKey::STORED_SIZE_USIZE.saturating_sub(1)],
            value: Some(vec![9u8]),
        }],
    )
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
