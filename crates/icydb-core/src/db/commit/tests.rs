use crate::{
    db::{
        Db, PreparedRowCommitOp, RowCommitHandler,
        commit::{
            CommitMarker, CommitRowOp, begin_commit, commit_marker_present,
            ensure_recovered_for_write, finish_commit, init_commit_store_for_tests,
            prepare_row_commit_for_entity, store,
        },
        index::{IndexKey, IndexStore},
        store::{DataKey, DataStore, RawDataKey, StoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin},
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    serialize::serialize,
    test_fixtures::entity_model_from_static,
    test_support::test_memory,
    traits::{
        AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement, EntitySchema,
        EntityValue, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto, ValidateCustom,
        Visitable,
    },
    types::{Id, Ulid},
};
use icydb_derive::FieldValues;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::BTreeSet};

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

impl StoreKind for RecoveryTestDataStore {
    type Canister = RecoveryTestCanister;
}

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct RecoveryTestEntity {
    id: Ulid,
}

impl AsView for RecoveryTestEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RecoveryTestEntity {}
impl SanitizeCustom for RecoveryTestEntity {}
impl ValidateAuto for RecoveryTestEntity {}
impl ValidateCustom for RecoveryTestEntity {}
impl Visitable for RecoveryTestEntity {}

impl Path for RecoveryTestEntity {
    const PATH: &'static str = "commit_tests::RecoveryTestEntity";
}

impl EntityKey for RecoveryTestEntity {
    type Key = Ulid;
}

impl EntityIdentity for RecoveryTestEntity {
    const ENTITY_NAME: &'static str = "RecoveryTestEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static RECOVERY_TEST_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
static RECOVERY_TEST_FIELD_NAMES: [&str; 1] = ["id"];
static RECOVERY_TEST_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static RECOVERY_TEST_MODEL: EntityModel = entity_model_from_static(
    "commit_tests::RecoveryTestEntity",
    "RecoveryTestEntity",
    &RECOVERY_TEST_FIELDS[0],
    &RECOVERY_TEST_FIELDS,
    &RECOVERY_TEST_INDEXES,
);

impl EntitySchema for RecoveryTestEntity {
    const MODEL: &'static EntityModel = &RECOVERY_TEST_MODEL;
    const FIELDS: &'static [&'static str] = &RECOVERY_TEST_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &RECOVERY_TEST_INDEXES;
}

impl EntityPlacement for RecoveryTestEntity {
    type Store = RecoveryTestDataStore;
    type Canister = RecoveryTestCanister;
}

impl EntityKind for RecoveryTestEntity {}

impl EntityValue for RecoveryTestEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct RecoveryIndexedEntity {
    id: Ulid,
    group: u32,
}

impl AsView for RecoveryIndexedEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RecoveryIndexedEntity {}
impl SanitizeCustom for RecoveryIndexedEntity {}
impl ValidateAuto for RecoveryIndexedEntity {}
impl ValidateCustom for RecoveryIndexedEntity {}
impl Visitable for RecoveryIndexedEntity {}

impl Path for RecoveryIndexedEntity {
    const PATH: &'static str = "commit_tests::RecoveryIndexedEntity";
}

impl EntityKey for RecoveryIndexedEntity {
    type Key = Ulid;
}

impl EntityIdentity for RecoveryIndexedEntity {
    const ENTITY_NAME: &'static str = "RecoveryIndexedEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static RECOVERY_INDEXED_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "group",
        kind: EntityFieldKind::Uint,
    },
];
static RECOVERY_INDEXED_FIELD_NAMES: [&str; 2] = ["id", "group"];
static RECOVERY_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_INDEX_FIELDS,
    false,
)];
static RECOVERY_INDEXED_INDEXES: [&IndexModel; 1] = [&RECOVERY_INDEXED_INDEX_MODELS[0]];
static RECOVERY_INDEXED_MODEL: EntityModel = entity_model_from_static(
    "commit_tests::RecoveryIndexedEntity",
    "RecoveryIndexedEntity",
    &RECOVERY_INDEXED_FIELDS[0],
    &RECOVERY_INDEXED_FIELDS,
    &RECOVERY_INDEXED_INDEXES,
);

impl EntitySchema for RecoveryIndexedEntity {
    const MODEL: &'static EntityModel = &RECOVERY_INDEXED_MODEL;
    const FIELDS: &'static [&'static str] = &RECOVERY_INDEXED_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &RECOVERY_INDEXED_INDEXES;
}

impl EntityPlacement for RecoveryIndexedEntity {
    type Store = RecoveryTestDataStore;
    type Canister = RecoveryTestCanister;
}

impl EntityKind for RecoveryIndexedEntity {}

impl EntityValue for RecoveryIndexedEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

fn prepare_recovery_test_row_op(
    db: &Db<RecoveryTestCanister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, crate::error::InternalError> {
    prepare_row_commit_for_entity::<RecoveryTestEntity>(db, op)
}

fn prepare_recovery_indexed_row_op(
    db: &Db<RecoveryTestCanister>,
    op: &CommitRowOp,
) -> Result<PreparedRowCommitOp, crate::error::InternalError> {
    prepare_row_commit_for_entity::<RecoveryIndexedEntity>(db, op)
}

static ROW_COMMIT_HANDLERS: &[RowCommitHandler<RecoveryTestCanister>] = &[
    RowCommitHandler::new(RecoveryTestEntity::PATH, prepare_recovery_test_row_op),
    RowCommitHandler::new(RecoveryIndexedEntity::PATH, prepare_recovery_indexed_row_op),
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

static DB: Db<RecoveryTestCanister> =
    Db::new_with_relations(&STORE_REGISTRY, &[], ROW_COMMIT_HANDLERS);

fn with_recovery_store<R>(f: impl FnOnce(crate::db::store::StoreHandle) -> R) -> R {
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

    let indexed_ids = indexed_ids_for(&first).expect("index entry should exist after replay");
    let expected_ids = [first.id, second.id].into_iter().collect::<BTreeSet<_>>();
    assert_eq!(indexed_ids, expected_ids);
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

    let inserted_indexed_ids =
        indexed_ids_for(&first).expect("index entry should exist after insert replay");
    let inserted_expected_ids = [first.id, second.id].into_iter().collect::<BTreeSet<_>>();
    assert_eq!(inserted_indexed_ids, inserted_expected_ids);

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
