use crate::{
    db::{
        Db,
        commit::{
            CommitKind, CommitMarker, begin_commit, commit_marker_present,
            ensure_recovered_for_write, init_commit_store_for_tests,
        },
        executor::{DeleteExecutor, SaveExecutor},
        query::{Query, ReadConsistency},
        store::{DataStore, DataStoreRegistry},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
    },
    test_fixtures::entity_model_from_static,
    traits::{
        CanisterKind, DataStoreKind, EntityIdentity, EntityKind, EntityPlacement, EntitySchema,
        EntityStorageKey, EntityValue, Path, SanitizeAuto, SanitizeCustom, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Id, Ulid},
};
use canic_cdk::structures::{
    DefaultMemoryImpl,
    memory::{MemoryId, MemoryManager, VirtualMemory},
};
use icydb_derive::FieldValues;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

///
/// TestCanister
///

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "executor_tests::TestCanister";
}

impl CanisterKind for TestCanister {}

///
/// TestDataStore
///

struct TestDataStore;

impl Path for TestDataStore {
    const PATH: &'static str = "executor_tests::TestDataStore";
}

impl DataStoreKind for TestDataStore {
    type Canister = TestCanister;
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(0)));
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(TestDataStore::PATH, &DATA_STORE);
        reg
    };
    static INDEX_REGISTRY: crate::db::index::IndexStoreRegistry =
        crate::db::index::IndexStoreRegistry::new();
}

static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY);

// Test-only stable memory allocation for in-memory stores.
fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());

    manager.get(MemoryId::new(id))
}

///
/// SimpleEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct SimpleEntity {
    id: Id<Self>,
}

impl View for SimpleEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for SimpleEntity {}
impl SanitizeCustom for SimpleEntity {}
impl ValidateAuto for SimpleEntity {}
impl ValidateCustom for SimpleEntity {}
impl Visitable for SimpleEntity {}

impl Path for SimpleEntity {
    const PATH: &'static str = "executor_tests::SimpleEntity";
}

impl EntityStorageKey for SimpleEntity {
    type Key = Ulid;
}

impl EntityIdentity for SimpleEntity {
    const ENTITY_NAME: &'static str = "SimpleEntity";
    const PRIMARY_KEY: &'static str = "id";
    const IDENTITY_NAMESPACE: &'static str = "SimpleEntity";
}

static SIMPLE_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
static SIMPLE_FIELD_NAMES: [&str; 1] = ["id"];
static SIMPLE_INDEXES: [&crate::model::index::IndexModel; 0] = [];
// NOTE: Executor tests use manual models to avoid schema macros.
static SIMPLE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::SimpleEntity",
    "SimpleEntity",
    &SIMPLE_FIELDS[0],
    &SIMPLE_FIELDS,
    &SIMPLE_INDEXES,
);

impl EntitySchema for SimpleEntity {
    const MODEL: &'static EntityModel = &SIMPLE_MODEL;
    const FIELDS: &'static [&'static str] = &SIMPLE_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &SIMPLE_INDEXES;
}

impl EntityPlacement for SimpleEntity {
    type DataStore = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for SimpleEntity {}

impl EntityValue for SimpleEntity {
    fn id(&self) -> Id<Self> {
        self.id
    }
}

// Clear the test data store and any pending commit marker between runs.
fn reset_store() {
    ensure_recovered_for_write(&DB).expect("write-side recovery should succeed");
    DATA_STORE.with(|store| store.borrow_mut().clear());
}

#[test]
fn executor_save_then_delete_round_trip() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);

    let entity = SimpleEntity {
        id: Id::from_storage_key(Ulid::generate()),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(saved.id().into_storage_key())
        .plan()
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.0.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete"
    );

    DB.with_data(|reg| {
        reg.with_store(TestDataStore::PATH, |store| {
            assert!(store.is_empty(), "store should be empty after delete");
        })
        .expect("store access should succeed");
    });
}

#[test]
fn delete_replays_incomplete_commit_marker() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);

    let entity = SimpleEntity {
        id: Id::from_storage_key(Ulid::generate()),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let marker = CommitMarker::new(CommitKind::Save, Vec::new(), Vec::new())
        .expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before delete"
    );

    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(saved.id().into_storage_key())
        .plan()
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.0.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete recovery"
    );
}
