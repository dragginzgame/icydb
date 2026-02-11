use crate::{
    db::{
        Context, Db,
        commit::{
            CommitKind, CommitMarker, begin_commit, commit_marker_present,
            ensure_recovered_for_write, init_commit_store_for_tests,
        },
        executor::{
            DeleteExecutor, LoadExecutor, SaveExecutor,
            trace::{QueryTraceEvent, QueryTraceSink, TracePhase},
        },
        query::{
            Query, ReadConsistency,
            predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        },
        store::{DataStore, DataStoreRegistry},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
    },
    test_fixtures::entity_model_from_static,
    traits::{
        AsView, CanisterKind, DataStoreKind, EntityIdentity, EntityKey, EntityKind,
        EntityPlacement, EntitySchema, EntityValue, Path, SanitizeAuto, SanitizeCustom,
        SingletonEntity, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Id, Ulid},
    value::Value,
};
use canic_cdk::structures::{
    DefaultMemoryImpl,
    memory::{MemoryId, MemoryManager, VirtualMemory},
};
use icydb_derive::FieldValues;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, sync::Mutex};

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

static TRACE_EVENTS: Mutex<Vec<QueryTraceEvent>> = Mutex::new(Vec::new());
static TEST_TRACE_SINK: TestTraceSink = TestTraceSink;

///
/// TestTraceSink
///

struct TestTraceSink;

impl QueryTraceSink for TestTraceSink {
    fn on_event(&self, event: QueryTraceEvent) {
        TRACE_EVENTS
            .lock()
            .expect("trace event lock should succeed")
            .push(event);
    }
}

// Clear and return all buffered trace events for structural assertions.
fn take_trace_events() -> Vec<QueryTraceEvent> {
    let mut events = TRACE_EVENTS
        .lock()
        .expect("trace event lock should succeed");
    let out = events.clone();
    events.clear();
    out
}

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
    id: Ulid,
}

impl AsView for SimpleEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
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

impl EntityKey for SimpleEntity {
    type Key = Ulid;
}

impl EntityIdentity for SimpleEntity {
    const ENTITY_NAME: &'static str = "SimpleEntity";
    const PRIMARY_KEY: &'static str = "id";
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
        Id::from_key(self.id)
    }
}

///
/// SingletonUnitEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct SingletonUnitEntity {
    id: (),
    label: String,
}

impl AsView for SingletonUnitEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for SingletonUnitEntity {}
impl SanitizeCustom for SingletonUnitEntity {}
impl ValidateAuto for SingletonUnitEntity {}
impl ValidateCustom for SingletonUnitEntity {}
impl Visitable for SingletonUnitEntity {}

impl Path for SingletonUnitEntity {
    const PATH: &'static str = "executor_tests::SingletonUnitEntity";
}

impl EntityKey for SingletonUnitEntity {
    type Key = ();
}

impl EntityIdentity for SingletonUnitEntity {
    const ENTITY_NAME: &'static str = "SingletonUnitEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static SINGLETON_UNIT_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Unit,
    },
    EntityFieldModel {
        name: "label",
        kind: EntityFieldKind::Text,
    },
];
static SINGLETON_UNIT_FIELD_NAMES: [&str; 2] = ["id", "label"];
static SINGLETON_UNIT_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static SINGLETON_UNIT_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::SingletonUnitEntity",
    "SingletonUnitEntity",
    &SINGLETON_UNIT_FIELDS[0],
    &SINGLETON_UNIT_FIELDS,
    &SINGLETON_UNIT_INDEXES,
);

impl EntitySchema for SingletonUnitEntity {
    const MODEL: &'static EntityModel = &SINGLETON_UNIT_MODEL;
    const FIELDS: &'static [&'static str] = &SINGLETON_UNIT_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &SINGLETON_UNIT_INDEXES;
}

impl EntityPlacement for SingletonUnitEntity {
    type DataStore = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for SingletonUnitEntity {}
impl SingletonEntity for SingletonUnitEntity {}

impl EntityValue for SingletonUnitEntity {
    #[allow(clippy::unit_arg)]
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// PhaseEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct PhaseEntity {
    id: Ulid,
    opt_rank: Option<u32>,
    rank: u32,
    tags: Vec<u32>,
    label: String,
}

impl AsView for PhaseEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for PhaseEntity {}
impl SanitizeCustom for PhaseEntity {}
impl ValidateAuto for PhaseEntity {}
impl ValidateCustom for PhaseEntity {}
impl Visitable for PhaseEntity {}

impl Path for PhaseEntity {
    const PATH: &'static str = "executor_tests::PhaseEntity";
}

impl EntityKey for PhaseEntity {
    type Key = Ulid;
}

impl EntityIdentity for PhaseEntity {
    const ENTITY_NAME: &'static str = "PhaseEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static PHASE_TAG_KIND: EntityFieldKind = EntityFieldKind::Uint;
static PHASE_FIELDS: [EntityFieldModel; 5] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        // Optional scalar fields are represented as scalar kinds in runtime models.
        name: "opt_rank",
        kind: EntityFieldKind::Uint,
    },
    EntityFieldModel {
        name: "rank",
        kind: EntityFieldKind::Uint,
    },
    EntityFieldModel {
        name: "tags",
        kind: EntityFieldKind::List(&PHASE_TAG_KIND),
    },
    EntityFieldModel {
        name: "label",
        kind: EntityFieldKind::Text,
    },
];
static PHASE_FIELD_NAMES: [&str; 5] = ["id", "opt_rank", "rank", "tags", "label"];
static PHASE_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static PHASE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::PhaseEntity",
    "PhaseEntity",
    &PHASE_FIELDS[0],
    &PHASE_FIELDS,
    &PHASE_INDEXES,
);

impl EntitySchema for PhaseEntity {
    const MODEL: &'static EntityModel = &PHASE_MODEL;
    const FIELDS: &'static [&'static str] = &PHASE_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &PHASE_INDEXES;
}

impl EntityPlacement for PhaseEntity {
    type DataStore = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for PhaseEntity {}

impl EntityValue for PhaseEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
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
        id: Ulid::generate(),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(saved.id().key())
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
        id: Ulid::generate(),
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
        .by_id(saved.id().key())
        .plan()
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.0.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete recovery"
    );
}

#[test]
fn load_replays_incomplete_commit_marker_after_startup_recovery() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let marker = CommitMarker::new(CommitKind::Save, Vec::new(), Vec::new())
        .expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before load"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .plan()
        .expect("load plan should build");
    let response = load.execute(plan).expect("load should succeed");

    assert!(
        response.0.is_empty(),
        "empty store should still load after recovery replay"
    );
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after read recovery"
    );
}

#[test]
fn load_applies_order_and_pagination() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [3_u128, 1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .offset(1)
        .plan()
        .expect("load plan should build");

    let response = load.execute(plan).expect("load should succeed");
    assert_eq!(response.0.len(), 1, "pagination should return one row");
    assert_eq!(
        response.0[0].1.id,
        Ulid::from_u128(2),
        "pagination should run after canonical ordering by id"
    );
}

#[test]
fn singleton_unit_key_insert_and_only_load_round_trip() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SingletonUnitEntity>::new(DB, false);
    let load = LoadExecutor::<SingletonUnitEntity>::new(DB, false);
    let expected = SingletonUnitEntity {
        id: (),
        label: "project".to_string(),
    };

    save.insert(expected.clone())
        .expect("singleton save should succeed");

    let plan = Query::<SingletonUnitEntity>::new(ReadConsistency::MissingOk)
        .only()
        .plan()
        .expect("singleton load plan should build");
    let response = load.execute(plan).expect("singleton load should succeed");

    assert_eq!(
        response.0.len(),
        1,
        "singleton only() should match exactly one row"
    );
    assert_eq!(
        response.0[0].1, expected,
        "loaded singleton should match inserted row"
    );
}

#[test]
fn delete_applies_order_and_delete_limit() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [30_u128, 10_u128, 20_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .order_by("id")
        .limit(1)
        .plan()
        .expect("delete plan should build");

    let response = delete.execute(plan).expect("delete should succeed");
    assert_eq!(response.0.len(), 1, "delete limit should remove one row");
    assert_eq!(
        response.0[0].1.id,
        Ulid::from_u128(10),
        "delete limit should run after canonical ordering by id"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let remaining_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should succeed");
    let remaining_ids: Vec<Ulid> = remaining
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
        .collect();

    assert_eq!(
        remaining_ids,
        vec![Ulid::from_u128(20), Ulid::from_u128(30)],
        "only the first ordered row should have been deleted"
    );
}

#[test]
fn load_filter_after_access_with_optional_equality() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    let id = Ulid::from_u128(501);
    save.insert(PhaseEntity {
        id,
        opt_rank: Some(7),
        rank: 7,
        tags: vec![1, 2, 3],
        label: "alpha".to_string(),
    })
    .expect("save should succeed");

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let equals_opt_value = Predicate::Compare(ComparePredicate::with_coercion(
        "opt_rank",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let match_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(equals_opt_value)
        .plan()
        .expect("optional equality plan should build");
    let match_response = load
        .execute(match_plan)
        .expect("optional equality should load");
    assert_eq!(
        match_response.0.len(),
        1,
        "filter should run after by_id access and keep matching rows"
    );

    let no_match = Predicate::Compare(ComparePredicate::with_coercion(
        "opt_rank",
        CompareOp::Eq,
        Value::Uint(99),
        CoercionId::Strict,
    ));
    let mismatch_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(no_match)
        .plan()
        .expect("mismatch plan should build");
    let mismatch_response = load
        .execute(mismatch_plan)
        .expect("mismatch predicate should execute");
    assert_eq!(
        mismatch_response.0.len(),
        0,
        "filter should be applied after access and drop non-matching rows"
    );
}

#[test]
fn load_in_and_text_ops_respect_ordered_pagination() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(601),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 3],
            label: "needle alpha".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(602),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![2],
            label: "other".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(603),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![9],
            label: "NEEDLE beta".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(604),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![4],
            label: "needle gamma".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(vec![Value::Uint(20), Value::Uint(30), Value::Uint(40)]),
            CoercionId::Strict,
        )),
        Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("needle".to_string()),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("in+text ordered page plan should build");
    let response = load
        .execute(plan)
        .expect("in+text ordered page should load");

    assert_eq!(
        response.0.len(),
        1,
        "ordered pagination should return one row"
    );
    assert_eq!(
        response.0[0].1.rank, 30,
        "pagination should apply to the filtered+ordered window"
    );
}

#[test]
fn load_contains_filters_after_by_id_access() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    let id = Ulid::from_u128(701);
    save.insert(PhaseEntity {
        id,
        opt_rank: Some(1),
        rank: 1,
        tags: vec![2, 9],
        label: "contains".to_string(),
    })
    .expect("save should succeed");

    let contains_nine = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        CompareOp::Contains,
        Value::Uint(9),
        CoercionId::CollectionElement,
    ));
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let hit_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(contains_nine)
        .plan()
        .expect("contains hit plan should build");
    let hit = load.execute(hit_plan).expect("contains hit should execute");
    assert_eq!(hit.0.len(), 1, "contains predicate should match row");

    let contains_missing = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        CompareOp::Contains,
        Value::Uint(8),
        CoercionId::CollectionElement,
    ));
    let miss_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(contains_missing)
        .plan()
        .expect("contains miss plan should build");
    let miss = load
        .execute(miss_plan)
        .expect("contains miss should execute");
    assert_eq!(
        miss.0.len(),
        0,
        "contains predicate should filter out non-matching rows after access"
    );
}

#[test]
fn delete_limit_applies_to_filtered_rows_only() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(801),
            opt_rank: Some(1),
            rank: 1,
            tags: vec![1],
            label: "keep-low-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(802),
            opt_rank: Some(2),
            rank: 2,
            tags: vec![2],
            label: "keep-low-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(803),
            opt_rank: Some(100),
            rank: 100,
            tags: vec![3],
            label: "delete-first".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(804),
            opt_rank: Some(200),
            rank: 200,
            tags: vec![4],
            label: "delete-second".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Gte,
        Value::Uint(100),
        CoercionId::NumericWiden,
    ));
    let delete = DeleteExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("filtered delete plan should build");
    let deleted = delete
        .execute(plan)
        .expect("filtered delete should execute");

    assert_eq!(
        deleted.0.len(),
        1,
        "delete limit should remove one filtered row"
    );
    assert_eq!(
        deleted.0[0].1.rank, 100,
        "delete limit should apply after filtering+ordering"
    );

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let remaining_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .plan()
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should execute");
    let remaining_ranks: Vec<u64> = remaining
        .0
        .into_iter()
        .map(|(_, entity)| u64::from(entity.rank))
        .collect();

    assert_eq!(
        remaining_ranks,
        vec![1, 2, 200],
        "only one row from the filtered window should be deleted"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn load_structural_guard_emits_post_access_phase_and_stats() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(901),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 3],
            label: "needle alpha".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(902),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![2],
            label: "other".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(903),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![9],
            label: "NEEDLE beta".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(904),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![4],
            label: "needle gamma".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(vec![Value::Uint(20), Value::Uint(30), Value::Uint(40)]),
            CoercionId::Strict,
        )),
        Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("needle".to_string()),
        },
    ]);

    let plan_for_stats = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("structural stats plan should build");
    let plan_for_execute = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("structural execute plan should build");

    // Structural assertion: post-access stats must report all load phases were applied.
    let logical = plan_for_stats.into_inner();
    let ctx = DB
        .recovered_context::<PhaseEntity>()
        .expect("recovered context should succeed");
    let data_rows = ctx
        .rows_from_access_plan(&logical.access, logical.consistency)
        .expect("access rows should load");
    let mut rows = Context::deserialize_rows(data_rows).expect("rows should deserialize");
    let stats = logical
        .apply_post_access::<PhaseEntity, _>(&mut rows)
        .expect("post-access should apply");
    assert!(stats.filtered, "filter phase should be applied");
    assert!(stats.ordered, "order phase should be applied");
    assert!(stats.paged, "pagination phase should be applied");
    assert!(
        !stats.delete_was_limited,
        "delete limit must remain inactive on load plans"
    );
    assert_eq!(stats.rows_after_filter, 3, "filter should keep three rows");
    assert_eq!(
        stats.rows_after_order, 3,
        "ordering should preserve row count"
    );
    assert_eq!(
        stats.rows_after_page, 1,
        "pagination should trim to one row"
    );
    assert_eq!(
        stats.rows_after_delete_limit, 1,
        "load plans should not apply delete limits"
    );

    // Runtime assertion: executor output and trace phase must both reflect post-access execution.
    let _ = take_trace_events();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let response = load
        .execute(plan_for_execute)
        .expect("structural load should execute");
    assert_eq!(response.0.len(), 1, "post-access output should be paged");
    assert_eq!(
        response.0[0].1.rank, 30,
        "paged row should come from filtered+ordered post-access window"
    );

    let events = take_trace_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            QueryTraceEvent::Phase {
                phase: TracePhase::PostAccess,
                rows: 1,
                ..
            }
        )),
        "trace must include post-access phase with final row count"
    );
}
