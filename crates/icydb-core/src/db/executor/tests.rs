use crate::{
    db::{
        Context, Db, DbSession,
        commit::{
            CommitKind, CommitMarker, begin_commit, commit_marker_present,
            ensure_recovered_for_write, init_commit_store_for_tests,
        },
        executor::{
            DeleteExecutor, LoadExecutor, SaveExecutor,
            trace::{QueryTraceEvent, QueryTraceSink, TracePhase},
        },
        query::{
            IntentError, Query, QueryError, ReadConsistency,
            plan::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
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
fn load_cursor_pagination_skips_strictly_before_limit() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1100),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1101),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1102),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1103),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor_boundary(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1100));

    let cursor1 = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit a continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(cursor1.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(1101),
        "cursor boundary must be applied before limit using strict ordering"
    );

    let cursor2 = page2
        .next_cursor
        .as_ref()
        .expect("page2 should emit a continuation cursor");
    let page3_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page3 plan should build");
    let page3_boundary = page3_plan
        .plan_cursor_boundary(Some(cursor2.as_slice()))
        .expect("cursor page3 boundary should plan");
    let page3 = load
        .execute_paged(page3_plan, page3_boundary)
        .expect("cursor page3 should execute");
    assert_eq!(page3.items.0.len(), 1, "page3 should return one row");
    assert_eq!(
        page3.items.0[0].1.id,
        Ulid::from_u128(1102),
        "strict cursor continuation must advance beyond the last returned row"
    );
}

#[test]
fn load_cursor_next_cursor_uses_last_returned_row_boundary() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1200),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1201),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1202),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1203),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor next-cursor plan should build");
    let page1_boundary = page1_plan
        .plan_cursor_boundary(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 2, "page1 should return two rows");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1200));
    assert_eq!(
        page1.items.0[1].1.id,
        Ulid::from_u128(1201),
        "page1 second row should be the PK tie-break winner for rank=20"
    );

    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("page1 should include next cursor");
    let token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("continuation cursor should decode");
    let comparison_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("comparison plan should build")
        .into_inner();
    let expected_boundary = comparison_plan
        .cursor_boundary_from_entity(&page1.items.0[1].1)
        .expect("expected boundary should build");
    assert_eq!(
        token.boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row boundary"
    );

    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(cursor_bytes.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1202), Ulid::from_u128(1203)],
        "page2 should resume strictly after page1's final row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_desc_order_resumes_strictly_after_boundary() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1400),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1401),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1402),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1403),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor_boundary(None)
        .expect("descending page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("descending page1 should execute");
    let page1_ids: Vec<Ulid> = page1.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        page1_ids,
        vec![Ulid::from_u128(1403), Ulid::from_u128(1401)],
        "descending page1 should apply rank DESC then canonical PK tie-break"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("descending page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect("descending page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("descending page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1402), Ulid::from_u128(1400)],
        "descending continuation must resume strictly after the boundary row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_rejects_signature_mismatch() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1300),
            opt_rank: Some(1),
            rank: 1,
            tags: vec![1],
            label: "a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1301),
            opt_rank: Some(2),
            rank: 2,
            tags: vec![2],
            label: "b".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let asc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("ascending cursor plan should build");
    let asc_boundary = asc_plan
        .plan_cursor_boundary(None)
        .expect("ascending boundary should plan");
    let asc_page = load
        .execute_paged(asc_plan, asc_boundary)
        .expect("ascending cursor page should execute");
    let cursor = asc_page
        .next_cursor
        .expect("ascending page should emit cursor");

    let desc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(1)
        .plan()
        .expect("descending plan should build");
    let err = desc_plan
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect_err("cursor from different canonical plan should be rejected");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "planning should reject plan-signature mismatch"
    );
}

#[test]
fn paged_query_builder_requires_explicit_limit() {
    let session = DbSession::new(DB);

    let Err(err) = session.load::<PhaseEntity>().order_by("rank").page() else {
        panic!("paged builder should require explicit limit")
    };

    assert!(
        matches!(err, QueryError::Intent(IntentError::CursorRequiresLimit)),
        "missing limit should be rejected at page-builder boundary"
    );
}

#[test]
fn paged_query_builder_rejects_offset() {
    let session = DbSession::new(DB);

    let Err(err) = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(10)
        .offset(2)
        .page()
    else {
        panic!("paged builder should reject offset usage")
    };

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorWithOffsetUnsupported)
        ),
        "offset should be rejected at page-builder boundary"
    );
}

#[test]
fn paged_query_builder_accepts_order_and_limit() {
    let session = DbSession::new(DB);

    session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept canonical cursor pagination intent");
}

#[test]
fn load_cursor_rejects_version_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("version-mismatch plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2001))),
        ],
    };
    let token = ContinuationToken::new(plan.continuation_signature(), boundary);
    let version_mismatch_cursor = token
        .encode_with_version_for_test(99)
        .expect("version-mismatch cursor should encode");

    let err = plan
        .plan_cursor_boundary(Some(version_mismatch_cursor.as_slice()))
        .expect_err("unsupported cursor version should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorVersionMismatch { version: 99 }
        ),
        "planning should reject unsupported cursor versions"
    );
}

#[test]
fn load_cursor_rejects_boundary_value_type_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("boundary-type plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Text("not-a-rank".to_string())),
            CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2002))),
        ],
    };
    let cursor = ContinuationToken::new(plan.continuation_signature(), boundary)
        .encode()
        .expect("boundary-type cursor should encode");

    let err = plan
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect_err("boundary field type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorBoundaryTypeMismatch { field, .. }
            if field == "rank"
        ),
        "planning should reject non-PK boundary type mismatches"
    );
}

#[test]
fn load_cursor_rejects_primary_key_type_mismatch_at_plan_time() {
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("pk-type plan should build");
    let boundary = CursorBoundary {
        slots: vec![
            CursorBoundarySlot::Present(Value::Uint(10)),
            CursorBoundarySlot::Present(Value::Text("not-a-ulid".to_string())),
        ],
    };
    let cursor = ContinuationToken::new(plan.continuation_signature(), boundary)
        .encode()
        .expect("pk-type cursor should encode");

    let err = plan
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect_err("pk type mismatch should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorPrimaryKeyTypeMismatch { field, .. }
            if field == "id"
        ),
        "planning should reject primary-key boundary type mismatches"
    );
}

#[test]
fn load_cursor_rejects_wrong_entity_path_at_plan_time() {
    let foreign_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("foreign entity plan should build");
    let foreign_cursor = ContinuationToken::new(
        foreign_plan.continuation_signature(),
        CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
                3001,
            )))],
        },
    )
    .encode()
    .expect("foreign entity cursor should encode");

    let local_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("local entity plan should build");
    let err = local_plan
        .plan_cursor_boundary(Some(foreign_cursor.as_slice()))
        .expect_err("cursor from a different entity path should be rejected during planning");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "planning should reject wrong-entity cursors via plan-signature mismatch"
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
fn load_ordering_treats_missing_values_consistently_with_direction() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(902),
            opt_rank: None,
            rank: 2,
            tags: vec![2],
            label: "missing-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(901),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "missing-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(903),
            opt_rank: Some(10),
            rank: 3,
            tags: vec![3],
            label: "present-10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(904),
            opt_rank: Some(20),
            rank: 4,
            tags: vec![4],
            label: "present-20".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let asc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("opt_rank")
        .plan()
        .expect("ascending optional-order plan should build");
    let asc = load
        .execute(asc_plan)
        .expect("ascending optional-order query should execute");
    let asc_ids: Vec<Ulid> = asc.0.into_iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        asc_ids,
        vec![
            Ulid::from_u128(901),
            Ulid::from_u128(902),
            Ulid::from_u128(903),
            Ulid::from_u128(904),
        ],
        "ascending order should treat missing as lowest and use PK tie-break within missing rows"
    );

    let desc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("opt_rank")
        .plan()
        .expect("descending optional-order plan should build");
    let desc = load
        .execute(desc_plan)
        .expect("descending optional-order query should execute");
    let desc_ids: Vec<Ulid> = desc.0.into_iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        desc_ids,
        vec![
            Ulid::from_u128(904),
            Ulid::from_u128(903),
            Ulid::from_u128(901),
            Ulid::from_u128(902),
        ],
        "descending order should reverse present/missing groups while preserving PK tie-break"
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
