mod cursor_validation;
mod lifecycle;
mod live_state;
mod paged_builder;
mod pagination;
mod semantics;
mod structural_trace;

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
    test_support::test_memory,
    traits::{
        AsView, CanisterKind, DataStoreKind, EntityIdentity, EntityKey, EntityKind,
        EntityPlacement, EntitySchema, EntityValue, Path, SanitizeAuto, SanitizeCustom,
        SingletonEntity, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Id, Ulid},
    value::Value,
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
