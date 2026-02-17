mod cursor_validation;
mod lifecycle;
mod live_state;
mod metrics;
mod paged_builder;
mod pagination;
mod semantics;
mod structural_trace;

use crate::{
    db::{
        Context, Db, DbSession, EntityRuntimeHooks,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered_for_write,
            init_commit_store_for_tests, prepare_row_commit_for_entity,
        },
        executor::{
            DeleteExecutor, LoadExecutor, SaveExecutor,
            trace::{
                QueryTraceEvent, QueryTraceSink, TracePhase, TracePushdownDecision,
                TracePushdownRejectionReason,
            },
        },
        index::IndexStore,
        query::{
            ReadConsistency,
            intent::{IntentError, Query, QueryError},
            plan::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
            predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        },
        relation::validate_delete_strong_relations_for_source,
        store::{DataStore, StoreRegistry},
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, RelationStrength},
        index::IndexModel,
    },
    test_fixtures::entity_model_from_static,
    test_support::test_memory,
    traits::{
        AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement, EntitySchema,
        EntityValue, Path, SanitizeAuto, SanitizeCustom, SingletonEntity, StoreKind, ValidateAuto,
        ValidateCustom, Visitable,
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

impl StoreKind for TestDataStore {
    type Canister = TestCanister;
}

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(0)));
    static INDEX_STORE: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(1)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(TestDataStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("test store registration should succeed");
        reg
    };
}

static DB: Db<TestCanister> = Db::new(&STORE_REGISTRY);

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

static SIMPLE_FIELDS: [FieldModel; 1] = [FieldModel {
    name: "id",
    kind: FieldKind::Ulid,
}];
static SIMPLE_FIELD_NAMES: [&str; 1] = ["id"];
static SIMPLE_INDEXES: [&IndexModel; 0] = [];
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
    const INDEXES: &'static [&'static IndexModel] = &SIMPLE_INDEXES;
}

impl EntityPlacement for SimpleEntity {
    type Store = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for SimpleEntity {}

impl EntityValue for SimpleEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// IndexedMetricsEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct IndexedMetricsEntity {
    id: Ulid,
    tag: u32,
    label: String,
}

impl AsView for IndexedMetricsEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for IndexedMetricsEntity {}
impl SanitizeCustom for IndexedMetricsEntity {}
impl ValidateAuto for IndexedMetricsEntity {}
impl ValidateCustom for IndexedMetricsEntity {}
impl Visitable for IndexedMetricsEntity {}

impl Path for IndexedMetricsEntity {
    const PATH: &'static str = "executor_tests::IndexedMetricsEntity";
}

impl EntityKey for IndexedMetricsEntity {
    type Key = Ulid;
}

impl EntityIdentity for IndexedMetricsEntity {
    const ENTITY_NAME: &'static str = "IndexedMetricsEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static INDEXED_METRICS_FIELDS: [FieldModel; 3] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "tag",
        kind: FieldKind::Uint,
    },
    FieldModel {
        name: "label",
        kind: FieldKind::Text,
    },
];
static INDEXED_METRICS_FIELD_NAMES: [&str; 3] = ["id", "tag", "label"];
static INDEXED_METRICS_INDEX_FIELDS: [&str; 1] = ["tag"];
static INDEXED_METRICS_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "tag",
    TestDataStore::PATH,
    &INDEXED_METRICS_INDEX_FIELDS,
    false,
)];
static INDEXED_METRICS_INDEXES: [&IndexModel; 1] = [&INDEXED_METRICS_INDEX_MODELS[0]];
static INDEXED_METRICS_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::IndexedMetricsEntity",
    "IndexedMetricsEntity",
    &INDEXED_METRICS_FIELDS[0],
    &INDEXED_METRICS_FIELDS,
    &INDEXED_METRICS_INDEXES,
);

impl EntitySchema for IndexedMetricsEntity {
    const MODEL: &'static EntityModel = &INDEXED_METRICS_MODEL;
    const FIELDS: &'static [&'static str] = &INDEXED_METRICS_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &INDEXED_METRICS_INDEXES;
}

impl EntityPlacement for IndexedMetricsEntity {
    type Store = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for IndexedMetricsEntity {}

impl EntityValue for IndexedMetricsEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// PushdownParityEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct PushdownParityEntity {
    id: Ulid,
    group: u32,
    rank: u32,
    label: String,
}

impl AsView for PushdownParityEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for PushdownParityEntity {}
impl SanitizeCustom for PushdownParityEntity {}
impl ValidateAuto for PushdownParityEntity {}
impl ValidateCustom for PushdownParityEntity {}
impl Visitable for PushdownParityEntity {}

impl Path for PushdownParityEntity {
    const PATH: &'static str = "executor_tests::PushdownParityEntity";
}

impl EntityKey for PushdownParityEntity {
    type Key = Ulid;
}

impl EntityIdentity for PushdownParityEntity {
    const ENTITY_NAME: &'static str = "PushdownParityEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static PUSHDOWN_PARITY_FIELDS: [FieldModel; 4] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "group",
        kind: FieldKind::Uint,
    },
    FieldModel {
        name: "rank",
        kind: FieldKind::Uint,
    },
    FieldModel {
        name: "label",
        kind: FieldKind::Text,
    },
];
static PUSHDOWN_PARITY_FIELD_NAMES: [&str; 4] = ["id", "group", "rank", "label"];
static PUSHDOWN_PARITY_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
static PUSHDOWN_PARITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "group_rank",
    TestDataStore::PATH,
    &PUSHDOWN_PARITY_INDEX_FIELDS,
    false,
)];
static PUSHDOWN_PARITY_INDEXES: [&IndexModel; 1] = [&PUSHDOWN_PARITY_INDEX_MODELS[0]];
static PUSHDOWN_PARITY_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::PushdownParityEntity",
    "PushdownParityEntity",
    &PUSHDOWN_PARITY_FIELDS[0],
    &PUSHDOWN_PARITY_FIELDS,
    &PUSHDOWN_PARITY_INDEXES,
);

impl EntitySchema for PushdownParityEntity {
    const MODEL: &'static EntityModel = &PUSHDOWN_PARITY_MODEL;
    const FIELDS: &'static [&'static str] = &PUSHDOWN_PARITY_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &PUSHDOWN_PARITY_INDEXES;
}

impl EntityPlacement for PushdownParityEntity {
    type Store = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for PushdownParityEntity {}

impl EntityValue for PushdownParityEntity {
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

static SINGLETON_UNIT_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Unit,
    },
    FieldModel {
        name: "label",
        kind: FieldKind::Text,
    },
];
static SINGLETON_UNIT_FIELD_NAMES: [&str; 2] = ["id", "label"];
static SINGLETON_UNIT_INDEXES: [&IndexModel; 0] = [];
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
    const INDEXES: &'static [&'static IndexModel] = &SINGLETON_UNIT_INDEXES;
}

impl EntityPlacement for SingletonUnitEntity {
    type Store = TestDataStore;
    type Canister = TestCanister;
}

impl EntityKind for SingletonUnitEntity {}
impl SingletonEntity for SingletonUnitEntity {}

impl EntityValue for SingletonUnitEntity {
    #[expect(clippy::unit_arg)]
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

static PHASE_TAG_KIND: FieldKind = FieldKind::Uint;
static PHASE_FIELDS: [FieldModel; 5] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        // Optional scalar fields are represented as scalar kinds in runtime models.
        name: "opt_rank",
        kind: FieldKind::Uint,
    },
    FieldModel {
        name: "rank",
        kind: FieldKind::Uint,
    },
    FieldModel {
        name: "tags",
        kind: FieldKind::List(&PHASE_TAG_KIND),
    },
    FieldModel {
        name: "label",
        kind: FieldKind::Text,
    },
];
static PHASE_FIELD_NAMES: [&str; 5] = ["id", "opt_rank", "rank", "tags", "label"];
static PHASE_INDEXES: [&IndexModel; 0] = [];
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
    const INDEXES: &'static [&'static IndexModel] = &PHASE_INDEXES;
}

impl EntityPlacement for PhaseEntity {
    type Store = TestDataStore;
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
    INDEX_STORE.with(|store| store.borrow_mut().clear());
}

///
/// RelationTestCanister
///

struct RelationTestCanister;

impl Path for RelationTestCanister {
    const PATH: &'static str = "executor_tests::RelationTestCanister";
}

impl CanisterKind for RelationTestCanister {}

///
/// RelationSourceStore
///

struct RelationSourceStore;

impl Path for RelationSourceStore {
    const PATH: &'static str = "executor_tests::RelationSourceStore";
}

impl StoreKind for RelationSourceStore {
    type Canister = RelationTestCanister;
}

///
/// RelationTargetStore
///

struct RelationTargetStore;

impl Path for RelationTargetStore {
    const PATH: &'static str = "executor_tests::RelationTargetStore";
}

impl StoreKind for RelationTargetStore {
    type Canister = RelationTestCanister;
}

thread_local! {
    static REL_SOURCE_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(40)));
    static REL_TARGET_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(41)));
    static REL_SOURCE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(42)));
    static REL_TARGET_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(43)));
    static REL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            RelationSourceStore::PATH,
            &REL_SOURCE_STORE,
            &REL_SOURCE_INDEX_STORE,
        )
        .expect("relation source store registration should succeed");
        reg.register_store(
            RelationTargetStore::PATH,
            &REL_TARGET_STORE,
            &REL_TARGET_INDEX_STORE,
        )
        .expect("relation target store registration should succeed");
        reg
    };
}

static REL_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RelationTestCanister>] = &[
    EntityRuntimeHooks::new(
        RelationTargetEntity::ENTITY_NAME,
        RelationTargetEntity::PATH,
        prepare_row_commit_for_entity::<RelationTargetEntity>,
        validate_delete_strong_relations_for_source::<RelationTargetEntity>,
    ),
    EntityRuntimeHooks::new(
        RelationSourceEntity::ENTITY_NAME,
        RelationSourceEntity::PATH,
        prepare_row_commit_for_entity::<RelationSourceEntity>,
        validate_delete_strong_relations_for_source::<RelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakSingleRelationSourceEntity::ENTITY_NAME,
        WeakSingleRelationSourceEntity::PATH,
        prepare_row_commit_for_entity::<WeakSingleRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakSingleRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakOptionalRelationSourceEntity::ENTITY_NAME,
        WeakOptionalRelationSourceEntity::PATH,
        prepare_row_commit_for_entity::<WeakOptionalRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakOptionalRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakListRelationSourceEntity::ENTITY_NAME,
        WeakListRelationSourceEntity::PATH,
        prepare_row_commit_for_entity::<WeakListRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakListRelationSourceEntity>,
    ),
];

static REL_DB: Db<RelationTestCanister> =
    Db::new_with_hooks(&REL_STORE_REGISTRY, REL_ENTITY_RUNTIME_HOOKS);

///
/// RelationTargetEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct RelationTargetEntity {
    id: Ulid,
}

impl AsView for RelationTargetEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RelationTargetEntity {}
impl SanitizeCustom for RelationTargetEntity {}
impl ValidateAuto for RelationTargetEntity {}
impl ValidateCustom for RelationTargetEntity {}
impl Visitable for RelationTargetEntity {}

impl Path for RelationTargetEntity {
    const PATH: &'static str = "executor_tests::RelationTargetEntity";
}

impl EntityKey for RelationTargetEntity {
    type Key = Ulid;
}

impl EntityIdentity for RelationTargetEntity {
    const ENTITY_NAME: &'static str = "RelationTargetEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static REL_TARGET_FIELDS: [FieldModel; 1] = [FieldModel {
    name: "id",
    kind: FieldKind::Ulid,
}];
static REL_TARGET_FIELD_NAMES: [&str; 1] = ["id"];
static REL_TARGET_INDEXES: [&IndexModel; 0] = [];
static REL_TARGET_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::RelationTargetEntity",
    "RelationTargetEntity",
    &REL_TARGET_FIELDS[0],
    &REL_TARGET_FIELDS,
    &REL_TARGET_INDEXES,
);

impl EntitySchema for RelationTargetEntity {
    const MODEL: &'static EntityModel = &REL_TARGET_MODEL;
    const FIELDS: &'static [&'static str] = &REL_TARGET_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &REL_TARGET_INDEXES;
}

impl EntityPlacement for RelationTargetEntity {
    type Store = RelationTargetStore;
    type Canister = RelationTestCanister;
}

impl EntityKind for RelationTargetEntity {}

impl EntityValue for RelationTargetEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// RelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct RelationSourceEntity {
    id: Ulid,
    target: Ulid,
}

impl AsView for RelationSourceEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for RelationSourceEntity {}
impl SanitizeCustom for RelationSourceEntity {}
impl ValidateAuto for RelationSourceEntity {}
impl ValidateCustom for RelationSourceEntity {}
impl Visitable for RelationSourceEntity {}

impl Path for RelationSourceEntity {
    const PATH: &'static str = "executor_tests::RelationSourceEntity";
}

impl EntityKey for RelationSourceEntity {
    type Key = Ulid;
}

impl EntityIdentity for RelationSourceEntity {
    const ENTITY_NAME: &'static str = "RelationSourceEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static REL_SOURCE_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "target",
        kind: FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: RelationTargetEntity::ENTITY_NAME,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Strong,
        },
    },
];
static REL_SOURCE_FIELD_NAMES: [&str; 2] = ["id", "target"];
static REL_SOURCE_INDEXES: [&IndexModel; 0] = [];
static REL_SOURCE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::RelationSourceEntity",
    "RelationSourceEntity",
    &REL_SOURCE_FIELDS[0],
    &REL_SOURCE_FIELDS,
    &REL_SOURCE_INDEXES,
);

impl EntitySchema for RelationSourceEntity {
    const MODEL: &'static EntityModel = &REL_SOURCE_MODEL;
    const FIELDS: &'static [&'static str] = &REL_SOURCE_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &REL_SOURCE_INDEXES;
}

impl EntityPlacement for RelationSourceEntity {
    type Store = RelationSourceStore;
    type Canister = RelationTestCanister;
}

impl EntityKind for RelationSourceEntity {}

impl EntityValue for RelationSourceEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// WeakSingleRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct WeakSingleRelationSourceEntity {
    id: Ulid,
    target: Ulid,
}

impl AsView for WeakSingleRelationSourceEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for WeakSingleRelationSourceEntity {}
impl SanitizeCustom for WeakSingleRelationSourceEntity {}
impl ValidateAuto for WeakSingleRelationSourceEntity {}
impl ValidateCustom for WeakSingleRelationSourceEntity {}
impl Visitable for WeakSingleRelationSourceEntity {}

impl Path for WeakSingleRelationSourceEntity {
    const PATH: &'static str = "executor_tests::WeakSingleRelationSourceEntity";
}

impl EntityKey for WeakSingleRelationSourceEntity {
    type Key = Ulid;
}

impl EntityIdentity for WeakSingleRelationSourceEntity {
    const ENTITY_NAME: &'static str = "WeakSingleRelationSourceEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static REL_WEAK_SINGLE_SOURCE_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "target",
        kind: FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: RelationTargetEntity::ENTITY_NAME,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Weak,
        },
    },
];
static REL_WEAK_SINGLE_SOURCE_FIELD_NAMES: [&str; 2] = ["id", "target"];
static REL_WEAK_SINGLE_SOURCE_INDEXES: [&IndexModel; 0] = [];
static REL_WEAK_SINGLE_SOURCE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::WeakSingleRelationSourceEntity",
    "WeakSingleRelationSourceEntity",
    &REL_WEAK_SINGLE_SOURCE_FIELDS[0],
    &REL_WEAK_SINGLE_SOURCE_FIELDS,
    &REL_WEAK_SINGLE_SOURCE_INDEXES,
);

impl EntitySchema for WeakSingleRelationSourceEntity {
    const MODEL: &'static EntityModel = &REL_WEAK_SINGLE_SOURCE_MODEL;
    const FIELDS: &'static [&'static str] = &REL_WEAK_SINGLE_SOURCE_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &REL_WEAK_SINGLE_SOURCE_INDEXES;
}

impl EntityPlacement for WeakSingleRelationSourceEntity {
    type Store = RelationSourceStore;
    type Canister = RelationTestCanister;
}

impl EntityKind for WeakSingleRelationSourceEntity {}

impl EntityValue for WeakSingleRelationSourceEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// WeakOptionalRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct WeakOptionalRelationSourceEntity {
    id: Ulid,
    target: Option<Ulid>,
}

impl AsView for WeakOptionalRelationSourceEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for WeakOptionalRelationSourceEntity {}
impl SanitizeCustom for WeakOptionalRelationSourceEntity {}
impl ValidateAuto for WeakOptionalRelationSourceEntity {}
impl ValidateCustom for WeakOptionalRelationSourceEntity {}
impl Visitable for WeakOptionalRelationSourceEntity {}

impl Path for WeakOptionalRelationSourceEntity {
    const PATH: &'static str = "executor_tests::WeakOptionalRelationSourceEntity";
}

impl EntityKey for WeakOptionalRelationSourceEntity {
    type Key = Ulid;
}

impl EntityIdentity for WeakOptionalRelationSourceEntity {
    const ENTITY_NAME: &'static str = "WeakOptionalRelationSourceEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static REL_WEAK_OPTIONAL_SOURCE_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "target",
        kind: FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: RelationTargetEntity::ENTITY_NAME,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Weak,
        },
    },
];
static REL_WEAK_OPTIONAL_SOURCE_FIELD_NAMES: [&str; 2] = ["id", "target"];
static REL_WEAK_OPTIONAL_SOURCE_INDEXES: [&IndexModel; 0] = [];
static REL_WEAK_OPTIONAL_SOURCE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::WeakOptionalRelationSourceEntity",
    "WeakOptionalRelationSourceEntity",
    &REL_WEAK_OPTIONAL_SOURCE_FIELDS[0],
    &REL_WEAK_OPTIONAL_SOURCE_FIELDS,
    &REL_WEAK_OPTIONAL_SOURCE_INDEXES,
);

impl EntitySchema for WeakOptionalRelationSourceEntity {
    const MODEL: &'static EntityModel = &REL_WEAK_OPTIONAL_SOURCE_MODEL;
    const FIELDS: &'static [&'static str] = &REL_WEAK_OPTIONAL_SOURCE_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &REL_WEAK_OPTIONAL_SOURCE_INDEXES;
}

impl EntityPlacement for WeakOptionalRelationSourceEntity {
    type Store = RelationSourceStore;
    type Canister = RelationTestCanister;
}

impl EntityKind for WeakOptionalRelationSourceEntity {}

impl EntityValue for WeakOptionalRelationSourceEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// WeakListRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct WeakListRelationSourceEntity {
    id: Ulid,
    targets: Vec<Ulid>,
}

impl AsView for WeakListRelationSourceEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for WeakListRelationSourceEntity {}
impl SanitizeCustom for WeakListRelationSourceEntity {}
impl ValidateAuto for WeakListRelationSourceEntity {}
impl ValidateCustom for WeakListRelationSourceEntity {}
impl Visitable for WeakListRelationSourceEntity {}

impl Path for WeakListRelationSourceEntity {
    const PATH: &'static str = "executor_tests::WeakListRelationSourceEntity";
}

impl EntityKey for WeakListRelationSourceEntity {
    type Key = Ulid;
}

impl EntityIdentity for WeakListRelationSourceEntity {
    const ENTITY_NAME: &'static str = "WeakListRelationSourceEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static REL_WEAK_LIST_TARGET_KIND: FieldKind = FieldKind::Relation {
    target_path: RelationTargetEntity::PATH,
    target_entity_name: RelationTargetEntity::ENTITY_NAME,
    target_store_path: RelationTargetStore::PATH,
    key_kind: &FieldKind::Ulid,
    strength: RelationStrength::Weak,
};
static REL_WEAK_LIST_SOURCE_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "targets",
        kind: FieldKind::List(&REL_WEAK_LIST_TARGET_KIND),
    },
];
static REL_WEAK_LIST_SOURCE_FIELD_NAMES: [&str; 2] = ["id", "targets"];
static REL_WEAK_LIST_SOURCE_INDEXES: [&IndexModel; 0] = [];
static REL_WEAK_LIST_SOURCE_MODEL: EntityModel = entity_model_from_static(
    "executor_tests::WeakListRelationSourceEntity",
    "WeakListRelationSourceEntity",
    &REL_WEAK_LIST_SOURCE_FIELDS[0],
    &REL_WEAK_LIST_SOURCE_FIELDS,
    &REL_WEAK_LIST_SOURCE_INDEXES,
);

impl EntitySchema for WeakListRelationSourceEntity {
    const MODEL: &'static EntityModel = &REL_WEAK_LIST_SOURCE_MODEL;
    const FIELDS: &'static [&'static str] = &REL_WEAK_LIST_SOURCE_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &REL_WEAK_LIST_SOURCE_INDEXES;
}

impl EntityPlacement for WeakListRelationSourceEntity {
    type Store = RelationSourceStore;
    type Canister = RelationTestCanister;
}

impl EntityKind for WeakListRelationSourceEntity {}

impl EntityValue for WeakListRelationSourceEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

// Clear relation test stores and any pending commit marker between runs.
fn reset_relation_stores() {
    ensure_recovered_for_write(&REL_DB).expect("relation write-side recovery should succeed");
    REL_DB.with_store_registry(|reg| {
        reg.try_get_store(RelationSourceStore::PATH)
            .map(|store| {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
            })
            .expect("relation source store access should succeed");
        reg.try_get_store(RelationTargetStore::PATH)
            .map(|store| {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
            })
            .expect("relation target store access should succeed");
    });
}
