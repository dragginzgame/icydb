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
        Context, Db, DbSession, PreparedRowCommitOp, RowCommitHandler,
        StrongRelationDeleteValidator,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered_for_write,
            init_commit_store_for_tests,
        },
        executor::{
            DeleteExecutor, LoadExecutor, SaveExecutor,
            trace::{QueryTraceEvent, QueryTraceSink, TracePhase},
        },
        index::IndexStore,
        query::{
            IntentError, Query, QueryError, ReadConsistency,
            plan::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
            predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        },
        store::{DataStore, RawDataKey, StoreRegistry},
        validate_delete_strong_relations_for_source,
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel, RelationStrength},
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
use std::{cell::RefCell, collections::BTreeSet, sync::Mutex};

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

static INDEXED_METRICS_FIELDS: [EntityFieldModel; 3] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "tag",
        kind: EntityFieldKind::Uint,
    },
    EntityFieldModel {
        name: "label",
        kind: EntityFieldKind::Text,
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
    type Store = TestDataStore;
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

// Route source-entity relation scanning through the generic delete-side RI helper.
fn validate_relation_source_delete_refs(
    db: &Db<RelationTestCanister>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), crate::error::InternalError> {
    validate_delete_strong_relations_for_source::<RelationSourceEntity>(
        db,
        target_path,
        deleted_target_keys,
    )
}

static REL_DELETE_RELATION_VALIDATORS: &[StrongRelationDeleteValidator<RelationTestCanister>] =
    &[StrongRelationDeleteValidator::new(
        validate_relation_source_delete_refs,
    )];

fn prepare_relation_target_row_op(
    db: &Db<RelationTestCanister>,
    op: &crate::db::CommitRowOp,
) -> Result<PreparedRowCommitOp, crate::error::InternalError> {
    crate::db::prepare_row_commit_for_entity::<RelationTargetEntity>(db, op)
}

fn prepare_relation_source_row_op(
    db: &Db<RelationTestCanister>,
    op: &crate::db::CommitRowOp,
) -> Result<PreparedRowCommitOp, crate::error::InternalError> {
    crate::db::prepare_row_commit_for_entity::<RelationSourceEntity>(db, op)
}

static REL_ROW_COMMIT_HANDLERS: &[RowCommitHandler<RelationTestCanister>] = &[
    RowCommitHandler::new(RelationTargetEntity::PATH, prepare_relation_target_row_op),
    RowCommitHandler::new(RelationSourceEntity::PATH, prepare_relation_source_row_op),
];

static REL_DB: Db<RelationTestCanister> = Db::new_with_relations(
    &REL_STORE_REGISTRY,
    REL_DELETE_RELATION_VALIDATORS,
    REL_ROW_COMMIT_HANDLERS,
);

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

static REL_TARGET_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
static REL_TARGET_FIELD_NAMES: [&str; 1] = ["id"];
static REL_TARGET_INDEXES: [&crate::model::index::IndexModel; 0] = [];
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
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &REL_TARGET_INDEXES;
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

static REL_SOURCE_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "target",
        kind: EntityFieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: RelationTargetEntity::ENTITY_NAME,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &EntityFieldKind::Ulid,
            strength: RelationStrength::Strong,
        },
    },
];
static REL_SOURCE_FIELD_NAMES: [&str; 2] = ["id", "target"];
static REL_SOURCE_INDEXES: [&crate::model::index::IndexModel; 0] = [];
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
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &REL_SOURCE_INDEXES;
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
