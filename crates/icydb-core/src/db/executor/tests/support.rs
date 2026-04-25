//! Module: db::executor::tests::support
//! Owns shared executor test fixtures, stores, entity schemas, and reset
//! helpers reused across the topical executor owner suites.
//! Does not own: the topical assertions themselves.
//! Boundary: keeps reusable executor test support out of the owner `mod.rs`
//! wiring file.

pub(in crate::db::executor::tests) use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered,
            init_commit_store_for_tests, prepare_row_commit_for_entity_with_structural_readers,
        },
        data::DataStore,
        executor::{
            DeleteExecutor, LoadExecutor, PreparedExecutionPlan, SaveExecutor,
            ScalarTerminalBoundaryRequest,
        },
        index::IndexStore,
        predicate::MissingRowPolicy,
        query::intent::Query,
        registry::StoreRegistry,
        relation::validate_delete_strong_relations_for_source,
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    model::{
        field::{FieldKind, RelationStrength},
        index::IndexModel,
    },
    testing::test_memory,
    traits::{EntityKind, EntityValue, Path},
    types::{Ulid, Unit},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cell::RefCell;

///
/// ScanBudgetCaptureSink
///
/// Small metrics sink shared by executor owner tests that assert row-scan
/// budgets.
///

#[derive(Default)]
pub(in crate::db::executor::tests) struct ScanBudgetCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl ScanBudgetCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for ScanBudgetCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

// Sum `RowsScanned` metrics for one entity path under the shared executor test sink.
pub(in crate::db::executor::tests) fn rows_scanned_for_entity(
    events: &[MetricsEvent],
    entity_path: &'static str,
) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let scanned = match event {
            MetricsEvent::RowsScanned {
                entity_path: path,
                rows_scanned,
            } if *path == entity_path => usize::try_from(*rows_scanned).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(scanned)
    })
}

// Run one closure under the shared executor test sink and return both output and
// scanned-row total for the requested entity path.
pub(in crate::db::executor::tests) fn capture_rows_scanned_for_entity<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, usize) {
    let sink = ScanBudgetCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

// Execute one shared COUNT scalar terminal from executor owner tests.
pub(in crate::db::executor::tests) fn execute_count_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Count)?
        .into_count()
}

// Execute one shared EXISTS scalar terminal from executor owner tests.
pub(in crate::db::executor::tests) fn execute_exists_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<bool, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Exists)?
        .into_exists()
}

// TestCanister

pub(in crate::db::executor::tests) struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = concat!(module_path!(), "::TestCanister");
}

impl crate::traits::CanisterKind for TestCanister {
    const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
}

// TestDataStore

pub(in crate::db::executor::tests) struct TestDataStore;

impl Path for TestDataStore {
    const PATH: &'static str = concat!(module_path!(), "::TestDataStore");
}

impl crate::traits::StoreKind for TestDataStore {
    type Canister = TestCanister;
}

thread_local! {
    pub(in crate::db::executor::tests) static DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(0)));
    pub(in crate::db::executor::tests) static INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(1)));
    pub(in crate::db::executor::tests) static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(TestDataStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("test store registration should succeed");
        reg
    };
}

pub(in crate::db::executor::tests) static DB: Db<TestCanister> = Db::new(&STORE_REGISTRY);

///
/// SimpleEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct SimpleEntity {
    pub(in crate::db::executor::tests) id: Ulid,
}

crate::test_entity_schema! {
    ident = SimpleEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SimpleEntity",
    entity_tag = crate::testing::SIMPLE_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// SingletonUnitEntity
///
/// Executor-lifecycle singleton fixture used to keep runtime `only()` load
/// behavior covered after the old semantics harness was pruned.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct SingletonUnitEntity {
    pub(in crate::db::executor::tests) id: Unit,
    pub(in crate::db::executor::tests) label: String,
}

crate::test_entity_schema! {
    ident = SingletonUnitEntity,
    id = Unit,
    id_field = id,
    singleton = true,
    entity_name = "SingletonUnitEntity",
    entity_tag = crate::testing::SINGLETON_UNIT_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Unit),
        ("label", FieldKind::Text),
    ],
    indexes = [],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// IndexedMetricsEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct IndexedMetricsEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) tag: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static INDEXED_METRICS_INDEX_FIELDS: [&str; 1] = ["tag"];
pub(in crate::db::executor::tests) static INDEXED_METRICS_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "tag",
        TestDataStore::PATH,
        &INDEXED_METRICS_INDEX_FIELDS,
        false,
    )];

crate::test_entity_schema! {
    ident = IndexedMetricsEntity,
    id = Ulid,
    id_field = id,
    entity_name = "IndexedMetricsEntity",
    entity_tag = crate::testing::INDEXED_METRICS_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&INDEXED_METRICS_INDEX_MODELS[0]],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// PushdownParityEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct PushdownParityEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) group: u32,
    pub(in crate::db::executor::tests) rank: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static PUSHDOWN_PARITY_INDEX_FIELDS: [&str; 2] =
    ["group", "rank"];
pub(in crate::db::executor::tests) static PUSHDOWN_PARITY_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "group_rank",
        TestDataStore::PATH,
        &PUSHDOWN_PARITY_INDEX_FIELDS,
        false,
    )];

crate::test_entity_schema! {
    ident = PushdownParityEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PushdownParityEntity",
    entity_tag = crate::testing::PUSHDOWN_PARITY_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&PUSHDOWN_PARITY_INDEX_MODELS[0]],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// UniqueIndexRangeEntity
///
/// Executor snapshot fixture for unique secondary range access. This keeps the
/// index-range execution snapshot coverage local to the revived executor test
/// harness instead of depending on pruned pagination backlogs.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct UniqueIndexRangeEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) code: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static UNIQUE_INDEX_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
pub(in crate::db::executor::tests) static UNIQUE_INDEX_RANGE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "code_unique",
        TestDataStore::PATH,
        &UNIQUE_INDEX_RANGE_INDEX_FIELDS,
        true,
    )];

crate::test_entity_schema! {
    ident = UniqueIndexRangeEntity,
    id = Ulid,
    id_field = id,
    entity_name = "UniqueIndexRangeEntity",
    entity_tag = crate::testing::UNIQUE_INDEX_RANGE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("code", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&UNIQUE_INDEX_RANGE_INDEX_MODELS[0]],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// PhaseEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct PhaseEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) opt_rank: Option<u32>,
    pub(in crate::db::executor::tests) rank: u32,
    pub(in crate::db::executor::tests) tags: Vec<u32>,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static PHASE_TAG_KIND: FieldKind = FieldKind::Uint;

crate::impl_test_entity_markers!(PhaseEntity);

crate::impl_test_entity_model_storage!(
    PhaseEntity,
    "PhaseEntity",
    0,
    fields = [
        crate::model::field::FieldModel::generated("id", FieldKind::Ulid),
        crate::model::field::FieldModel::generated_with_storage_decode_and_nullability(
            "opt_rank",
            FieldKind::Uint,
            crate::model::field::FieldStorageDecode::ByKind,
            true,
        ),
        crate::model::field::FieldModel::generated("rank", FieldKind::Uint),
        crate::model::field::FieldModel::generated("tags", FieldKind::List(&PHASE_TAG_KIND)),
        crate::model::field::FieldModel::generated("label", FieldKind::Text)
    ],
    indexes = [],
);

crate::impl_test_entity_runtime_surface!(PhaseEntity, Ulid, "PhaseEntity", MODEL_DEF);

impl crate::traits::EntityPlacement for PhaseEntity {
    type Store = TestDataStore;
    type Canister = TestCanister;
}

impl crate::traits::EntityKind for PhaseEntity {
    const ENTITY_TAG: crate::types::EntityTag = crate::testing::PHASE_ENTITY_TAG;
}

impl crate::traits::EntityValue for PhaseEntity {
    fn id(&self) -> crate::types::Id<Self> {
        crate::types::Id::from_key(self.id)
    }
}

// Clear the test data store and any pending commit marker between runs.
pub(in crate::db::executor::tests) fn reset_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&DB).expect("write-side recovery should succeed");
    DATA_STORE.with(|store| store.borrow_mut().clear());
    INDEX_STORE.with(|store| store.borrow_mut().clear());
}

// RelationTestCanister

pub(in crate::db::executor::tests) struct RelationTestCanister;

impl Path for RelationTestCanister {
    const PATH: &'static str = concat!(module_path!(), "::RelationTestCanister");
}

impl crate::traits::CanisterKind for RelationTestCanister {
    const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
}

// RelationSourceStore

pub(in crate::db::executor::tests) struct RelationSourceStore;

impl Path for RelationSourceStore {
    const PATH: &'static str = concat!(module_path!(), "::RelationSourceStore");
}

impl crate::traits::StoreKind for RelationSourceStore {
    type Canister = RelationTestCanister;
}

// RelationTargetStore

pub(in crate::db::executor::tests) struct RelationTargetStore;

impl Path for RelationTargetStore {
    const PATH: &'static str = concat!(module_path!(), "::RelationTargetStore");
}

impl crate::traits::StoreKind for RelationTargetStore {
    type Canister = RelationTestCanister;
}

thread_local! {
    pub(in crate::db::executor::tests) static REL_SOURCE_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(40)));
    pub(in crate::db::executor::tests) static REL_TARGET_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(41)));
    pub(in crate::db::executor::tests) static REL_SOURCE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(42)));
    pub(in crate::db::executor::tests) static REL_TARGET_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(43)));
    pub(in crate::db::executor::tests) static REL_STORE_REGISTRY: StoreRegistry = {
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

pub(in crate::db::executor::tests) static REL_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<
    RelationTestCanister,
>] = &[
    EntityRuntimeHooks::new(
        RelationTargetEntity::ENTITY_TAG,
        <RelationTargetEntity as crate::traits::EntitySchema>::MODEL,
        RelationTargetEntity::PATH,
        RelationTargetStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RelationTargetEntity>,
        validate_delete_strong_relations_for_source::<RelationTargetEntity>,
    ),
    EntityRuntimeHooks::new(
        RelationSourceEntity::ENTITY_TAG,
        <RelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        RelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RelationSourceEntity>,
        validate_delete_strong_relations_for_source::<RelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakSingleRelationSourceEntity::ENTITY_TAG,
        <WeakSingleRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakSingleRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<WeakSingleRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakSingleRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakOptionalRelationSourceEntity::ENTITY_TAG,
        <WeakOptionalRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakOptionalRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<WeakOptionalRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakOptionalRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakListRelationSourceEntity::ENTITY_TAG,
        <WeakListRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakListRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<WeakListRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakListRelationSourceEntity>,
    ),
];

pub(in crate::db::executor::tests) static REL_DB: Db<RelationTestCanister> =
    Db::new_with_hooks(&REL_STORE_REGISTRY, REL_ENTITY_RUNTIME_HOOKS);

///
/// RelationTargetEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct RelationTargetEntity {
    pub(in crate::db::executor::tests) id: Ulid,
}

crate::test_entity_schema! {
    ident = RelationTargetEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RelationTargetEntity",
    entity_tag = crate::testing::RELATION_TARGET_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = RelationTargetStore,
    canister = RelationTestCanister,
}

///
/// RelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct RelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Ulid,
}

crate::test_entity_schema! {
    ident = RelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RelationSourceEntity",
    entity_tag = crate::testing::RELATION_SOURCE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name:
                    <RelationTargetEntity as crate::traits::EntitySchema>::MODEL.name(),
                target_entity_tag: RelationTargetEntity::ENTITY_TAG,
                target_store_path: RelationTargetStore::PATH,
                key_kind: &FieldKind::Ulid,
                strength: RelationStrength::Strong,
            }
        ),
    ],
    indexes = [],
    store = RelationSourceStore,
    canister = RelationTestCanister,
}

///
/// WeakSingleRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakSingleRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Ulid,
}

crate::test_entity_schema! {
    ident = WeakSingleRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakSingleRelationSourceEntity",
    entity_tag = crate::testing::WEAK_SINGLE_RELATION_SOURCE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name:
                    <RelationTargetEntity as crate::traits::EntitySchema>::MODEL.name(),
                target_entity_tag: RelationTargetEntity::ENTITY_TAG,
                target_store_path: RelationTargetStore::PATH,
                key_kind: &FieldKind::Ulid,
                strength: RelationStrength::Weak,
            }
        ),
    ],
    indexes = [],
    store = RelationSourceStore,
    canister = RelationTestCanister,
}

///
/// WeakOptionalRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakOptionalRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Option<Ulid>,
}

crate::test_entity_schema! {
    ident = WeakOptionalRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakOptionalRelationSourceEntity",
    entity_tag = crate::testing::WEAK_OPTIONAL_RELATION_SOURCE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name:
                    <RelationTargetEntity as crate::traits::EntitySchema>::MODEL.name(),
                target_entity_tag: RelationTargetEntity::ENTITY_TAG,
                target_store_path: RelationTargetStore::PATH,
                key_kind: &FieldKind::Ulid,
                strength: RelationStrength::Weak,
            }
        ),
    ],
    indexes = [],
    store = RelationSourceStore,
    canister = RelationTestCanister,
}

///
/// WeakListRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakListRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) targets: Vec<Ulid>,
}

pub(in crate::db::executor::tests) static REL_WEAK_LIST_TARGET_KIND: FieldKind =
    FieldKind::Relation {
        target_path: RelationTargetEntity::PATH,
        target_entity_name: <RelationTargetEntity as crate::traits::EntitySchema>::MODEL.name(),
        target_entity_tag: RelationTargetEntity::ENTITY_TAG,
        target_store_path: RelationTargetStore::PATH,
        key_kind: &FieldKind::Ulid,
        strength: RelationStrength::Weak,
    };

crate::test_entity_schema! {
    ident = WeakListRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakListRelationSourceEntity",
    entity_tag = crate::testing::WEAK_LIST_RELATION_SOURCE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("targets", FieldKind::List(&REL_WEAK_LIST_TARGET_KIND)),
    ],
    indexes = [],
    store = RelationSourceStore,
    canister = RelationTestCanister,
}

///
/// WeakSetRelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakSetRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) targets: Vec<Ulid>,
}

pub(in crate::db::executor::tests) static REL_WEAK_SET_TARGET_KIND: FieldKind =
    FieldKind::Relation {
        target_path: RelationTargetEntity::PATH,
        target_entity_name: <RelationTargetEntity as crate::traits::EntitySchema>::MODEL.name(),
        target_entity_tag: RelationTargetEntity::ENTITY_TAG,
        target_store_path: RelationTargetStore::PATH,
        key_kind: &FieldKind::Ulid,
        strength: RelationStrength::Weak,
    };

crate::test_entity_schema! {
    ident = WeakSetRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakSetRelationSourceEntity",
    entity_tag = crate::testing::WEAK_SET_RELATION_SOURCE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("targets", FieldKind::Set(&REL_WEAK_SET_TARGET_KIND)),
    ],
    indexes = [],
    store = RelationSourceStore,
    canister = RelationTestCanister,
}

// Clear relation test stores and any pending commit marker between runs.
pub(in crate::db::executor::tests) fn reset_relation_stores() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&REL_DB).expect("relation write-side recovery should succeed");
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
