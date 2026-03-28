//! Module: db::executor::tests
//! Responsibility: module-local ownership and contracts for db::executor::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod aggregate_core;
mod aggregate_path;
mod aggregate_projection;
mod continuation_structure;
mod cursor_validation;
mod lifecycle;
mod live_state;
mod load_structure;
mod metrics;
mod mutation_save;
mod ordering;
mod pagination;
mod post_access;
mod reverse_index;
mod semantics;
mod set_access;
mod stale_secondary;

use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered,
            init_commit_store_for_tests, prepare_row_commit_for_entity,
            prepare_row_commit_for_entity_with_structural_readers,
        },
        data::DataStore,
        executor::{DeleteExecutor, LoadExecutor, SaveExecutor},
        index::IndexStore,
        predicate::MissingRowPolicy,
        query::intent::Query,
        registry::StoreRegistry,
        relation::validate_delete_strong_relations_for_source,
    },
    model::{
        field::{FieldKind, RelationStrength},
        index::IndexModel,
    },
    testing::test_memory,
    traits::{EntityKind, EntityValue, Path},
    types::{Ulid, Unit},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

// TestCanister

crate::test_canister! {
    ident = TestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

// TestDataStore

crate::test_store! {
    ident = TestDataStore,
    canister = TestCanister,
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

///
/// SimpleEntity
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SimpleEntity {
    id: Ulid,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SingletonUnitEntity {
    id: Unit,
    label: String,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct IndexedMetricsEntity {
    id: Ulid,
    tag: u32,
    label: String,
}

static INDEXED_METRICS_INDEX_FIELDS: [&str; 1] = ["tag"];
static INDEXED_METRICS_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct PushdownParityEntity {
    id: Ulid,
    group: u32,
    rank: u32,
    label: String,
}

static PUSHDOWN_PARITY_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
static PUSHDOWN_PARITY_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct UniqueIndexRangeEntity {
    id: Ulid,
    code: u32,
    label: String,
}

static UNIQUE_INDEX_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
static UNIQUE_INDEX_RANGE_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct PhaseEntity {
    id: Ulid,
    opt_rank: Option<u32>,
    rank: u32,
    tags: Vec<u32>,
    label: String,
}

static PHASE_TAG_KIND: FieldKind = FieldKind::Uint;

crate::impl_test_entity_markers!(PhaseEntity);

crate::impl_test_entity_model_storage!(
    PhaseEntity,
    "PhaseEntity",
    0,
    fields = [
        crate::model::field::FieldModel::new("id", FieldKind::Ulid),
        crate::model::field::FieldModel::new_with_storage_decode_and_nullability(
            "opt_rank",
            FieldKind::Uint,
            crate::model::field::FieldStorageDecode::ByKind,
            true,
        ),
        crate::model::field::FieldModel::new("rank", FieldKind::Uint),
        crate::model::field::FieldModel::new("tags", FieldKind::List(&PHASE_TAG_KIND)),
        crate::model::field::FieldModel::new("label", FieldKind::Text)
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
fn reset_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&DB).expect("write-side recovery should succeed");
    DATA_STORE.with(|store| store.borrow_mut().clear());
    INDEX_STORE.with(|store| store.borrow_mut().clear());
}

// RelationTestCanister

crate::test_canister! {
    ident = RelationTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

// RelationSourceStore

crate::test_store! {
    ident = RelationSourceStore,
    canister = RelationTestCanister,
}

// RelationTargetStore

crate::test_store! {
    ident = RelationTargetStore,
    canister = RelationTestCanister,
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
        RelationTargetEntity::ENTITY_TAG,
        <RelationTargetEntity as crate::traits::EntitySchema>::MODEL,
        RelationTargetEntity::PATH,
        RelationTargetStore::PATH,
        prepare_row_commit_for_entity::<RelationTargetEntity>,
        prepare_row_commit_for_entity_with_structural_readers::<RelationTargetEntity>,
        validate_delete_strong_relations_for_source::<RelationTargetEntity>,
    ),
    EntityRuntimeHooks::new(
        RelationSourceEntity::ENTITY_TAG,
        <RelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        RelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity::<RelationSourceEntity>,
        prepare_row_commit_for_entity_with_structural_readers::<RelationSourceEntity>,
        validate_delete_strong_relations_for_source::<RelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakSingleRelationSourceEntity::ENTITY_TAG,
        <WeakSingleRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakSingleRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity::<WeakSingleRelationSourceEntity>,
        prepare_row_commit_for_entity_with_structural_readers::<WeakSingleRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakSingleRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakOptionalRelationSourceEntity::ENTITY_TAG,
        <WeakOptionalRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakOptionalRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity::<WeakOptionalRelationSourceEntity>,
        prepare_row_commit_for_entity_with_structural_readers::<WeakOptionalRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakOptionalRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakListRelationSourceEntity::ENTITY_TAG,
        <WeakListRelationSourceEntity as crate::traits::EntitySchema>::MODEL,
        WeakListRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        prepare_row_commit_for_entity::<WeakListRelationSourceEntity>,
        prepare_row_commit_for_entity_with_structural_readers::<WeakListRelationSourceEntity>,
        validate_delete_strong_relations_for_source::<WeakListRelationSourceEntity>,
    ),
];

static REL_DB: Db<RelationTestCanister> =
    Db::new_with_hooks(&REL_STORE_REGISTRY, REL_ENTITY_RUNTIME_HOOKS);

///
/// RelationTargetEntity
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct RelationTargetEntity {
    id: Ulid,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct RelationSourceEntity {
    id: Ulid,
    target: Ulid,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct WeakSingleRelationSourceEntity {
    id: Ulid,
    target: Ulid,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct WeakOptionalRelationSourceEntity {
    id: Ulid,
    target: Option<Ulid>,
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

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct WeakListRelationSourceEntity {
    id: Ulid,
    targets: Vec<Ulid>,
}

static REL_WEAK_LIST_TARGET_KIND: FieldKind = FieldKind::Relation {
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

// Clear relation test stores and any pending commit marker between runs.
fn reset_relation_stores() {
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
