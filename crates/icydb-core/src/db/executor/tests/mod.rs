mod aggregate;
mod cursor_validation;
mod lifecycle;
mod live_state;
mod metrics;
mod mutation_save;
mod paged_builder;
mod pagination;
mod route;
mod semantics;
mod stream_key;

use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered_for_write,
            init_commit_store_for_tests,
        },
        contracts::{CoercionId, CompareOp, ComparePredicate, Predicate, ReadConsistency},
        cursor::{ContinuationToken, CursorBoundary, CursorBoundarySlot},
        data::DataStore,
        executor::{
            DeleteExecutor, ExecutionOptimization, LoadExecutor, SaveExecutor,
            prepare_row_commit_for_entity,
        },
        index::IndexStore,
        query::intent::{IntentError, Query, QueryError},
        registry::StoreRegistry,
        relation::validate_delete_strong_relations_for_source,
    },
    model::{
        field::{FieldKind, RelationStrength},
        index::IndexModel,
    },
    testing::test_memory,
    traits::{EntityIdentity, EntityKind, EntityValue, Path},
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct SimpleEntity {
    id: Ulid,
}

crate::test_entity_schema! {
    ident = SimpleEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SimpleEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// IndexedMetricsEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
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
    primary_key = "id",
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
/// UniqueIndexRangeEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
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
    primary_key = "id",
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
/// PushdownParityEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
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
    primary_key = "id",
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
/// SingletonUnitEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct SingletonUnitEntity {
    id: (),
    label: String,
}

crate::test_entity_schema! {
    ident = SingletonUnitEntity,
    id = (),
    id_field = id,
    singleton = true,
    entity_name = "SingletonUnitEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Unit), ("label", FieldKind::Text)],
    indexes = [],
    store = TestDataStore,
    canister = TestCanister,
}

///
/// PhaseEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct PhaseEntity {
    id: Ulid,
    opt_rank: Option<u32>,
    rank: u32,
    tags: Vec<u32>,
    label: String,
}

static PHASE_TAG_KIND: FieldKind = FieldKind::Uint;

crate::test_entity_schema! {
    ident = PhaseEntity,
    id = Ulid,
    id_field = id,
    entity_name = "PhaseEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        // Optional scalar fields are represented as scalar kinds in runtime models.
        ("opt_rank", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("tags", FieldKind::List(&PHASE_TAG_KIND)),
        ("label", FieldKind::Text),
    ],
    indexes = [],
    store = TestDataStore,
    canister = TestCanister,
}

// Clear the test data store and any pending commit marker between runs.
fn reset_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered_for_write(&DB).expect("write-side recovery should succeed");
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct RelationTargetEntity {
    id: Ulid,
}

crate::test_entity_schema! {
    ident = RelationTargetEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RelationTargetEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = RelationTargetStore,
    canister = RelationTestCanister,
}

///
/// RelationSourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct RelationSourceEntity {
    id: Ulid,
    target: Ulid,
}

crate::test_entity_schema! {
    ident = RelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RelationSourceEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name: RelationTargetEntity::ENTITY_NAME,
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct WeakSingleRelationSourceEntity {
    id: Ulid,
    target: Ulid,
}

crate::test_entity_schema! {
    ident = WeakSingleRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakSingleRelationSourceEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name: RelationTargetEntity::ENTITY_NAME,
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct WeakOptionalRelationSourceEntity {
    id: Ulid,
    target: Option<Ulid>,
}

crate::test_entity_schema! {
    ident = WeakOptionalRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakOptionalRelationSourceEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "target",
            FieldKind::Relation {
                target_path: RelationTargetEntity::PATH,
                target_entity_name: RelationTargetEntity::ENTITY_NAME,
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

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct WeakListRelationSourceEntity {
    id: Ulid,
    targets: Vec<Ulid>,
}

static REL_WEAK_LIST_TARGET_KIND: FieldKind = FieldKind::Relation {
    target_path: RelationTargetEntity::PATH,
    target_entity_name: RelationTargetEntity::ENTITY_NAME,
    target_store_path: RelationTargetStore::PATH,
    key_kind: &FieldKind::Ulid,
    strength: RelationStrength::Weak,
};

crate::test_entity_schema! {
    ident = WeakListRelationSourceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "WeakListRelationSourceEntity",
    primary_key = "id",
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
