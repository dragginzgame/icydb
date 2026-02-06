use super::*;
use crate::{
    db::{
        index::IndexStoreRegistry,
        store::{DataStore, DataStoreRegistry},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel, RelationStrength},
    },
    test_fixtures::entity_model_from_static,
    traits::{
        CanisterKind, DataStoreKind, EntityIdentity, EntityKind, EntityPlacement, EntitySchema,
        EntityValue, Path, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, View,
        Visitable,
    },
    types::{Id, Ref, Ulid},
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
    const PATH: &'static str = "save_tests::TestCanister";
}

impl CanisterKind for TestCanister {}

///
/// SourceStore
///

struct SourceStore;

impl Path for SourceStore {
    const PATH: &'static str = "save_tests::SourceStore";
}

impl DataStoreKind for SourceStore {
    type Canister = TestCanister;
}

///
/// TargetStore
///

struct TargetStore;

impl Path for TargetStore {
    const PATH: &'static str = "save_tests::TargetStore";
}

impl DataStoreKind for TargetStore {
    type Canister = TestCanister;
}

thread_local! {
    static SOURCE_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(0)));
    static TARGET_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(1)));
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(SourceStore::PATH, &SOURCE_DATA_STORE);
        reg.register(TargetStore::PATH, &TARGET_DATA_STORE);
        reg
    };
    static INDEX_REGISTRY: IndexStoreRegistry = IndexStoreRegistry::new();
}

static DB: Db<TestCanister> = Db::new(&DATA_REGISTRY, &INDEX_REGISTRY);

// Test-only stable memory allocation for in-memory stores.
fn test_memory(id: u8) -> VirtualMemory<DefaultMemoryImpl> {
    let manager = MemoryManager::init(DefaultMemoryImpl::default());

    manager.get(MemoryId::new(id))
}

///
/// TargetEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct TargetEntity {
    id: Id<Self>,
}

impl View for TargetEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for TargetEntity {}
impl SanitizeCustom for TargetEntity {}
impl ValidateAuto for TargetEntity {}
impl ValidateCustom for TargetEntity {}
impl Visitable for TargetEntity {}

impl Path for TargetEntity {
    const PATH: &'static str = "save_tests::TargetEntity";
}

impl EntityIdentity for TargetEntity {
    type Id = Ulid;

    const ENTITY_NAME: &'static str = "TargetEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static TARGET_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
static TARGET_FIELD_NAMES: [&str; 1] = ["id"];
static TARGET_INDEXES: [&crate::model::index::IndexModel; 0] = [];
// NOTE: Save executor tests use manual models to avoid schema macros.
static TARGET_MODEL: EntityModel = entity_model_from_static(
    "save_tests::TargetEntity",
    "TargetEntity",
    &TARGET_FIELDS[0],
    &TARGET_FIELDS,
    &TARGET_INDEXES,
);

impl EntitySchema for TargetEntity {
    const MODEL: &'static EntityModel = &TARGET_MODEL;
    const FIELDS: &'static [&'static str] = &TARGET_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &TARGET_INDEXES;
}

impl EntityPlacement for TargetEntity {
    type DataStore = TargetStore;
    type Canister = TestCanister;
}

impl EntityKind for TargetEntity {}

impl EntityValue for TargetEntity {
    fn id(&self) -> Self::Id {
        *self.id.key()
    }
}

///
/// SourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct SourceEntity {
    id: Id<Self>,
    target: Ref<TargetEntity>,
}

impl View for SourceEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for SourceEntity {}
impl SanitizeCustom for SourceEntity {}
impl ValidateAuto for SourceEntity {}
impl ValidateCustom for SourceEntity {}
impl Visitable for SourceEntity {}

impl Path for SourceEntity {
    const PATH: &'static str = "save_tests::SourceEntity";
}

impl EntityIdentity for SourceEntity {
    type Id = Ulid;

    const ENTITY_NAME: &'static str = "SourceEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static SOURCE_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "target",
        kind: EntityFieldKind::Ref {
            target_path: TargetEntity::PATH,
            target_entity_name: TargetEntity::ENTITY_NAME,
            target_store_path: TargetStore::PATH,
            key_kind: &EntityFieldKind::Ulid,
            strength: RelationStrength::Strong,
        },
    },
];
static SOURCE_FIELD_NAMES: [&str; 2] = ["id", "target"];
static SOURCE_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static SOURCE_MODEL: EntityModel = entity_model_from_static(
    "save_tests::SourceEntity",
    "SourceEntity",
    &SOURCE_FIELDS[0],
    &SOURCE_FIELDS,
    &SOURCE_INDEXES,
);

impl EntitySchema for SourceEntity {
    const MODEL: &'static EntityModel = &SOURCE_MODEL;
    const FIELDS: &'static [&'static str] = &SOURCE_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &SOURCE_INDEXES;
}

impl EntityPlacement for SourceEntity {
    type DataStore = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for SourceEntity {}

impl EntityValue for SourceEntity {
    fn id(&self) -> Self::Id {
        *self.id.key()
    }
}

#[test]
fn strong_relation_missing_fails_preflight() {
    let executor = SaveExecutor::<SourceEntity>::new(DB, false);
    let entity = SourceEntity {
        id: Id::new(Ulid::generate()),
        target: Ref::new(Ulid::generate()),
    };

    let err = executor
        .validate_strong_relations(&entity)
        .expect_err("expected missing strong relation to fail");

    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );
}
