use super::*;
use crate::{
    db::{
        commit::{ensure_recovered_for_write, init_commit_store_for_tests},
        index::IndexStoreRegistry,
        store::{DataStore, DataStoreRegistry},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel, RelationStrength},
    },
    test_fixtures::entity_model_from_static,
    traits::{
        AsView, CanisterKind, DataStoreKind, EntityIdentity, EntityKey, EntityKind,
        EntityPlacement, EntitySchema, EntityValue, Path, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
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

// Clear test stores and ensure recovery has completed before each test mutation.
fn reset_store() {
    ensure_recovered_for_write(&DB).expect("write-side recovery should succeed");
    DB.with_data(|reg| {
        reg.with_store_mut(SourceStore::PATH, DataStore::clear)
            .expect("source store access should succeed");
        reg.with_store_mut(TargetStore::PATH, DataStore::clear)
            .expect("target store access should succeed");
    });
}

///
/// TargetEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct TargetEntity {
    id: Ulid,
}

impl AsView for TargetEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
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

impl EntityKey for TargetEntity {
    type Key = Ulid;
}

impl EntityIdentity for TargetEntity {
    const ENTITY_NAME: &'static str = "TargetEntity";
    const PRIMARY_KEY: &'static str = "id";
    const IDENTITY_NAMESPACE: &'static str = "TargetEntity";
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
    fn id(&self) -> Id<Self> {
        Id::from_storage_key(self.id)
    }
}

///
/// SourceEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct SourceEntity {
    id: Ulid,
    target: Ulid,
}

impl AsView for SourceEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
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

impl EntityKey for SourceEntity {
    type Key = Ulid;
}

impl EntityIdentity for SourceEntity {
    const ENTITY_NAME: &'static str = "SourceEntity";
    const PRIMARY_KEY: &'static str = "id";
    const IDENTITY_NAMESPACE: &'static str = "SourceEntity";
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
    fn id(&self) -> Id<Self> {
        Id::from_storage_key(self.id)
    }
}

///
/// SourceSetEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct SourceSetEntity {
    id: Ulid,
    targets: Vec<Ulid>,
}

impl AsView for SourceSetEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for SourceSetEntity {}
impl SanitizeCustom for SourceSetEntity {}
impl ValidateAuto for SourceSetEntity {}
impl ValidateCustom for SourceSetEntity {}
impl Visitable for SourceSetEntity {}

impl Path for SourceSetEntity {
    const PATH: &'static str = "save_tests::SourceSetEntity";
}

impl EntityKey for SourceSetEntity {
    type Key = Ulid;
}

impl EntityIdentity for SourceSetEntity {
    const ENTITY_NAME: &'static str = "SourceSetEntity";
    const PRIMARY_KEY: &'static str = "id";
    const IDENTITY_NAMESPACE: &'static str = "SourceSetEntity";
}

static SOURCE_SET_TARGET_KIND: EntityFieldKind = EntityFieldKind::Ref {
    target_path: TargetEntity::PATH,
    target_entity_name: TargetEntity::ENTITY_NAME,
    target_store_path: TargetStore::PATH,
    key_kind: &EntityFieldKind::Ulid,
    strength: RelationStrength::Strong,
};
static SOURCE_SET_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "targets",
        kind: EntityFieldKind::Set(&SOURCE_SET_TARGET_KIND),
    },
];
static SOURCE_SET_FIELD_NAMES: [&str; 2] = ["id", "targets"];
static SOURCE_SET_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static SOURCE_SET_MODEL: EntityModel = entity_model_from_static(
    "save_tests::SourceSetEntity",
    "SourceSetEntity",
    &SOURCE_SET_FIELDS[0],
    &SOURCE_SET_FIELDS,
    &SOURCE_SET_INDEXES,
);

impl EntitySchema for SourceSetEntity {
    const MODEL: &'static EntityModel = &SOURCE_SET_MODEL;
    const FIELDS: &'static [&'static str] = &SOURCE_SET_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &SOURCE_SET_INDEXES;
}

impl EntityPlacement for SourceSetEntity {
    type DataStore = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for SourceSetEntity {}

impl EntityValue for SourceSetEntity {
    fn id(&self) -> Id<Self> {
        Id::from_storage_key(self.id)
    }
}

#[test]
fn strong_relation_missing_fails_preflight() {
    let executor = SaveExecutor::<SourceEntity>::new(DB, false);

    let entity = SourceEntity {
        id: Ulid::generate(),
        target: Ulid::generate(), // non-existent target
    };

    let err = executor
        .validate_strong_relations(&entity)
        .expect_err("expected missing strong relation to fail");

    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn strong_set_relation_missing_key_fails_save() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let executor = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let missing = Ulid::generate();
    let entity = SourceSetEntity {
        id: Ulid::generate(),
        targets: vec![missing],
    };

    let err = executor
        .insert(entity)
        .expect_err("missing set relation should fail");
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );

    let source_empty = DB
        .with_data(|reg| reg.with_store(SourceStore::PATH, |store| store.iter().next().is_none()))
        .expect("source store access should succeed");
    assert!(
        source_empty,
        "source store must remain empty after failed save"
    );
}

#[test]
fn strong_set_relation_all_present_save_succeeds() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let target_save = SaveExecutor::<TargetEntity>::new(DB, false);
    let target_a = Ulid::generate();
    let target_b = Ulid::generate();
    target_save
        .insert(TargetEntity { id: target_a })
        .expect("target A save should succeed");
    target_save
        .insert(TargetEntity { id: target_b })
        .expect("target B save should succeed");

    let source_save = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let saved = source_save
        .insert(SourceSetEntity {
            id: Ulid::generate(),
            targets: vec![target_a, target_b],
        })
        .expect("source save should succeed when all targets exist");

    assert!(saved.targets.contains(&target_a));
    assert!(saved.targets.contains(&target_b));
}

#[test]
fn strong_set_relation_mixed_valid_invalid_fails_atomically() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let target_save = SaveExecutor::<TargetEntity>::new(DB, false);
    let valid = Ulid::generate();
    target_save
        .insert(TargetEntity { id: valid })
        .expect("valid target save should succeed");

    let invalid = Ulid::generate();
    let source_save = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let err = source_save
        .insert(SourceSetEntity {
            id: Ulid::generate(),
            targets: vec![valid, invalid],
        })
        .expect_err("mixed valid/invalid set relation should fail");
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );

    let source_empty = DB
        .with_data(|reg| reg.with_store(SourceStore::PATH, |store| store.iter().next().is_none()))
        .expect("source store access should succeed");
    assert!(
        source_empty,
        "source save must be atomic: failed save must not persist partial rows"
    );
}

#[test]
fn set_field_encoding_requires_canonical_order_and_uniqueness() {
    let kind = EntityFieldKind::Set(&EntityFieldKind::Ulid);
    let lower = Value::Ulid(Ulid::from_u128(1));
    let higher = Value::Ulid(Ulid::from_u128(2));

    let err = SaveExecutor::<SourceSetEntity>::validate_deterministic_field_value(
        "targets",
        &kind,
        &Value::List(vec![higher, lower]),
    )
    .expect_err("unordered set encoding must fail");
    assert!(
        err.message
            .contains("set field must be strictly ordered and deduplicated"),
        "unexpected error: {err:?}"
    );

    let dup = Value::Ulid(Ulid::from_u128(7));
    let err = SaveExecutor::<SourceSetEntity>::validate_deterministic_field_value(
        "targets",
        &kind,
        &Value::List(vec![dup.clone(), dup]),
    )
    .expect_err("duplicate set entries must fail");
    assert!(
        err.message
            .contains("set field must be strictly ordered and deduplicated"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn map_field_encoding_requires_canonical_entry_order() {
    let kind = EntityFieldKind::Map {
        key: &EntityFieldKind::Text,
        value: &EntityFieldKind::Uint,
    };
    let unordered = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Uint(9u64)),
        (Value::Text("a".to_string()), Value::Uint(1u64)),
    ]);

    let err = SaveExecutor::<SourceSetEntity>::validate_deterministic_field_value(
        "settings", &kind, &unordered,
    )
    .expect_err("unordered map entries must fail");
    assert!(
        err.message
            .contains("map field entries are not in canonical deterministic order"),
        "unexpected error: {err:?}"
    );
}
