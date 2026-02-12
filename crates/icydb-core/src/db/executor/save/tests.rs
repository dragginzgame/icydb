use super::*;
use crate::{
    db::{
        commit::{ensure_recovered_for_write, init_commit_store_for_tests},
        index::{IndexStore, IndexStoreRegistry},
        store::{DataStore, DataStoreRegistry},
    },
    error::{ErrorClass, ErrorOrigin},
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel, RelationStrength},
        index::IndexModel,
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
    static UNIQUE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(2), test_memory(3)));
    static DATA_REGISTRY: DataStoreRegistry = {
        let mut reg = DataStoreRegistry::new();
        reg.register(SourceStore::PATH, &SOURCE_DATA_STORE);
        reg.register(TargetStore::PATH, &TARGET_DATA_STORE);
        reg
    };
    static INDEX_REGISTRY: IndexStoreRegistry = {
        let mut reg = IndexStoreRegistry::new();
        reg.register(UNIQUE_INDEX_STORE_PATH, &UNIQUE_INDEX_STORE);
        reg
    };
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
    DB.with_index(|reg| {
        reg.with_store_mut(UNIQUE_INDEX_STORE_PATH, IndexStore::clear)
            .expect("unique index store access should succeed");
    });
}

const UNIQUE_INDEX_STORE_PATH: &str = "save_tests::UniqueIndexStore";

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
        Id::from_key(self.id)
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
}

static SOURCE_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "target",
        kind: EntityFieldKind::Relation {
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
        Id::from_key(self.id)
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
}

static SOURCE_SET_TARGET_KIND: EntityFieldKind = EntityFieldKind::Relation {
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
        Id::from_key(self.id)
    }
}

///
/// UniqueEmailEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct UniqueEmailEntity {
    id: Ulid,
    email: String,
}

impl AsView for UniqueEmailEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for UniqueEmailEntity {}
impl SanitizeCustom for UniqueEmailEntity {}
impl ValidateAuto for UniqueEmailEntity {}
impl ValidateCustom for UniqueEmailEntity {}
impl Visitable for UniqueEmailEntity {}

impl Path for UniqueEmailEntity {
    const PATH: &'static str = "save_tests::UniqueEmailEntity";
}

impl EntityKey for UniqueEmailEntity {
    type Key = Ulid;
}

impl EntityIdentity for UniqueEmailEntity {
    const ENTITY_NAME: &'static str = "UniqueEmailEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static UNIQUE_EMAIL_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "email",
        kind: EntityFieldKind::Text,
    },
];
static UNIQUE_EMAIL_FIELD_NAMES: [&str; 2] = ["id", "email"];
static UNIQUE_EMAIL_INDEX_FIELDS: [&str; 1] = ["email"];
static UNIQUE_EMAIL_INDEX: IndexModel = IndexModel::new(
    "save_tests::UniqueEmailEntity::email",
    UNIQUE_INDEX_STORE_PATH,
    &UNIQUE_EMAIL_INDEX_FIELDS,
    true,
);
static UNIQUE_EMAIL_INDEXES: [&IndexModel; 1] = [&UNIQUE_EMAIL_INDEX];
static UNIQUE_EMAIL_MODEL: EntityModel = entity_model_from_static(
    "save_tests::UniqueEmailEntity",
    "UniqueEmailEntity",
    &UNIQUE_EMAIL_FIELDS[0],
    &UNIQUE_EMAIL_FIELDS,
    &UNIQUE_EMAIL_INDEXES,
);

impl EntitySchema for UniqueEmailEntity {
    const MODEL: &'static EntityModel = &UNIQUE_EMAIL_MODEL;
    const FIELDS: &'static [&'static str] = &UNIQUE_EMAIL_FIELD_NAMES;
    const INDEXES: &'static [&'static IndexModel] = &UNIQUE_EMAIL_INDEXES;
}

impl EntityPlacement for UniqueEmailEntity {
    type DataStore = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for UniqueEmailEntity {}

impl EntityValue for UniqueEmailEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// MismatchedPkEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct MismatchedPkEntity {
    id: Ulid,
    actual_id: Ulid,
}

impl AsView for MismatchedPkEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for MismatchedPkEntity {}
impl SanitizeCustom for MismatchedPkEntity {}
impl ValidateAuto for MismatchedPkEntity {}
impl ValidateCustom for MismatchedPkEntity {}
impl Visitable for MismatchedPkEntity {}

impl Path for MismatchedPkEntity {
    const PATH: &'static str = "save_tests::MismatchedPkEntity";
}

impl EntityKey for MismatchedPkEntity {
    type Key = Ulid;
}

impl EntityIdentity for MismatchedPkEntity {
    const ENTITY_NAME: &'static str = "MismatchedPkEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static MISMATCHED_PK_FIELDS: [EntityFieldModel; 2] = [
    EntityFieldModel {
        name: "id",
        kind: EntityFieldKind::Ulid,
    },
    EntityFieldModel {
        name: "actual_id",
        kind: EntityFieldKind::Ulid,
    },
];
static MISMATCHED_PK_FIELD_NAMES: [&str; 2] = ["id", "actual_id"];
static MISMATCHED_PK_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static MISMATCHED_PK_MODEL: EntityModel = entity_model_from_static(
    "save_tests::MismatchedPkEntity",
    "MismatchedPkEntity",
    &MISMATCHED_PK_FIELDS[0],
    &MISMATCHED_PK_FIELDS,
    &MISMATCHED_PK_INDEXES,
);

impl EntitySchema for MismatchedPkEntity {
    const MODEL: &'static EntityModel = &MISMATCHED_PK_MODEL;
    const FIELDS: &'static [&'static str] = &MISMATCHED_PK_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] = &MISMATCHED_PK_INDEXES;
}

impl EntityPlacement for MismatchedPkEntity {
    type DataStore = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for MismatchedPkEntity {}

impl EntityValue for MismatchedPkEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.actual_id)
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

#[test]
fn save_rejects_primary_key_field_and_identity_mismatch() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let executor = SaveExecutor::<MismatchedPkEntity>::new(DB, false);
    let entity = MismatchedPkEntity {
        id: Ulid::from_u128(10),
        actual_id: Ulid::from_u128(20),
    };

    let err = executor
        .insert(entity)
        .expect_err("mismatched primary key identity should fail save");
    assert!(
        err.message.contains("entity primary key mismatch"),
        "unexpected error: {err:?}"
    );

    let source_empty = DB
        .with_data(|reg| reg.with_store(SourceStore::PATH, |store| store.iter().next().is_none()))
        .expect("source store access should succeed");
    assert!(
        source_empty,
        "failed invariant checks must not persist rows"
    );
}

#[test]
fn unique_index_violation_rejected_on_insert() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    save.insert(UniqueEmailEntity {
        id: Ulid::from_u128(10),
        email: "alice@example.com".to_string(),
    })
    .expect("first unique insert should succeed");

    let err = save
        .insert(UniqueEmailEntity {
            id: Ulid::from_u128(11),
            email: "alice@example.com".to_string(),
        })
        .expect_err("duplicate unique index value should fail");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin"
    );
    assert!(
        err.message.contains("index constraint violation"),
        "unexpected error: {err:?}"
    );

    let rows = DB
        .with_data(|reg| reg.with_store(SourceStore::PATH, |store| store.iter().count()))
        .expect("source store access should succeed");
    assert_eq!(rows, 1, "conflicting insert must not persist");
}

#[test]
fn unique_index_violation_rejected_on_update() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    save.insert(UniqueEmailEntity {
        id: Ulid::from_u128(20),
        email: "alice@example.com".to_string(),
    })
    .expect("first unique row should save");
    save.insert(UniqueEmailEntity {
        id: Ulid::from_u128(21),
        email: "bob@example.com".to_string(),
    })
    .expect("second unique row should save");

    let err = save
        .update(UniqueEmailEntity {
            id: Ulid::from_u128(21),
            email: "alice@example.com".to_string(),
        })
        .expect_err("update that collides with unique index should fail");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin"
    );
    assert!(
        err.message.contains("index constraint violation"),
        "unexpected error: {err:?}"
    );

    let rows = DB
        .with_data(|reg| reg.with_store(SourceStore::PATH, |store| store.iter().count()))
        .expect("source store access should succeed");
    assert_eq!(rows, 2, "failed update must not remove persisted rows");
}
