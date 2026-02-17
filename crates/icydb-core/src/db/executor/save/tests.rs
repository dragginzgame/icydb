use super::*;
use crate::{
    db::{
        commit::{ensure_recovered_for_write, init_commit_store_for_tests},
        data::{DataKey, DataStore},
        executor::DeleteExecutor,
        index::IndexStore,
        query::{ReadConsistency, intent::Query},
        registry::StoreRegistry,
    },
    error::{ErrorClass, ErrorOrigin},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, RelationStrength},
        index::IndexModel,
    },
    obs::{metrics_report, metrics_reset_all},
    test_fixtures::entity_model_from_static,
    test_support::test_memory,
    traits::{
        AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement, EntitySchema,
        EntityValue, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto, ValidateCustom,
        Visitable,
    },
    types::{Id, Ulid},
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

impl StoreKind for SourceStore {
    type Canister = TestCanister;
}

///
/// TargetStore
///

struct TargetStore;

impl Path for TargetStore {
    const PATH: &'static str = "save_tests::TargetStore";
}

impl StoreKind for TargetStore {
    type Canister = TestCanister;
}

const UNIQUE_INDEX_STORE_PATH: &str = SourceStore::PATH;

thread_local! {
    static SOURCE_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(0)));
    static TARGET_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(1)));
    static UNIQUE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(2)));
    static TARGET_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(3)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(SourceStore::PATH, &SOURCE_DATA_STORE, &UNIQUE_INDEX_STORE)
            .expect("source store registration should succeed");
        reg.register_store(TargetStore::PATH, &TARGET_DATA_STORE, &TARGET_INDEX_STORE)
            .expect("target store registration should succeed");
        reg
    };
}

static DB: Db<TestCanister> = Db::new(&STORE_REGISTRY);

fn with_data_store<R>(path: &'static str, f: impl FnOnce(&DataStore) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(path).map(|store| store.with_data(f)))
        .expect("data store access should succeed")
}

fn with_data_store_mut<R>(path: &'static str, f: impl FnOnce(&mut DataStore) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(path).map(|store| store.with_data_mut(f)))
        .expect("data store access should succeed")
}

fn with_index_store_mut<R>(path: &'static str, f: impl FnOnce(&mut IndexStore) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(path).map(|store| store.with_index_mut(f)))
        .expect("index store access should succeed")
}

// Clear test stores and ensure recovery has completed before each test mutation.
fn reset_store() {
    ensure_recovered_for_write(&DB).expect("write-side recovery should succeed");
    with_data_store_mut(SourceStore::PATH, DataStore::clear);
    with_data_store_mut(TargetStore::PATH, DataStore::clear);
    with_index_store_mut(UNIQUE_INDEX_STORE_PATH, IndexStore::clear);
    with_index_store_mut(TargetStore::PATH, IndexStore::clear);
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
}

static TARGET_FIELDS: [FieldModel; 1] = [FieldModel {
    name: "id",
    kind: FieldKind::Ulid,
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
    type Store = TargetStore;
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

static SOURCE_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "target",
        kind: FieldKind::Relation {
            target_path: TargetEntity::PATH,
            target_entity_name: TargetEntity::ENTITY_NAME,
            target_store_path: TargetStore::PATH,
            key_kind: &FieldKind::Ulid,
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
    type Store = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for SourceEntity {}

impl EntityValue for SourceEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

///
/// InvalidRelationMetadataEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
struct InvalidRelationMetadataEntity {
    id: Ulid,
    target: Ulid,
}

impl AsView for InvalidRelationMetadataEntity {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for InvalidRelationMetadataEntity {}
impl SanitizeCustom for InvalidRelationMetadataEntity {}
impl ValidateAuto for InvalidRelationMetadataEntity {}
impl ValidateCustom for InvalidRelationMetadataEntity {}
impl Visitable for InvalidRelationMetadataEntity {}

impl Path for InvalidRelationMetadataEntity {
    const PATH: &'static str = "save_tests::InvalidRelationMetadataEntity";
}

impl EntityKey for InvalidRelationMetadataEntity {
    type Key = Ulid;
}

impl EntityIdentity for InvalidRelationMetadataEntity {
    const ENTITY_NAME: &'static str = "InvalidRelationMetadataEntity";
    const PRIMARY_KEY: &'static str = "id";
}

static INVALID_RELATION_METADATA_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "target",
        kind: FieldKind::Relation {
            target_path: TargetEntity::PATH,
            target_entity_name: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            target_store_path: TargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Strong,
        },
    },
];
static INVALID_RELATION_METADATA_FIELD_NAMES: [&str; 2] = ["id", "target"];
static INVALID_RELATION_METADATA_INDEXES: [&crate::model::index::IndexModel; 0] = [];
static INVALID_RELATION_METADATA_MODEL: EntityModel = entity_model_from_static(
    "save_tests::InvalidRelationMetadataEntity",
    "InvalidRelationMetadataEntity",
    &INVALID_RELATION_METADATA_FIELDS[0],
    &INVALID_RELATION_METADATA_FIELDS,
    &INVALID_RELATION_METADATA_INDEXES,
);

impl EntitySchema for InvalidRelationMetadataEntity {
    const MODEL: &'static EntityModel = &INVALID_RELATION_METADATA_MODEL;
    const FIELDS: &'static [&'static str] = &INVALID_RELATION_METADATA_FIELD_NAMES;
    const INDEXES: &'static [&'static crate::model::index::IndexModel] =
        &INVALID_RELATION_METADATA_INDEXES;
}

impl EntityPlacement for InvalidRelationMetadataEntity {
    type Store = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for InvalidRelationMetadataEntity {}

impl EntityValue for InvalidRelationMetadataEntity {
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

static SOURCE_SET_TARGET_KIND: FieldKind = FieldKind::Relation {
    target_path: TargetEntity::PATH,
    target_entity_name: TargetEntity::ENTITY_NAME,
    target_store_path: TargetStore::PATH,
    key_kind: &FieldKind::Ulid,
    strength: RelationStrength::Strong,
};
static SOURCE_SET_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "targets",
        kind: FieldKind::Set(&SOURCE_SET_TARGET_KIND),
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
    type Store = SourceStore;
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

static UNIQUE_EMAIL_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "email",
        kind: FieldKind::Text,
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
    type Store = SourceStore;
    type Canister = TestCanister;
}

impl EntityKind for UniqueEmailEntity {}

impl EntityValue for UniqueEmailEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

fn load_unique_email_entity(id: Ulid) -> Option<UniqueEmailEntity> {
    let data_key = DataKey::try_new::<UniqueEmailEntity>(id)
        .expect("unique email data key should build")
        .to_raw()
        .expect("unique email data key should encode");

    with_data_store(SourceStore::PATH, |data_store| {
        data_store.get(&data_key).map(|row| {
            row.try_decode::<UniqueEmailEntity>()
                .expect("unique email row decode should succeed")
        })
    })
}

fn load_source_set_entity(id: Ulid) -> Option<SourceSetEntity> {
    let data_key = DataKey::try_new::<SourceSetEntity>(id)
        .expect("source-set data key should build")
        .to_raw()
        .expect("source-set data key should encode");

    with_data_store(SourceStore::PATH, |data_store| {
        data_store.get(&data_key).map(|row| {
            row.try_decode::<SourceSetEntity>()
                .expect("source-set row decode should succeed")
        })
    })
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

static MISMATCHED_PK_FIELDS: [FieldModel; 2] = [
    FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    },
    FieldModel {
        name: "actual_id",
        kind: FieldKind::Ulid,
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
    type Store = SourceStore;
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

    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing strong relation should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing strong relation should originate from executor validation",
    );
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn strong_relation_invalid_metadata_fails_internal() {
    let executor = SaveExecutor::<InvalidRelationMetadataEntity>::new(DB, false);
    let entity = InvalidRelationMetadataEntity {
        id: Ulid::generate(),
        target: Ulid::generate(),
    };

    let err = executor
        .validate_strong_relations(&entity)
        .expect_err("invalid relation metadata should fail deterministic preflight");
    assert_eq!(
        err.class,
        ErrorClass::Internal,
        "invalid relation metadata should classify as internal",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "invalid relation metadata should originate from executor boundary",
    );
    assert!(
        err.message.contains("strong relation target name invalid"),
        "unexpected error: {err:?}",
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
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing set relation should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing set relation should originate from executor validation",
    );
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );

    let source_empty = with_data_store(SourceStore::PATH, |data_store| {
        data_store.iter().next().is_none()
    });
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
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing strong relation in set should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing strong relation in set should originate from executor validation",
    );
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}"
    );

    let source_empty = with_data_store(SourceStore::PATH, |data_store| {
        data_store.iter().next().is_none()
    });
    assert!(
        source_empty,
        "source save must be atomic: failed save must not persist partial rows"
    );
}

#[test]
fn insert_many_atomic_rejects_partial_commit_on_late_failure() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<TargetEntity>::new(DB, false);
    let existing = Ulid::from_u128(41);
    save.insert(TargetEntity { id: existing })
        .expect("seed row insert should succeed");

    let new_id = Ulid::from_u128(42);
    let err = save
        .insert_many_atomic(vec![
            TargetEntity { id: new_id },
            TargetEntity { id: existing },
        ])
        .expect_err("atomic insert batch should fail on duplicate key");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "duplicate key should classify as conflict",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Store,
        "duplicate key should originate from store checks",
    );

    let rows = with_data_store(TargetStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(
        rows, 1,
        "atomic insert batch must not persist earlier rows when a later row fails"
    );
}

#[test]
fn insert_many_atomic_rejects_duplicate_keys_in_request() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<TargetEntity>::new(DB, false);
    let dup = Ulid::from_u128(47);
    let err = save
        .insert_many_atomic(vec![TargetEntity { id: dup }, TargetEntity { id: dup }])
        .expect_err("atomic insert batch should reject duplicate keys in one request");
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "duplicate key request should fail deterministic pre-commit validation",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "duplicate key request should fail at executor boundary",
    );
    assert!(
        err.message.contains("duplicate key"),
        "unexpected error: {err:?}",
    );

    let rows = with_data_store(TargetStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(
        rows, 0,
        "duplicate-key atomic batch must not persist any row"
    );
}

#[test]
fn insert_many_non_atomic_commits_prefix_before_late_failure() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<TargetEntity>::new(DB, false);
    let existing = Ulid::from_u128(51);
    save.insert(TargetEntity { id: existing })
        .expect("seed row insert should succeed");

    let new_id = Ulid::from_u128(52);
    let err = save
        .insert_many_non_atomic(vec![
            TargetEntity { id: new_id },
            TargetEntity { id: existing },
        ])
        .expect_err("non-atomic insert batch should fail on duplicate key");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "duplicate key should classify as conflict",
    );

    let rows = with_data_store(TargetStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(
        rows, 2,
        "non-atomic insert batch must preserve earlier committed rows before failure"
    );
}

#[test]
fn insert_many_empty_batch_is_noop_for_atomic_and_non_atomic_lanes() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<TargetEntity>::new(DB, false);
    let atomic = save
        .insert_many_atomic(Vec::<TargetEntity>::new())
        .expect("atomic empty batch should succeed");
    let non_atomic = save
        .insert_many_non_atomic(Vec::<TargetEntity>::new())
        .expect("non-atomic empty batch should succeed");

    assert!(
        atomic.is_empty(),
        "atomic empty batch should return no rows"
    );
    assert!(
        non_atomic.is_empty(),
        "non-atomic empty batch should return no rows",
    );

    let rows = with_data_store(TargetStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(rows, 0, "empty batches must not persist rows");
}

#[test]
fn update_many_atomic_rejects_partial_commit_on_late_conflict() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let first = Ulid::from_u128(60);
    let second = Ulid::from_u128(61);
    save.insert(UniqueEmailEntity {
        id: first,
        email: "a@example.com".to_string(),
    })
    .expect("first seed row should save");
    save.insert(UniqueEmailEntity {
        id: second,
        email: "b@example.com".to_string(),
    })
    .expect("second seed row should save");

    let err = save
        .update_many_atomic(vec![
            UniqueEmailEntity {
                id: first,
                email: "carol@example.com".to_string(),
            },
            UniqueEmailEntity {
                id: second,
                email: "carol@example.com".to_string(),
            },
        ])
        .expect_err("atomic update batch should fail on unique index conflict");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin",
    );

    let first_row = load_unique_email_entity(first).expect("first row should remain");
    let second_row = load_unique_email_entity(second).expect("second row should remain");
    assert_eq!(
        first_row.email, "a@example.com",
        "atomic update batch failure must not persist earlier updates",
    );
    assert_eq!(
        second_row.email, "b@example.com",
        "atomic update batch failure must not persist later updates",
    );
}

#[test]
fn update_many_non_atomic_commits_prefix_before_late_conflict() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let first = Ulid::from_u128(62);
    let second = Ulid::from_u128(63);
    save.insert(UniqueEmailEntity {
        id: first,
        email: "a@example.com".to_string(),
    })
    .expect("first seed row should save");
    save.insert(UniqueEmailEntity {
        id: second,
        email: "b@example.com".to_string(),
    })
    .expect("second seed row should save");

    let err = save
        .update_many_non_atomic(vec![
            UniqueEmailEntity {
                id: first,
                email: "carol@example.com".to_string(),
            },
            UniqueEmailEntity {
                id: second,
                email: "carol@example.com".to_string(),
            },
        ])
        .expect_err("non-atomic update batch should fail on unique index conflict");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin",
    );

    let first_row = load_unique_email_entity(first).expect("first row should remain");
    let second_row = load_unique_email_entity(second).expect("second row should remain");
    assert_eq!(
        first_row.email, "carol@example.com",
        "non-atomic update batch should keep earlier committed updates",
    );
    assert_eq!(
        second_row.email, "b@example.com",
        "non-atomic update batch should leave later row unchanged on failure",
    );
}

#[test]
fn replace_many_atomic_mixed_existing_missing_rejects_partial_commit_on_conflict() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let existing = Ulid::from_u128(70);
    let missing = Ulid::from_u128(72);
    save.insert(UniqueEmailEntity {
        id: existing,
        email: "a@example.com".to_string(),
    })
    .expect("existing seed row should save");

    let err = save
        .replace_many_atomic(vec![
            UniqueEmailEntity {
                id: existing,
                email: "carol@example.com".to_string(),
            },
            UniqueEmailEntity {
                id: missing,
                email: "carol@example.com".to_string(),
            },
        ])
        .expect_err("atomic replace batch should fail on unique index conflict");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin",
    );

    let existing_row = load_unique_email_entity(existing).expect("existing row should remain");
    assert_eq!(
        existing_row.email, "a@example.com",
        "atomic replace failure must not persist earlier replacements",
    );
    let missing_row = load_unique_email_entity(missing);
    assert!(
        missing_row.is_none(),
        "atomic replace failure must not insert missing-row replacement",
    );
}

#[test]
fn replace_many_non_atomic_mixed_existing_missing_commits_prefix_before_conflict() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let existing = Ulid::from_u128(73);
    let missing = Ulid::from_u128(74);
    save.insert(UniqueEmailEntity {
        id: existing,
        email: "a@example.com".to_string(),
    })
    .expect("existing seed row should save");

    let err = save
        .replace_many_non_atomic(vec![
            UniqueEmailEntity {
                id: existing,
                email: "carol@example.com".to_string(),
            },
            UniqueEmailEntity {
                id: missing,
                email: "carol@example.com".to_string(),
            },
        ])
        .expect_err("non-atomic replace batch should fail on unique index conflict");
    assert_eq!(
        err.class,
        ErrorClass::Conflict,
        "expected conflict error class",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Index,
        "expected index error origin",
    );

    let existing_row = load_unique_email_entity(existing).expect("existing row should remain");
    assert_eq!(
        existing_row.email, "carol@example.com",
        "non-atomic replace batch should keep earlier committed replacements",
    );
    let missing_row = load_unique_email_entity(missing);
    assert!(
        missing_row.is_none(),
        "failed non-atomic replacement should not persist the failing item",
    );
}

#[test]
fn insert_many_atomic_with_strong_relations_mixed_valid_invalid_fails_atomically() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let target_save = SaveExecutor::<TargetEntity>::new(DB, false);
    let valid_target = Ulid::from_u128(80);
    target_save
        .insert(TargetEntity { id: valid_target })
        .expect("valid target should save");

    let missing_target = Ulid::from_u128(81);
    let source_save = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let err = source_save
        .insert_many_atomic(vec![
            SourceSetEntity {
                id: Ulid::from_u128(82),
                targets: vec![valid_target],
            },
            SourceSetEntity {
                id: Ulid::from_u128(83),
                targets: vec![valid_target, missing_target],
            },
        ])
        .expect_err("atomic relation batch should fail when one item has missing strong relation");
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing strong relation should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing strong relation should originate from executor validation",
    );
    assert!(
        err.message.contains("strong relation missing"),
        "unexpected error: {err:?}",
    );

    let source_rows = with_data_store(SourceStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(
        source_rows, 0,
        "atomic relation batch failure must not persist any source row",
    );
}

#[test]
fn update_many_atomic_with_strong_relations_mixed_valid_invalid_fails_atomically() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let target_save = SaveExecutor::<TargetEntity>::new(DB, false);
    let valid_a = Ulid::from_u128(84);
    let valid_b = Ulid::from_u128(85);
    target_save
        .insert(TargetEntity { id: valid_a })
        .expect("valid target A should save");
    target_save
        .insert(TargetEntity { id: valid_b })
        .expect("valid target B should save");

    let source_save = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let first_id = Ulid::from_u128(86);
    let second_id = Ulid::from_u128(87);
    source_save
        .insert(SourceSetEntity {
            id: first_id,
            targets: vec![valid_a],
        })
        .expect("first source seed row should save");
    source_save
        .insert(SourceSetEntity {
            id: second_id,
            targets: vec![valid_a],
        })
        .expect("second source seed row should save");

    let missing_target = Ulid::from_u128(88);
    let err = source_save
        .update_many_atomic(vec![
            SourceSetEntity {
                id: first_id,
                targets: vec![valid_b],
            },
            SourceSetEntity {
                id: second_id,
                targets: vec![valid_b, missing_target],
            },
        ])
        .expect_err("atomic relation update batch should fail when one item has missing relation");
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing strong relation should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing strong relation should originate from executor validation",
    );

    let first_row = load_source_set_entity(first_id).expect("first source row should remain");
    let second_row = load_source_set_entity(second_id).expect("second source row should remain");
    assert_eq!(
        first_row.targets,
        vec![valid_a],
        "atomic relation update failure must not persist earlier updates",
    );
    assert_eq!(
        second_row.targets,
        vec![valid_a],
        "atomic relation update failure must not persist later updates",
    );
}

#[test]
fn replace_many_atomic_with_strong_relations_mixed_valid_invalid_fails_atomically() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let target_save = SaveExecutor::<TargetEntity>::new(DB, false);
    let valid_target = Ulid::from_u128(89);
    target_save
        .insert(TargetEntity { id: valid_target })
        .expect("valid target should save");

    let source_save = SaveExecutor::<SourceSetEntity>::new(DB, false);
    let existing_id = Ulid::from_u128(90);
    source_save
        .insert(SourceSetEntity {
            id: existing_id,
            targets: vec![valid_target],
        })
        .expect("existing source row should save");

    let missing_target = Ulid::from_u128(91);
    let inserted_id = Ulid::from_u128(92);
    let err = source_save
        .replace_many_atomic(vec![
            SourceSetEntity {
                id: existing_id,
                targets: vec![valid_target],
            },
            SourceSetEntity {
                id: inserted_id,
                targets: vec![valid_target, missing_target],
            },
        ])
        .expect_err("atomic relation replace batch should fail when one item has missing relation");
    assert_eq!(
        err.class,
        ErrorClass::Unsupported,
        "missing strong relation should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Executor,
        "missing strong relation should originate from executor validation",
    );

    let existing_row =
        load_source_set_entity(existing_id).expect("existing source row should remain");
    assert_eq!(
        existing_row.targets,
        vec![valid_target],
        "atomic relation replace failure must not persist earlier replacements",
    );
    let inserted_row = load_source_set_entity(inserted_id);
    assert!(
        inserted_row.is_none(),
        "atomic relation replace failure must not insert later rows",
    );
}

#[test]
fn batch_lane_metrics_atomic_success_failure_and_non_atomic_partial_are_distinct() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    metrics_reset_all();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);

    // Atomic success: both rows commit, so both index inserts are counted.
    save.insert_many_atomic(vec![
        UniqueEmailEntity {
            id: Ulid::from_u128(93),
            email: "x@example.com".to_string(),
        },
        UniqueEmailEntity {
            id: Ulid::from_u128(94),
            email: "y@example.com".to_string(),
        },
    ])
    .expect("atomic insert batch should succeed");
    let after_atomic_success = metrics_report(None)
        .counters
        .expect("metrics counters should exist after atomic success");
    assert_eq!(
        after_atomic_success.ops.index_inserts, 2,
        "atomic success should emit index inserts for all committed rows",
    );

    // Atomic pre-commit failure: no index delta should be emitted.
    metrics_reset_all();
    let err = save
        .insert_many_atomic(vec![
            UniqueEmailEntity {
                id: Ulid::from_u128(95),
                email: "z@example.com".to_string(),
            },
            UniqueEmailEntity {
                id: Ulid::from_u128(95),
                email: "z@example.com".to_string(),
            },
        ])
        .expect_err("atomic duplicate-key batch should fail pre-commit");
    assert_eq!(err.class, ErrorClass::Unsupported);
    let after_atomic_failure = metrics_report(None)
        .counters
        .expect("metrics counters should exist after atomic failure");
    assert_eq!(
        after_atomic_failure.ops.index_inserts, 0,
        "atomic pre-commit failure must not emit index insert deltas",
    );
    assert_eq!(
        after_atomic_failure.ops.index_removes, 0,
        "atomic pre-commit failure must not emit index remove deltas",
    );

    // Non-atomic partial failure: successful prefix should emit index delta.
    metrics_reset_all();
    let existing = Ulid::from_u128(96);
    save.insert(UniqueEmailEntity {
        id: existing,
        email: "base@example.com".to_string(),
    })
    .expect("seed row should save");
    save.insert_many_non_atomic(vec![
        UniqueEmailEntity {
            id: Ulid::from_u128(97),
            email: "partial@example.com".to_string(),
        },
        UniqueEmailEntity {
            id: existing,
            email: "base@example.com".to_string(),
        },
    ])
    .expect_err("non-atomic batch should fail after prefix commit");
    let after_non_atomic_partial = metrics_report(None)
        .counters
        .expect("metrics counters should exist after non-atomic partial failure");
    assert_eq!(
        after_non_atomic_partial.ops.index_inserts, 2,
        "non-atomic path should count seed insert + committed prefix insert",
    );
}

#[test]
fn set_field_encoding_requires_canonical_order_and_uniqueness() {
    let kind = FieldKind::Set(&FieldKind::Ulid);
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
    let kind = FieldKind::Map {
        key: &FieldKind::Text,
        value: &FieldKind::Uint,
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

    let source_empty = with_data_store(SourceStore::PATH, |data_store| {
        data_store.iter().next().is_none()
    });
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

    let rows = with_data_store(SourceStore::PATH, |data_store| data_store.iter().count());
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

    let rows = with_data_store(SourceStore::PATH, |data_store| data_store.iter().count());
    assert_eq!(rows, 2, "failed update must not remove persisted rows");
}

#[test]
fn unique_index_update_same_pk_same_components_is_allowed() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let id = Ulid::from_u128(30);
    save.insert(UniqueEmailEntity {
        id,
        email: "alice@example.com".to_string(),
    })
    .expect("seed unique row should save");

    let updated = save
        .update(UniqueEmailEntity {
            id,
            email: "alice@example.com".to_string(),
        })
        .expect("update with same pk and identical unique components should succeed");
    assert_eq!(updated.id, id);
    assert_eq!(updated.email, "alice@example.com");

    let persisted = load_unique_email_entity(id).expect("row should remain after no-op update");
    assert_eq!(persisted.email, "alice@example.com");
}

#[test]
fn unique_index_delete_then_insert_same_value_succeeds() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<UniqueEmailEntity>::new(DB, false);
    let delete = DeleteExecutor::<UniqueEmailEntity>::new(DB, false);

    let original = Ulid::from_u128(40);
    save.insert(UniqueEmailEntity {
        id: original,
        email: "alice@example.com".to_string(),
    })
    .expect("seed unique row should save");

    let delete_plan = Query::<UniqueEmailEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(original)
        .plan()
        .expect("delete plan should build");
    let deleted = delete
        .execute(delete_plan)
        .expect("delete should clear existing unique row");
    assert_eq!(deleted.0.len(), 1);

    let replacement = Ulid::from_u128(41);
    save.insert(UniqueEmailEntity {
        id: replacement,
        email: "alice@example.com".to_string(),
    })
    .expect("reinsert after delete should succeed for same unique value");

    let original_row = load_unique_email_entity(original);
    let replacement_row = load_unique_email_entity(replacement);
    assert!(original_row.is_none(), "deleted row should remain removed");
    assert!(
        replacement_row.is_some(),
        "replacement row should persist with reclaimed unique value"
    );
}
