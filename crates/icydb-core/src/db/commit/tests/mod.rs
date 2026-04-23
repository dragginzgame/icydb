//! Module: db::commit::tests
//! Covers commit application and persistence invariants for the write path.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Db, EntityRuntimeHooks, Predicate,
        codec::{
            ROW_FORMAT_VERSION_CURRENT, decode_row_payload_bytes,
            serialize_row_payload_with_version,
        },
        commit::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, CommitRowOp, begin_commit,
            commit_marker_present, encode_commit_marker_payload, ensure_recovered, finish_commit,
            init_commit_store_for_tests, marker::encode_single_row_commit_marker_payload,
            prepare_row_commit_for_entity_with_structural_readers,
            rollback_prepared_row_ops_reverse, store,
        },
        data::{CanonicalRow, DataKey, DataStore, RawDataKey, RawRow, StorageKey},
        executor::SaveExecutor,
        index::{IndexKey, IndexStore, RawIndexEntry},
        registry::{StoreHandle, StoreRegistry},
        relation::validate_delete_strong_relations_for_source,
        schema::commit_schema_fingerprint_for_entity,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    testing::test_memory,
    traits::{EntityKind, EntitySchema, Path, ValueCodec},
    types::Ulid,
    value::{Value, ValueEnum},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeSet, sync::LazyLock};

type RecoveryStoreSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);

static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

//
// RecoveryTestCanister
//

crate::test_canister! {
    ident = RecoveryTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

//
// RecoveryTestDataStore
//

crate::test_store! {
    ident = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

///
/// RecoveryTestEntity
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryTestEntity {
    id: Ulid,
}

crate::test_entity_schema! {
    ident = RecoveryTestEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryTestEntity",
    entity_tag = crate::testing::RECOVERY_TEST_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid)],
    indexes = [],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryPayloadEntity {
    id: Ulid,
    name: String,
}

crate::test_entity_schema! {
    ident = RecoveryPayloadEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryPayloadEntity",
    entity_tag = crate::testing::RECOVERY_PAYLOAD_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("name", FieldKind::Text)],
    indexes = [],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryIndexedEntity {
    id: Ulid,
    group: u32,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUniqueEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUniqueCasefoldEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUpperExpressionEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalEntity {
    id: Ulid,
    group: u32,
    active: bool,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueEntity {
    id: Ulid,
    email: String,
    active: bool,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueCasefoldEntity {
    id: Ulid,
    email: String,
    active: bool,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueEnumEntity {
    id: Ulid,
    #[icydb(meta)]
    status: Value,
    active: bool,
}

impl Default for RecoveryConditionalUniqueEnumEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            status: enum_status("Pending"),
            active: false,
        }
    }
}

static RECOVERY_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_INDEX_FIELDS,
    false,
)];
static RECOVERY_UNIQUE_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UNIQUE_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "email_unique",
    RecoveryTestDataStore::PATH,
    &RECOVERY_UNIQUE_INDEX_FIELDS,
    true,
)];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "email_unique_casefold",
        RecoveryTestDataStore::PATH,
        &RECOVERY_UNIQUE_CASEFOLD_INDEX_FIELDS,
        &RECOVERY_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS,
        true,
    )];
static RECOVERY_UPPER_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UPPER_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Upper("email"))];
static RECOVERY_UPPER_EXPRESSION_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "email_upper",
        RecoveryTestDataStore::PATH,
        &RECOVERY_UPPER_EXPRESSION_INDEX_FIELDS,
        &RECOVERY_UPPER_EXPRESSION_INDEX_KEY_ITEMS,
        false,
    )];
static RECOVERY_CONDITIONAL_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_CONDITIONAL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated_with_predicate(
    "group_active_only",
    RecoveryTestDataStore::PATH,
    &RECOVERY_CONDITIONAL_INDEX_FIELDS,
    false,
    Some(active_true_predicate_metadata()),
)];
static RECOVERY_CONDITIONAL_UNIQUE_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_CONDITIONAL_UNIQUE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_predicate(
        "email_unique_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_INDEX_FIELDS,
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items_and_predicate(
        "email_unique_casefold_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_FIELDS,
        Some(&RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS),
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_FIELDS: [&str; 1] = ["status"];
static RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_predicate(
        "status_unique_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_FIELDS,
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS: [&str; 1] = ["missing_group"];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL: IndexModel = IndexModel::generated(
    "missing_group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS,
    false,
);

crate::test_entity_schema! {
    ident = RecoveryIndexedEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryIndexedEntity",
    entity_tag = crate::testing::RECOVERY_INDEXED_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("group", FieldKind::Uint)],
    indexes = [&RECOVERY_INDEXED_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryUniqueEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryUniqueEntity",
    entity_tag = crate::testing::RECOVERY_UNIQUE_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("email", FieldKind::Text)],
    indexes = [&RECOVERY_UNIQUE_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryUniqueCasefoldEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryUniqueCasefoldEntity",
    entity_tag = crate::testing::RECOVERY_UNIQUE_CASEFOLD_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("email", FieldKind::Text)],
    indexes = [&RECOVERY_UNIQUE_CASEFOLD_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryUpperExpressionEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryUpperExpressionEntity",
    entity_tag = crate::testing::RECOVERY_UPPER_EXPRESSION_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("email", FieldKind::Text)],
    indexes = [&RECOVERY_UPPER_EXPRESSION_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryConditionalEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryConditionalEntity",
    entity_tag = crate::testing::RECOVERY_CONDITIONAL_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("active", FieldKind::Bool),
    ],
    indexes = [&RECOVERY_CONDITIONAL_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryConditionalUniqueEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryConditionalUniqueEntity",
    entity_tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
        ("active", FieldKind::Bool),
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryConditionalUniqueCasefoldEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryConditionalUniqueCasefoldEntity",
    entity_tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("email", FieldKind::Text),
        ("active", FieldKind::Bool),
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_entity_schema! {
    ident = RecoveryConditionalUniqueEnumEntity,
    id = Ulid,
    id_field = id,
    entity_name = "RecoveryConditionalUniqueEnumEntity",
    entity_tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_ENUM_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        (
            "status",
            FieldKind::Enum {
                path: RECOVERY_STATUS_ENUM_PATH,
                variants: &[],
            },
            crate::model::field::FieldStorageDecode::Value
        ),
        ("active", FieldKind::Bool),
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_MODELS[0]],
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

static ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_TAG,
        <RecoveryIndexedEntity as EntitySchema>::MODEL,
        RecoveryIndexedEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUniqueEntity::ENTITY_TAG,
        <RecoveryUniqueEntity as EntitySchema>::MODEL,
        RecoveryUniqueEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryUniqueEntity>,
        validate_delete_strong_relations_for_source::<RecoveryUniqueEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUniqueCasefoldEntity::ENTITY_TAG,
        <RecoveryUniqueCasefoldEntity as EntitySchema>::MODEL,
        RecoveryUniqueCasefoldEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryUniqueCasefoldEntity>,
        validate_delete_strong_relations_for_source::<RecoveryUniqueCasefoldEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUpperExpressionEntity::ENTITY_TAG,
        <RecoveryUpperExpressionEntity as EntitySchema>::MODEL,
        RecoveryUpperExpressionEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryUpperExpressionEntity>,
        validate_delete_strong_relations_for_source::<RecoveryUpperExpressionEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalEntity::ENTITY_TAG,
        <RecoveryConditionalEntity as EntitySchema>::MODEL,
        RecoveryConditionalEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryConditionalEntity>,
        validate_delete_strong_relations_for_source::<RecoveryConditionalEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryConditionalUniqueEntity>,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueCasefoldEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueCasefoldEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<
            RecoveryConditionalUniqueCasefoldEntity,
        >,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueCasefoldEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueEnumEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueEnumEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueEnumEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryConditionalUniqueEnumEntity>,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueEnumEntity>,
    ),
];

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(19)));
    static INDEX_STORE: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(20)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(RecoveryTestDataStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("test store registration should succeed");
        reg
    };
}

static DB: Db<RecoveryTestCanister> = Db::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS);

// Intentionally miswired runtime hooks used only to verify dispatch invariants.
static MISWIRED_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] =
    &[EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    )];

static MISWIRED_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, MISWIRED_ENTITY_RUNTIME_HOOKS);

static DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryIndexedEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryTestEntity>,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_TAG,
        <RecoveryIndexedEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<RecoveryIndexedEntity>,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_PATH_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS);

fn duplicate_name_db() -> Db<RecoveryTestCanister> {
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS)
}

fn with_recovery_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(RecoveryTestDataStore::PATH).map(f))
        .expect("recovery test store access should succeed")
}

// Reset marker + data store to isolate recovery tests.
fn reset_recovery_state() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker reset should succeed");

    with_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

fn row_op_for_path_with_schema(
    path: &'static str,
    data_key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    schema_fingerprint: [u8; 16],
) -> CommitRowOp {
    CommitRowOp::try_new_bytes(path, &data_key, before, after, schema_fingerprint)
        .expect("recovery test row op key bytes should decode")
}

fn row_op_for_path(
    path: &'static str,
    data_key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> CommitRowOp {
    let schema_fingerprint = match path {
        RecoveryTestEntity::PATH => commit_schema_fingerprint_for_entity::<RecoveryTestEntity>(),
        RecoveryIndexedEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryIndexedEntity>()
        }
        RecoveryUniqueEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>()
        }
        RecoveryUniqueCasefoldEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>()
        }
        RecoveryConditionalEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryConditionalEntity>()
        }
        RecoveryConditionalUniqueEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryConditionalUniqueEntity>()
        }
        RecoveryConditionalUniqueCasefoldEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryConditionalUniqueCasefoldEntity>()
        }
        RecoveryConditionalUniqueEnumEntity::PATH => {
            commit_schema_fingerprint_for_entity::<RecoveryConditionalUniqueEnumEntity>()
        }
        _ => [0u8; 16],
    };
    row_op_for_path_with_schema(path, data_key, before, after, schema_fingerprint)
}

fn row_bytes_for(key: &RawDataKey) -> Option<Vec<u8>> {
    with_recovery_store(|store| {
        store.with_data(|data_store| data_store.get(key).map(|row| row.as_bytes().to_vec()))
    })
}

fn indexed_ids_for(entity: &RecoveryIndexedEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("index key build should succeed")
        .expect("index key should exist")
        .to_raw();

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                entry
                    .try_decode()
                    .expect("index entry decode should succeed")
                    .iter_ids()
                    .map(|storage_key| {
                        Ulid::from_value(&storage_key.as_value())
                            .expect("decoded index key should be a Ulid")
                    })
                    .collect::<BTreeSet<_>>()
            })
        })
    })
}

fn conditional_indexed_ids_for(entity: &RecoveryConditionalEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryConditionalEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("conditional index key build should succeed")
        .expect("conditional index key should exist")
        .to_raw();

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                entry
                    .try_decode()
                    .expect("conditional index entry decode should succeed")
                    .iter_ids()
                    .map(|storage_key| {
                        Ulid::from_value(&storage_key.as_value())
                            .expect("decoded conditional index key should be a Ulid")
                    })
                    .collect::<BTreeSet<_>>()
            })
        })
    })
}

fn index_key_bytes_snapshot() -> Vec<Vec<u8>> {
    let mut keys = with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store
                .entries()
                .into_iter()
                .map(|(raw_key, _)| raw_key.as_bytes().to_vec())
                .collect::<Vec<_>>()
        })
    });
    keys.sort();
    keys
}

// Capture one deterministic snapshot of row-store and index-store raw bytes.
fn recovery_store_snapshot() -> RecoveryStoreSnapshot {
    with_recovery_store(|store| {
        let mut data_rows = store.with_data(|data_store| {
            data_store
                .iter()
                .map(|entry| {
                    (
                        entry.key().as_bytes().to_vec(),
                        entry.value().as_bytes().to_vec(),
                    )
                })
                .collect::<Vec<_>>()
        });
        let mut index_rows = store.with_index(|index_store| {
            index_store
                .entries()
                .into_iter()
                .map(|(raw_key, raw_entry)| {
                    (raw_key.as_bytes().to_vec(), raw_entry.as_bytes().to_vec())
                })
                .collect::<Vec<_>>()
        });
        data_rows.sort();
        index_rows.sort();

        (data_rows, index_rows)
    })
}

// Apply prepared row operations through the forward (non-recovery) apply path.
fn apply_row_ops_forward(row_ops: &[CommitRowOp]) -> Result<(), InternalError> {
    for row_op in row_ops {
        DB.prepare_row_commit_op(row_op)?.apply();
    }

    Ok(())
}

fn indexed_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryIndexedEntity>(id)
        .expect("indexed key should build")
        .to_raw()
        .expect("indexed key should encode")
}

fn unique_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryUniqueEntity>(id)
        .expect("unique key should build")
        .to_raw()
        .expect("unique key should encode")
}

fn conditional_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryConditionalEntity>(id)
        .expect("conditional key should build")
        .to_raw()
        .expect("conditional key should encode")
}

fn conditional_unique_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryConditionalUniqueEntity>(id)
        .expect("conditional-unique key should build")
        .to_raw()
        .expect("conditional-unique key should encode")
}

fn conditional_unique_casefold_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryConditionalUniqueCasefoldEntity>(id)
        .expect("conditional-unique-casefold key should build")
        .to_raw()
        .expect("conditional-unique-casefold key should encode")
}

fn conditional_unique_enum_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<RecoveryConditionalUniqueEnumEntity>(id)
        .expect("conditional-unique-enum key should build")
        .to_raw()
        .expect("conditional-unique-enum key should encode")
}

fn indexed_row_bytes(entity: &RecoveryIndexedEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn unique_row_bytes(entity: &RecoveryUniqueEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_row_bytes(entity: &RecoveryConditionalEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_unique_row_bytes(entity: &RecoveryConditionalUniqueEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_unique_casefold_row_bytes(
    entity: &RecoveryConditionalUniqueCasefoldEntity,
) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_unique_enum_row_bytes(entity: &RecoveryConditionalUniqueEnumEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn canonical_row_bytes<E: crate::db::PersistedRow>(entity: &E) -> Vec<u8> {
    CanonicalRow::from_entity(entity)
        .expect("canonical row encoding should succeed")
        .into_raw_row()
        .as_bytes()
        .to_vec()
}

fn canonical_row_payload_bytes<E: crate::db::PersistedRow>(entity: &E) -> Vec<u8> {
    let row = CanonicalRow::from_entity(entity)
        .expect("canonical row encoding should succeed")
        .into_raw_row();

    decode_row_payload_bytes(row.as_bytes())
        .expect("canonical row payload should decode")
        .into_owned()
}

const RECOVERY_STATUS_ENUM_PATH: &str = "db::commit::tests::RecoveryConditionalStatus";

fn enum_status(variant: &str) -> Value {
    Value::Enum(ValueEnum::new(variant, Some(RECOVERY_STATUS_ENUM_PATH)))
}

// Build one deterministic seed snapshot used by forward/replay equivalence checks.
fn mixed_recovery_seed_ops() -> Vec<CommitRowOp> {
    let indexed_first_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9301),
        group: 41,
    };
    let indexed_second_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9302),
        group: 41,
    };
    let unique_first_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9303),
        email: "case-a@example.com".to_string(),
    };
    let unique_second_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9304),
        email: "case-b@example.com".to_string(),
    };

    vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_first_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_first_v1)),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_second_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_second_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_first_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_first_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_second_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_second_v1)),
        ),
    ]
}

// Build one mixed marker sequence with one operation per key over the seeded snapshot.
fn mixed_recovery_marker_ops() -> Vec<CommitRowOp> {
    let indexed_first_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9301),
        group: 41,
    };
    let indexed_first_v2 = RecoveryIndexedEntity {
        id: indexed_first_v1.id,
        group: 42,
    };
    let indexed_second_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9302),
        group: 41,
    };
    let indexed_third_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9305),
        group: 42,
    };
    let unique_first_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9303),
        email: "case-a@example.com".to_string(),
    };
    let unique_first_v2 = RecoveryUniqueEntity {
        id: unique_first_v1.id,
        email: "case-a2@example.com".to_string(),
    };
    let unique_second_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9304),
        email: "case-b@example.com".to_string(),
    };
    let unique_third_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9306),
        email: "case-c@example.com".to_string(),
    };

    vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_first_v1.id).as_bytes().to_vec(),
            Some(indexed_row_bytes(&indexed_first_v1)),
            Some(indexed_row_bytes(&indexed_first_v2)),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_second_v1.id).as_bytes().to_vec(),
            Some(indexed_row_bytes(&indexed_second_v1)),
            None,
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_first_v1.id).as_bytes().to_vec(),
            Some(unique_row_bytes(&unique_first_v1)),
            Some(unique_row_bytes(&unique_first_v2)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_second_v1.id).as_bytes().to_vec(),
            Some(unique_row_bytes(&unique_second_v1)),
            None,
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_third_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_third_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_third_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_third_v1)),
        ),
    ]
}

// Build one deterministic conditional-index seed snapshot used by forward/replay checks.
fn conditional_recovery_seed_ops() -> Vec<CommitRowOp> {
    let activate_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: false,
    };
    let deactivate_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: true,
    };
    let move_key_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let delete_active_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9404),
        group: 35,
        active: true,
    };
    let delete_inactive_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9405),
        group: 36,
        active: false,
    };

    vec![
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(activate_later.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&activate_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(deactivate_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&deactivate_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(move_key_later.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&move_key_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_active_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&delete_active_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_inactive_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&delete_inactive_later)),
        ),
    ]
}

// Build one deterministic conditional-index marker sequence that spans membership transitions.
fn conditional_recovery_marker_ops() -> Vec<CommitRowOp> {
    let activate_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: false,
    };
    let activate_after = RecoveryConditionalEntity {
        id: activate_before.id,
        group: activate_before.group,
        active: true,
    };
    let deactivate_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: true,
    };
    let deactivate_after = RecoveryConditionalEntity {
        id: deactivate_before.id,
        group: deactivate_before.group,
        active: false,
    };
    let move_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let move_after = RecoveryConditionalEntity {
        id: move_before.id,
        group: 34,
        active: true,
    };
    let delete_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9404),
        group: 35,
        active: true,
    };
    let delete_inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9405),
        group: 36,
        active: false,
    };
    let insert_inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9406),
        group: 37,
        active: false,
    };
    let insert_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9407),
        group: 38,
        active: true,
    };

    vec![
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(activate_before.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&activate_before)),
            Some(conditional_row_bytes(&activate_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(deactivate_before.id)
                .as_bytes()
                .to_vec(),
            Some(conditional_row_bytes(&deactivate_before)),
            Some(conditional_row_bytes(&deactivate_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(move_before.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&move_before)),
            Some(conditional_row_bytes(&move_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_active.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&delete_active)),
            None,
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_inactive.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&delete_inactive)),
            None,
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(insert_inactive.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&insert_inactive)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(insert_active.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&insert_active)),
        ),
    ]
}

#[test]
fn commit_forward_apply_and_replay_preserve_identical_store_state_for_mixed_marker_sequence() {
    let seed_ops = mixed_recovery_seed_ops();
    let marker_ops = mixed_recovery_marker_ops();

    // Phase 1: seed one shared pre-commit snapshot and apply forward marker mutations.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("seed state apply should succeed for mixed fixture");
    apply_row_ops_forward(marker_ops.as_slice())
        .expect("forward apply should succeed for deterministic mixed marker sequence");
    let forward_snapshot = recovery_store_snapshot();

    // Phase 2: replay the same marker from the same seeded snapshot and compare outcomes.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("seed state apply should succeed before replay marker");
    let marker =
        CommitMarker::new(marker_ops).expect("mixed marker sequence should build for replay path");
    begin_commit(marker).expect("replay marker begin_commit should persist marker");
    ensure_recovered(&DB).expect("replay marker should recover successfully");
    let replay_snapshot = recovery_store_snapshot();

    assert_eq!(
        replay_snapshot, forward_snapshot,
        "forward apply and replay must converge on identical data/index store state"
    );
    assert!(
        !commit_marker_present().expect("commit marker presence check should succeed"),
        "successful replay must clear the persisted marker"
    );
}

#[test]
fn conditional_index_forward_apply_and_replay_preserve_identical_store_state_for_membership_matrix()
{
    let seed_ops = conditional_recovery_seed_ops();
    let marker_ops = conditional_recovery_marker_ops();

    // Phase 1: apply the full conditional-membership transition matrix through live apply.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("conditional seed state apply should succeed for matrix fixture");
    apply_row_ops_forward(marker_ops.as_slice())
        .expect("forward apply should succeed for conditional membership transition matrix");
    let forward_snapshot = recovery_store_snapshot();

    // Phase 2: replay the same marker from the same seeded snapshot and compare outcomes.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("conditional seed state apply should succeed before replay marker");
    let marker = CommitMarker::new(marker_ops)
        .expect("conditional membership transition marker should build for replay");
    begin_commit(marker).expect("conditional replay marker begin_commit should persist marker");
    ensure_recovered(&DB).expect("conditional marker replay should recover successfully");
    let replay_snapshot = recovery_store_snapshot();

    assert_eq!(
        replay_snapshot, forward_snapshot,
        "conditional-index forward apply and replay must converge on identical store state"
    );
    assert!(
        !commit_marker_present().expect("commit marker presence check should succeed"),
        "successful conditional replay must clear the persisted marker"
    );

    // Phase 3: lock the final membership shape for representative transition outcomes.
    let activated = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: true,
    };
    let deactivated = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: false,
    };
    let moved_old_key = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let moved_new_key = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 34,
        active: true,
    };
    let inserted_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9407),
        group: 38,
        active: true,
    };
    assert_eq!(
        conditional_indexed_ids_for(&activated),
        Some(BTreeSet::from([activated.id])),
        "false->true transitions must create conditional index membership",
    );
    assert!(
        conditional_indexed_ids_for(&deactivated).is_none(),
        "true->false transitions must remove conditional index membership",
    );
    assert!(
        conditional_indexed_ids_for(&moved_old_key).is_none(),
        "true->true key-move transitions must remove old conditional index key membership",
    );
    assert_eq!(
        conditional_indexed_ids_for(&moved_new_key),
        Some(BTreeSet::from([moved_new_key.id])),
        "true->true key-move transitions must create new conditional index key membership",
    );
    assert_eq!(
        conditional_indexed_ids_for(&inserted_active),
        Some(BTreeSet::from([inserted_active.id])),
        "none->true inserts must publish conditional index membership",
    );
}

#[test]
fn index_key_new_rejects_missing_index_field_on_entity_model() {
    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(9901),
        group: 7,
    };

    let err = IndexKey::new(&entity, &RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL)
        .expect_err("index fields missing from the entity model must fail as invariants");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn conditional_index_mutation_tracks_false_true_false_membership_transitions() {
    reset_recovery_state();

    let inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9_931),
        group: 7,
        active: false,
    };
    let active = RecoveryConditionalEntity {
        id: inactive.id,
        group: inactive.group,
        active: true,
    };
    let inactive_again = RecoveryConditionalEntity {
        id: inactive.id,
        group: inactive.group,
        active: false,
    };
    let key = conditional_data_key(inactive.id);

    // Phase 1: inactive insert must not create a conditional index entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        None,
        Some(conditional_row_bytes(&inactive)),
    )])
    .expect("inactive conditional row insert should succeed");
    assert!(
        conditional_indexed_ids_for(&inactive).is_none(),
        "inactive conditional rows must stay absent from the index",
    );

    // Phase 2: false -> true transition must insert one entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        Some(conditional_row_bytes(&inactive)),
        Some(conditional_row_bytes(&active)),
    )])
    .expect("conditional false->true transition should succeed");
    assert_eq!(
        conditional_indexed_ids_for(&active),
        Some(BTreeSet::from([active.id])),
        "active conditional rows must be present in the index",
    );

    // Phase 3: true -> false transition must remove that entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        Some(conditional_row_bytes(&active)),
        Some(conditional_row_bytes(&inactive_again)),
    )])
    .expect("conditional true->false transition should succeed");
    assert!(
        conditional_indexed_ids_for(&inactive_again).is_none(),
        "inactive conditional rows must be removed from the index",
    );
}

#[test]
fn conditional_unique_index_skips_unique_validation_when_predicate_is_false() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9_941),
        email: "conditional-unique@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9_942),
        email: first_active.email.clone(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: baseline active row reserves the unique conditional slot.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(first_active.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_row_bytes(&first_active)),
    )])
    .expect("active conditional-unique insert should succeed");

    // Phase 2: duplicate email with inactive predicate must bypass unique checks.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_row_bytes(&second_inactive)),
    )])
    .expect("inactive duplicate should bypass conditional unique validation");

    // Phase 3: activating the duplicate row must enforce unique ownership.
    let err = apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        Some(conditional_unique_row_bytes(&second_inactive)),
        Some(conditional_unique_row_bytes(&second_active)),
    )])
    .expect_err("active duplicate should violate conditional unique index");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn conditional_unique_expression_index_skips_unique_validation_when_predicate_is_false() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueCasefoldEntity {
        id: Ulid::from_u128(9_946),
        email: "Conditional-CaseFold@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueCasefoldEntity {
        id: Ulid::from_u128(9_947),
        email: "conditional-casefold@example.com".to_string(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueCasefoldEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: baseline active row reserves the conditional+expression unique slot.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(first_active.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_casefold_row_bytes(&first_active)),
    )])
    .expect("active conditional expression-unique insert should succeed");

    // Phase 2: inactive duplicate bypasses unique validation while predicate=false.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_casefold_row_bytes(&second_inactive)),
    )])
    .expect("inactive duplicate should bypass conditional expression-unique validation");

    // Phase 3: activating the duplicate row must enforce canonical LOWER(email) uniqueness.
    let err = apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        Some(conditional_unique_casefold_row_bytes(&second_inactive)),
        Some(conditional_unique_casefold_row_bytes(&second_active)),
    )])
    .expect_err("active duplicate should violate conditional expression-unique index");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn conditional_unique_index_rejects_duplicate_active_enum_variant() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueEnumEntity {
        id: Ulid::from_u128(9_943),
        status: enum_status("Paid"),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueEnumEntity {
        id: Ulid::from_u128(9_944),
        status: first_active.status.clone(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueEnumEntity {
        id: second_inactive.id,
        status: second_inactive.status.clone(),
        active: true,
    };
    let third_active_distinct = RecoveryConditionalUniqueEnumEntity {
        id: Ulid::from_u128(9_945),
        status: enum_status("Pending"),
        active: true,
    };

    // Phase 1: baseline active enum variant reserves the unique conditional slot.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEnumEntity::PATH,
        conditional_unique_enum_data_key(first_active.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_enum_row_bytes(&first_active)),
    )])
    .expect("active conditional-unique enum insert should succeed");

    // Phase 2: predicate-false duplicate variant should bypass unique checks.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEnumEntity::PATH,
        conditional_unique_enum_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_enum_row_bytes(&second_inactive)),
    )])
    .expect("inactive duplicate enum variant should bypass conditional unique validation");

    // Phase 3: distinct active enum variant should still be accepted.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEnumEntity::PATH,
        conditional_unique_enum_data_key(third_active_distinct.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_enum_row_bytes(&third_active_distinct)),
    )])
    .expect("distinct active enum variant should remain insertable");

    // Phase 4: activating duplicate enum variant must enforce unique ownership.
    let err = apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEnumEntity::PATH,
        conditional_unique_enum_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        Some(conditional_unique_enum_row_bytes(&second_inactive)),
        Some(conditional_unique_enum_row_bytes(&second_active)),
    )])
    .expect_err("active duplicate enum variant should violate conditional unique index");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn commit_marker_round_trip_clears_after_finish() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(Vec::new()).expect("commit marker creation should succeed");

    let guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present after begin_commit"
    );

    finish_commit(guard, |_| Ok(())).expect("finish_commit should clear marker");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after finish_commit"
    );
}

#[test]
fn finish_commit_error_keeps_marker_for_recovery() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(Vec::new()).expect("commit marker creation should succeed");

    let guard = begin_commit(marker).expect("begin_commit should persist marker");
    let err = finish_commit(guard, |_| {
        Err(InternalError::executor_invariant("simulated apply failure"))
    })
    .expect_err("failed finish_commit should surface apply error");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed finish_commit should keep marker persisted for recovery replay"
    );

    // Cleanup so unrelated tests do not observe this intentionally-persisted marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
    with_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

#[test]
fn finish_commit_mixed_state_failure_rolls_back_index_prefix_without_row_visibility() {
    reset_recovery_state();

    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(915),
        group: 19,
    };
    let data_key = DataKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&entity);
    let row_op = row_op_for_path(
        RecoveryIndexedEntity::PATH,
        data_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    );
    let marker =
        CommitMarker::new(vec![row_op.clone()]).expect("commit marker creation should succeed");
    let guard = begin_commit(marker).expect("begin_commit should persist marker");

    // Simulate a mixed-state apply edge:
    // - apply index mutations
    // - fail before row write
    // - rollback must remove the applied index mutation
    let err = finish_commit(guard, |_| {
        let context = DB.context::<RecoveryIndexedEntity>();
        let prepared = prepare_row_commit_for_entity_with_structural_readers::<
            RecoveryIndexedEntity,
        >(&DB, &row_op, &context, &context)?;
        let rollback = prepared.snapshot_rollback();
        for index_op in prepared.index_ops {
            index_op.store.with_borrow_mut(|store| {
                if let Some(value) = index_op.value {
                    store.insert(index_op.key, value);
                } else {
                    store.remove(&index_op.key);
                }
            });
        }
        rollback_prepared_row_ops_reverse(vec![rollback]);

        Err(InternalError::executor_invariant(
            "simulated mixed-state row-stage failure after index apply",
        ))
    })
    .expect_err("mixed-state finish_commit path should surface apply error");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed mixed-state apply must keep marker persisted for recovery replay"
    );
    assert_eq!(
        row_bytes_for(&data_key),
        None,
        "mixed-state apply failure must not leave row bytes visible"
    );
    assert!(
        indexed_ids_for(&entity).is_none(),
        "mixed-state apply failure must not leave index membership visible"
    );

    // Cleanup so unrelated tests do not observe this intentionally-persisted marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_replay_is_idempotent() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(901),
    };
    let raw_key = DataKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&entity);
    let marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes.clone()),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );

    // First replay applies marker operations and clears the marker.
    ensure_recovered(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first = row_bytes_for(&raw_key);
    assert_eq!(first, Some(row_bytes));

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second recovery replay should be a no-op");
    let second = row_bytes_for(&raw_key);
    assert_eq!(second, first);
}

#[test]
fn recovery_rejects_corrupt_marker_data_key_decode() {
    reset_recovery_state();

    let row_bytes = canonical_row_bytes(&RecoveryTestEntity {
        id: Ulid::from_u128(902),
    });
    let malformed_key = vec![0u8; DataKey::STORED_SIZE_USIZE.saturating_sub(1)];
    let mut marker_payload = Vec::new();
    marker_payload.extend_from_slice(&[0u8; 16]);
    marker_payload.extend_from_slice(&1u32.to_le_bytes());
    marker_payload.extend_from_slice(
        &u32::try_from(RecoveryTestEntity::PATH.len())
            .expect("entity path length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(RecoveryTestEntity::PATH.as_bytes());
    marker_payload.extend_from_slice(
        &u32::try_from(malformed_key.len())
            .expect("data key length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(&malformed_key);
    marker_payload.push(0b0000_0010);
    marker_payload.extend_from_slice(
        &u32::try_from(row_bytes.len())
            .expect("after payload length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(&row_bytes);
    marker_payload.extend_from_slice(&commit_schema_fingerprint_for_entity::<RecoveryTestEntity>());

    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("raw marker envelope encode should succeed");
    let control_slot_bytes =
        store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes, Vec::new())
            .expect("raw control-slot encode should succeed");
    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
    .expect("corrupt test marker should persist raw bytes");

    let err = ensure_recovered(&DB).expect_err("recovery should reject corrupt marker bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    let marker_still_present = store::with_commit_store(|store| Ok(!store.is_empty()))
        .expect("raw commit marker presence check should succeed");
    assert!(
        marker_still_present,
        "marker should remain present when recovery prevalidation fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_incompatible_marker_format_version_fail_closed() {
    reset_recovery_state();

    let marker = CommitMarker {
        id: [0xAB; 16],
        row_ops: Vec::new(),
    };
    let marker_payload =
        encode_commit_marker_payload(&marker).expect("marker payload encode should succeed");
    let future_version = COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_add(1);
    let marker_bytes =
        store::CommitStore::encode_raw_marker_envelope_for_tests(future_version, marker_payload)
            .expect("future-version marker envelope encode should succeed");
    let control_slot_bytes =
        store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes, Vec::<u8>::new())
            .expect("control-slot envelope encode should succeed");
    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
    .expect("test helper should persist raw marker bytes");

    let err =
        ensure_recovered(&DB).expect_err("recovery should reject incompatible marker versions");
    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    let marker_still_present = store::with_commit_store(|store| Ok(!store.is_empty()))
        .expect("raw commit marker presence check should succeed");
    assert!(
        marker_still_present,
        "marker should remain present when recovery decode fails compatibility checks"
    );

    // Cleanup so unrelated tests do not observe this intentionally-incompatible marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn single_row_control_slot_direct_encoder_matches_canonical_two_stage_encoding() {
    let marker_id = [0x5A; 16];
    let raw_key = DataKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(111))
        .expect("single-row encoder test data key should build")
        .to_raw()
        .expect("single-row encoder test data key should encode");
    let row_op = row_op_for_path(
        RecoveryPayloadEntity::PATH,
        raw_key.as_bytes().to_vec(),
        Some(canonical_row_bytes(&RecoveryPayloadEntity {
            id: Ulid::from_u128(111),
            name: "before".to_string(),
        })),
        Some(canonical_row_bytes(&RecoveryPayloadEntity {
            id: Ulid::from_u128(111),
            name: "after".to_string(),
        })),
    );
    let migration_bytes = vec![0xAA, 0xBB, 0xCC];

    let marker_payload = encode_single_row_commit_marker_payload(marker_id, &row_op)
        .expect("single-row marker payload encode should succeed");
    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("single-row marker envelope encode should succeed");
    let canonical = store::CommitStore::encode_raw_control_slot_for_tests(
        marker_bytes,
        migration_bytes.clone(),
    )
    .expect("canonical control-slot encode should succeed");
    let direct = store::CommitStore::encode_raw_single_row_control_slot_for_tests(
        marker_id,
        &row_op,
        migration_bytes,
    )
    .expect("direct single-row control-slot encode should succeed");

    assert_eq!(
        direct, canonical,
        "single-row direct control-slot encoding must stay byte-for-byte canonical"
    );
}

#[test]
fn multi_row_control_slot_direct_encoder_matches_canonical_two_stage_encoding() {
    let marker = CommitMarker {
        id: [0x6B; 16],
        row_ops: vec![
            row_op_for_path(
                RecoveryPayloadEntity::PATH,
                DataKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(211))
                    .expect("multi-row encoder first key should build")
                    .to_raw()
                    .expect("multi-row encoder first key should encode")
                    .as_bytes()
                    .to_vec(),
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(211),
                    name: "before-a".to_string(),
                })),
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(211),
                    name: "after-a".to_string(),
                })),
            ),
            row_op_for_path(
                RecoveryPayloadEntity::PATH,
                DataKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(212))
                    .expect("multi-row encoder second key should build")
                    .to_raw()
                    .expect("multi-row encoder second key should encode")
                    .as_bytes()
                    .to_vec(),
                None,
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(212),
                    name: "after-b".to_string(),
                })),
            ),
        ],
    };
    let migration_bytes = vec![0x11, 0x22, 0x33, 0x44];

    let marker_payload = encode_commit_marker_payload(&marker)
        .expect("multi-row marker payload encode should succeed");
    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("multi-row marker envelope encode should succeed");
    let canonical = store::CommitStore::encode_raw_control_slot_for_tests(
        marker_bytes,
        migration_bytes.clone(),
    )
    .expect("canonical multi-row control-slot encode should succeed");
    let direct =
        store::CommitStore::encode_raw_direct_control_slot_for_tests(&marker, migration_bytes)
            .expect("direct multi-row control-slot encode should succeed");

    assert_eq!(
        direct, canonical,
        "multi-row direct control-slot encoding must stay byte-for-byte canonical"
    );
}

#[test]
fn recovery_replay_rolls_back_applied_prefix_when_later_marker_op_fails_prepare() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(913),
        group: 17,
    };
    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let first_row = canonical_row_bytes(&first);

    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(914),
        group: 18,
    };
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let second_row = canonical_row_bytes(&second);
    let unsupported_path = "commit_tests::UnknownEntity";
    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            unsupported_path,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
        ),
    ])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB).expect_err(
        "recovery should fail when a later marker op has an unsupported entity path during replay",
    );
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay should keep marker persisted for later recovery attempts"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        None,
        "recovery must roll back the already-applied prefix row when a later marker op fails"
    );
    assert!(
        indexed_ids_for(&first).is_none(),
        "recovery must roll back the already-applied prefix index mutation when a later marker op fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_unsupported_entity_path_without_fallback() {
    reset_recovery_state();

    let raw_key = DataKey::try_new::<RecoveryTestEntity>(Ulid::from_u128(911))
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&RecoveryTestEntity {
        id: Ulid::from_u128(911),
    });
    let unsupported_path = "commit_tests::UnknownEntity";
    let marker = CommitMarker::new(vec![row_op_for_path(
        unsupported_path,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err =
        ensure_recovered(&DB).expect_err("recovery should reject unsupported entity path markers");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        err.message.contains("unsupported entity path"),
        "unsupported entity diagnostics should include dispatch context: {err:?}"
    );
    assert!(
        err.message.contains(unsupported_path),
        "unsupported entity diagnostics should include the unknown path: {err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery dispatch fails"
    );
    assert_eq!(
        row_bytes_for(&raw_key),
        None,
        "recovery must not partially apply rows when dispatch fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-unsupported marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_miswired_hook_entity_path_mismatch_as_corruption() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(912),
    };
    let raw_key = DataKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&entity);
    let marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&MISWIRED_DB)
        .expect_err("miswired hook dispatch should fail with path mismatch corruption");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        err.message.contains("commit marker entity path mismatch"),
        "dispatch corruption should include mismatch context: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryIndexedEntity::PATH),
        "dispatch corruption should include the hook-expected entity path: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryTestEntity::PATH),
        "dispatch corruption should include the marker entity path: {err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery dispatch detects hook mismatch"
    );
    assert_eq!(
        row_bytes_for(&raw_key),
        None,
        "recovery must not partially apply rows when hook/entity dispatch mismatches"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn runtime_hook_lookup_rejects_duplicate_entity_tags() {
    #[cfg(debug_assertions)]
    {
        let Err(err) = std::panic::catch_unwind(duplicate_name_db) else {
            panic!("duplicate entity tags must fail during hook table construction");
        };
        let message = if let Some(message) = err.downcast_ref::<&'static str>() {
            (*message).to_string()
        } else if let Some(message) = err.downcast_ref::<String>() {
            message.clone()
        } else {
            panic!("duplicate-tag panic payload must be string-like: {err:?}");
        };

        assert!(
            message.contains("duplicate EntityTag detected in runtime hooks"),
            "duplicate-tag construction panic should include invariant context: {message}"
        );
    }

    #[cfg(not(debug_assertions))]
    {
        let duplicate_name_db = duplicate_name_db();
        let Err(err) =
            duplicate_name_db.runtime_hook_for_entity_tag(RecoveryTestEntity::ENTITY_TAG)
        else {
            panic!("duplicate entity tags must fail runtime-hook lookup")
        };
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message
                .contains("duplicate runtime hooks for entity tag"),
            "duplicate-tag runtime-hook lookup should include invariant context: {err:?}"
        );
        let expected_tag = RecoveryTestEntity::ENTITY_TAG.value().to_string();
        assert!(
            err.message.contains(expected_tag.as_str()),
            "duplicate-tag runtime-hook lookup should include conflicting tag: {err:?}"
        );
    }
}

#[test]
fn prepare_row_commit_rejects_duplicate_entity_paths() {
    let raw_key = DataKey::try_new::<RecoveryTestEntity>(Ulid::from_u128(9_991))
        .expect("duplicate-path test data key should build")
        .to_raw()
        .expect("duplicate-path test data key should encode");
    let op = row_op_for_path(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        None,
    );
    let Err(err) = DUPLICATE_PATH_DB.prepare_row_commit_op(&op) else {
        panic!("duplicate entity paths must fail prepare dispatch")
    };
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert!(
        err.message
            .contains("duplicate runtime hooks for entity path"),
        "duplicate-path prepare dispatch should include invariant context: {err:?}"
    );
    assert!(
        err.message.contains(RecoveryTestEntity::PATH),
        "duplicate-path prepare dispatch should include conflicting path: {err:?}"
    );
}

#[test]
fn recovery_replay_rejects_schema_fingerprint_mismatch() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(9801),
    };
    let key = DataKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row = canonical_row_bytes(&entity);

    let marker = CommitMarker::new(vec![row_op_for_path_with_schema(
        RecoveryTestEntity::PATH,
        key.as_bytes().to_vec(),
        None,
        Some(row),
        commit_schema_fingerprint_for_entity::<RecoveryIndexedEntity>(),
    )])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB)
        .expect_err("recovery should reject mismatched commit schema fingerprint");
    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        err.message.contains("schema fingerprint mismatch"),
        "fingerprint mismatch should include explicit reason: {err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when replay rejects schema fingerprint mismatch"
    );
    assert_eq!(
        row_bytes_for(&key),
        None,
        "row bytes must remain absent when replay fails before apply"
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_replay_merges_multi_row_shared_index_key() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(903),
        group: 7,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(904),
        group: 7,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), Some(second_row));

    let indexed_ids_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_ids_second = indexed_ids_for(&second).expect("second index entry should exist");
    assert_eq!(
        indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replays_interrupted_atomic_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(907),
        group: 9,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(908),
        group: 9,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Simulate an interrupted atomic batch by persisting the marker without apply.
    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert!(
        indexed_ids_for(&first).is_none(),
        "interrupted batch index state must not be visible before recovery replay"
    );

    // First replay applies marker row ops and clears the marker.
    ensure_recovered(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(first_row));
    assert_eq!(second_after, Some(second_row));

    let indexed_after_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_after_second = indexed_ids_for(&second).expect("second index entry should exist");
    let expected_first = std::iter::once(first.id).collect::<BTreeSet<_>>();
    let expected_second = std::iter::once(second.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_after_first, expected_first);
    assert_eq!(indexed_after_second, expected_second);

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second recovery replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    let indexed_second_first =
        indexed_ids_for(&first).expect("first index entry should remain after idempotent replay");
    let indexed_second_second =
        indexed_ids_for(&second).expect("second index entry should remain after idempotent replay");
    assert_eq!(indexed_second_first, expected_first);
    assert_eq!(indexed_second_second, expected_second);
}

#[test]
fn recovery_replay_interrupted_conflicting_unique_batch_fails_closed() {
    reset_recovery_state();

    let first = RecoveryUniqueEntity {
        id: Ulid::from_u128(911),
        email: "dup@example.com".to_string(),
    };
    let second = RecoveryUniqueEntity {
        id: Ulid::from_u128(912),
        email: "dup@example.com".to_string(),
    };

    let first_key = DataKey::try_new::<RecoveryUniqueEntity>(first.id)
        .expect("first unique data key should build")
        .to_raw()
        .expect("first unique data key should encode");
    let second_key = DataKey::try_new::<RecoveryUniqueEntity>(second.id)
        .expect("second unique data key should build")
        .to_raw()
        .expect("second unique data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Simulate interrupted atomic marker persistence for two writes that conflict
    // on one unique secondary index value.
    let marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
    ])
    .expect("conflicting unique marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist conflicting unique marker");

    let err = ensure_recovered(&DB)
        .expect_err("recovery should fail closed on conflicting unique replay marker");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed unique replay must keep marker persisted for retry",
    );
    assert!(
        index_key_bytes_snapshot().is_empty(),
        "failed rebuild must not leave partially rebuilt unique index state",
    );

    let retry_err = ensure_recovered(&DB)
        .expect_err("repeated recovery attempts should remain fail-closed until marker is fixed");
    assert_eq!(retry_err.class, ErrorClass::Conflict);
    assert_eq!(retry_err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "retry failure must keep marker persisted",
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn unique_conflict_classification_parity_holds_between_live_apply_and_replay() {
    reset_recovery_state();

    // Phase 1: capture live save-path unique conflict classification.
    let save = SaveExecutor::<RecoveryUniqueEntity>::new(DB, false);
    save.insert(RecoveryUniqueEntity {
        id: Ulid::from_u128(9211),
        email: "dup-live-replay@example.com".to_string(),
    })
    .expect("seed unique row should save in live path");
    let live_err = save
        .insert(RecoveryUniqueEntity {
            id: Ulid::from_u128(9212),
            email: "dup-live-replay@example.com".to_string(),
        })
        .expect_err("live save path should reject duplicate unique value");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);
    assert!(
        live_err.message.contains("index constraint violation"),
        "live unique conflict should remain explicit: {live_err:?}"
    );

    // Phase 2: capture replay-path unique conflict classification for the same semantic conflict.
    reset_recovery_state();
    let replay_first = RecoveryUniqueEntity {
        id: Ulid::from_u128(9221),
        email: "dup-live-replay@example.com".to_string(),
    };
    let replay_second = RecoveryUniqueEntity {
        id: Ulid::from_u128(9222),
        email: "dup-live-replay@example.com".to_string(),
    };

    let replay_first_key = DataKey::try_new::<RecoveryUniqueEntity>(replay_first.id)
        .expect("first replay key should build")
        .to_raw()
        .expect("first replay key should encode");
    let replay_second_key = DataKey::try_new::<RecoveryUniqueEntity>(replay_second.id)
        .expect("second replay key should build")
        .to_raw()
        .expect("second replay key should encode");

    let replay_first_row = canonical_row_bytes(&replay_first);
    let replay_second_row = canonical_row_bytes(&replay_second);

    let replay_marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            replay_first_key.as_bytes().to_vec(),
            None,
            Some(replay_first_row),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            replay_second_key.as_bytes().to_vec(),
            None,
            Some(replay_second_row),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
    ])
    .expect("replay unique-conflict marker should build");
    begin_commit(replay_marker).expect("begin_commit should persist replay conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject duplicate unique value marker");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        replay_err.message.contains("index constraint violation"),
        "replay unique conflict should remain explicit: {replay_err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay unique conflict must keep marker persisted for retry",
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn unique_expression_index_enforces_casefolded_conflicts_on_live_saves() {
    reset_recovery_state();

    // Phase 1: seed one row with mixed-case email.
    let save = SaveExecutor::<RecoveryUniqueCasefoldEntity>::new(DB, false);
    save.insert(RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9311),
        email: "CaseFold@Test.example".to_string(),
    })
    .expect("seed casefold unique row should save");

    // Phase 2: inserting a case-variant duplicate must violate unique ownership.
    let conflicting = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9312),
        email: "casefold@test.example".to_string(),
    };
    let live_err = save
        .insert(conflicting.clone())
        .expect_err("casefold duplicate should violate unique expression index");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);
    assert!(
        live_err.message.contains("index constraint violation"),
        "casefold unique conflict should remain explicit: {live_err:?}"
    );

    // Phase 3: rejected insert must not leave a persisted primary row.
    let conflicting_key = DataKey::try_new::<RecoveryUniqueCasefoldEntity>(conflicting.id)
        .expect("conflicting casefold key should build")
        .to_raw()
        .expect("conflicting casefold key should encode");
    assert!(
        row_bytes_for(&conflicting_key).is_none(),
        "conflicting casefold insert should not persist primary row",
    );
}

#[test]
fn unique_expression_conflict_classification_parity_holds_between_live_apply_and_replay() {
    reset_recovery_state();

    // Phase 1: capture live save-path casefold expression-unique conflict classification.
    let save = SaveExecutor::<RecoveryUniqueCasefoldEntity>::new(DB, false);
    save.insert(RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9313),
        email: "CaseFold-Replay@Test.example".to_string(),
    })
    .expect("seed casefold replay row should save in live path");
    let live_err = save
        .insert(RecoveryUniqueCasefoldEntity {
            id: Ulid::from_u128(9314),
            email: "casefold-replay@test.example".to_string(),
        })
        .expect_err("live save path should reject casefold duplicate unique value");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);
    assert!(
        live_err.message.contains("index constraint violation"),
        "live casefold unique conflict should remain explicit: {live_err:?}"
    );

    // Phase 2: capture replay-path classification for the same casefold semantic conflict.
    reset_recovery_state();
    let replay_first = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9315),
        email: "CaseFold-Replay@Test.example".to_string(),
    };
    let replay_second = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9316),
        email: "casefold-replay@test.example".to_string(),
    };

    let replay_marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueCasefoldEntity::PATH,
            DataKey::try_new::<RecoveryUniqueCasefoldEntity>(replay_first.id)
                .expect("first casefold replay key should build")
                .to_raw()
                .expect("first casefold replay key should encode")
                .as_bytes()
                .to_vec(),
            None,
            Some(canonical_row_bytes(&replay_first)),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueCasefoldEntity::PATH,
            DataKey::try_new::<RecoveryUniqueCasefoldEntity>(replay_second.id)
                .expect("second casefold replay key should build")
                .to_raw()
                .expect("second casefold replay key should encode")
                .as_bytes()
                .to_vec(),
            None,
            Some(canonical_row_bytes(&replay_second)),
            commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>(),
        ),
    ])
    .expect("replay casefold unique-conflict marker should build");
    begin_commit(replay_marker)
        .expect("begin_commit should persist replay casefold conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject casefold duplicate unique value marker");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        replay_err.message.contains("index constraint violation"),
        "replay casefold unique conflict should remain explicit: {replay_err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay casefold unique conflict must keep marker persisted for retry",
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn conditional_unique_conflict_classification_parity_holds_between_live_update_and_replay() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9231),
        email: "dup-conditional-live-replay@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9232),
        email: first_active.email.clone(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: capture live save-path conditional-unique conflict classification.
    let save = SaveExecutor::<RecoveryConditionalUniqueEntity>::new(DB, false);
    save.insert(first_active.clone())
        .expect("seed active conditional-unique row should save in live path");
    save.insert(second_inactive.clone())
        .expect("inactive duplicate should save in live path");
    let live_err = save
        .update(second_active.clone())
        .expect_err("live update path should reject duplicate conditional-unique activation");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);
    assert!(
        live_err.message.contains("index constraint violation"),
        "live conditional-unique conflict should remain explicit: {live_err:?}"
    );

    // Phase 2: capture replay-path conditional-unique conflict for the same activation conflict.
    reset_recovery_state();
    let first_key = conditional_unique_data_key(first_active.id);
    let second_key = conditional_unique_data_key(second_inactive.id);
    let first_row = conditional_unique_row_bytes(&first_active);
    let second_inactive_row = conditional_unique_row_bytes(&second_inactive);
    let second_active_row = conditional_unique_row_bytes(&second_active);

    apply_row_ops_forward(&[
        row_op_for_path(
            RecoveryConditionalUniqueEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            RecoveryConditionalUniqueEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_inactive_row.clone()),
        ),
    ])
    .expect("seed state apply should succeed before replay conflict marker");

    let replay_marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        second_key.as_bytes().to_vec(),
        Some(second_inactive_row.clone()),
        Some(second_active_row),
    )])
    .expect("replay conditional-unique conflict marker should build");
    begin_commit(replay_marker).expect("begin_commit should persist replay conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject duplicate conditional-unique activation");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        replay_err.message.contains("index constraint violation"),
        "replay conditional-unique conflict should remain explicit: {replay_err:?}"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay conditional-unique conflict must keep marker persisted for retry",
    );
    assert_eq!(
        row_bytes_for(&second_key),
        Some(second_inactive_row),
        "failed replay must keep the prior predicate-false row state visible",
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[expect(clippy::too_many_lines)]
#[test]
fn recovery_replays_interrupted_atomic_update_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let old_first = RecoveryIndexedEntity {
        id: Ulid::from_u128(909),
        group: 10,
    };
    let old_second = RecoveryIndexedEntity {
        id: Ulid::from_u128(910),
        group: 10,
    };
    let new_first = RecoveryIndexedEntity {
        id: old_first.id,
        group: 11,
    };
    let new_second = RecoveryIndexedEntity {
        id: old_second.id,
        group: 11,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(old_first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(old_second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");

    let old_first_row = canonical_row_bytes(&old_first);
    let old_second_row = canonical_row_bytes(&old_second);
    let new_first_row = canonical_row_bytes(&new_first);
    let new_second_row = canonical_row_bytes(&new_second);

    // Phase 1: establish the pre-update durable state (group=10).
    let seed_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(old_first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(old_second_row.clone()),
        ),
    ])
    .expect("seed marker creation should succeed");
    begin_commit(seed_marker).expect("seed begin_commit should persist marker");
    ensure_recovered(&DB).expect("seed replay should succeed");

    let old_indexed_ids_first =
        indexed_ids_for(&old_first).expect("old first index entry should exist after seed replay");
    let old_indexed_ids_second = indexed_ids_for(&old_second)
        .expect("old second index entry should exist after seed replay");
    assert_eq!(
        old_indexed_ids_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        old_indexed_ids_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(row_bytes_for(&first_key), Some(old_first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(old_second_row.clone()));

    // Phase 2: simulate an interrupted atomic update marker (group=10 -> group=11).
    let update_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            Some(old_first_row),
            Some(new_first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            Some(old_second_row),
            Some(new_second_row.clone()),
        ),
    ])
    .expect("update marker creation should succeed");
    begin_commit(update_marker).expect("update begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        Some(canonical_row_bytes(&old_first)),
        "pre-recovery row bytes should still reflect old update state"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        Some(canonical_row_bytes(&old_second)),
        "pre-recovery row bytes should still reflect old update state"
    );
    let pre_update_old_indexed_first =
        indexed_ids_for(&old_first).expect("old first index entry should still exist pre-recovery");
    let pre_update_old_indexed_second = indexed_ids_for(&old_second)
        .expect("old second index entry should still exist pre-recovery");
    assert_eq!(
        pre_update_old_indexed_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        pre_update_old_indexed_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert!(
        indexed_ids_for(&new_first).is_none(),
        "new index entry must not be visible before update replay"
    );

    // First replay applies update row ops and clears the marker.
    ensure_recovered(&DB).expect("update replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be cleared after replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(new_first_row));
    assert_eq!(second_after, Some(new_second_row));
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should be removed after update replay"
    );
    let new_indexed_ids_first = indexed_ids_for(&new_first)
        .expect("new first index entry should exist after update replay");
    let new_indexed_ids_second = indexed_ids_for(&new_second)
        .expect("new second index entry should exist after update replay");
    assert_eq!(
        new_indexed_ids_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_ids_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second update replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should remain absent after idempotent replay"
    );
    let new_indexed_second_first = indexed_ids_for(&new_first)
        .expect("new first index entry should remain after idempotent replay");
    let new_indexed_second_second = indexed_ids_for(&new_second)
        .expect("new second index entry should remain after idempotent replay");
    assert_eq!(
        new_indexed_second_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_second_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replay_mixed_save_save_delete_sequence_preserves_final_index_state() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(905),
        group: 8,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(906),
        group: 8,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Phase 1: replay two inserts sharing the same index key.
    let save_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(save_marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("recovery replay should succeed");
    assert_eq!(row_bytes_for(&first_key), Some(first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(second_row.clone()));

    let inserted_indexed_ids_first =
        indexed_ids_for(&first).expect("first index entry should exist after insert replay");
    let inserted_indexed_ids_second =
        indexed_ids_for(&second).expect("second index entry should exist after insert replay");
    assert_eq!(
        inserted_indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        inserted_indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );

    // Phase 2: replay a delete that removes one of the inserted rows.
    let delete_marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryIndexedEntity::PATH,
        second_key.as_bytes().to_vec(),
        Some(second_row),
        None,
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_marker).expect("delete begin_commit should persist marker");

    ensure_recovered(&DB).expect("delete recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), None);

    let indexed_ids = indexed_ids_for(&first).expect("index entry should exist after replay");
    let expected_ids = std::iter::once(first.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_ids, expected_ids);
}

#[test]
fn recovery_replay_preserves_index_key_raw_bytes_across_reloads() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(913),
        group: 20,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(914),
        group: 21,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("first recovery replay should succeed");
    let first_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        first_snapshot, expected,
        "index key bytes after replay should match precomputed canonical bytes"
    );

    ensure_recovered(&DB).expect("second recovery replay should be no-op");
    let second_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        second_snapshot, expected,
        "index key bytes should remain stable after subsequent replay"
    );
    assert_eq!(second_snapshot, first_snapshot);
}

#[test]
fn recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(920),
        group: 30,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(921),
        group: 31,
    };
    let stale = RecoveryIndexedEntity {
        id: Ulid::from_u128(999),
        group: 99,
    };

    let first_key = DataKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DataKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale key build should succeed")
        .expect("stale key should exist")
        .to_raw();
    let stale_storage_key =
        StorageKey::try_from_value(&stale.id.to_value()).expect("stale storage key should encode");
    let stale_entry = RawIndexEntry::try_from_keys(vec![stale_storage_key])
        .expect("stale index entry should encode");

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert_raw_for_test(
                first_key,
                RawRow::try_new(first_row).expect("first row raw construction should succeed"),
            );
            data_store.insert_raw_for_test(
                second_key,
                RawRow::try_new(second_row).expect("second row raw construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild indexes from data rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery"
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale index entries and recreate canonical entries from rows"
    );
    assert_eq!(
        indexed_ids_for(&first).expect("first index entry should exist"),
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_for(&second).expect("second index entry should exist"),
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_startup_gate_rebuilds_conditional_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let active = RecoveryConditionalEntity {
        id: Ulid::from_u128(926),
        group: 61,
        active: true,
    };
    let inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(927),
        group: 62,
        active: false,
    };
    let stale = RecoveryConditionalEntity {
        id: Ulid::from_u128(928),
        group: 63,
        active: true,
    };

    let active_key = DataKey::try_new::<RecoveryConditionalEntity>(active.id)
        .expect("active data key should build")
        .to_raw()
        .expect("active data key should encode");
    let inactive_key = DataKey::try_new::<RecoveryConditionalEntity>(inactive.id)
        .expect("inactive data key should build")
        .to_raw()
        .expect("inactive data key should encode");
    let active_row = canonical_row_bytes(&active);
    let inactive_row = canonical_row_bytes(&inactive);

    let index = RecoveryConditionalEntity::MODEL.indexes()[0];
    let inactive_index_key = IndexKey::new(&inactive, index)
        .expect("inactive index key build should succeed")
        .expect("inactive index key should exist")
        .to_raw();
    let stale_index_key = IndexKey::new(&stale, index)
        .expect("stale index key build should succeed")
        .expect("stale index key should exist")
        .to_raw();
    let inactive_entry = RawIndexEntry::try_from_keys(vec![
        StorageKey::try_from_value(&inactive.id.to_value())
            .expect("inactive storage key should encode"),
    ])
    .expect("inactive stale index entry should encode");
    let stale_entry = RawIndexEntry::try_from_keys(vec![
        StorageKey::try_from_value(&stale.id.to_value()).expect("stale storage key should encode"),
    ])
    .expect("stale index entry should encode");

    // Phase 1: seed authoritative rows and intentionally stale conditional index state.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert_raw_for_test(
                active_key,
                RawRow::try_new(active_row).expect("active raw row construction should succeed"),
            );
            data_store.insert_raw_for_test(
                inactive_key,
                RawRow::try_new(inactive_row)
                    .expect("inactive raw row construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(inactive_index_key, inactive_entry);
            index_store.insert(stale_index_key, stale_entry);
        });
    });

    // Phase 2: startup recovery must rebuild conditional index state from row truth only.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild conditional indexes from row truth");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after conditional startup recovery",
    );

    let mut expected = vec![
        IndexKey::new(&active, index)
            .expect("active conditional index key build should succeed")
            .expect("active conditional index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();
    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should keep predicate-true rows only and purge stale/predicate-false entries",
    );
    assert_eq!(
        conditional_indexed_ids_for(&active).expect("active conditional index entry should exist"),
        std::iter::once(active.id).collect::<BTreeSet<_>>(),
        "predicate-true rows must remain indexed after startup rebuild",
    );
    assert!(
        conditional_indexed_ids_for(&inactive).is_none(),
        "predicate-false rows must remain absent from the conditional index after rebuild",
    );
    assert!(
        conditional_indexed_ids_for(&stale).is_none(),
        "stale index-only rows must be dropped during conditional rebuild",
    );
}

#[test]
fn recovery_startup_gate_rebuilds_upper_expression_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let first = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(940),
        email: "Alice@Example.Com".to_string(),
    };
    let second = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(941),
        email: "bob@example.com".to_string(),
    };
    let stale = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(999),
        email: "stale@example.com".to_string(),
    };

    let first_key = DataKey::try_new::<RecoveryUpperExpressionEntity>(first.id)
        .expect("first expression data key should build")
        .to_raw()
        .expect("first expression data key should encode");
    let second_key = DataKey::try_new::<RecoveryUpperExpressionEntity>(second.id)
        .expect("second expression data key should build")
        .to_raw()
        .expect("second expression data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryUpperExpressionEntity::MODEL.indexes()[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale expression index key build should succeed")
        .expect("stale expression index key should exist")
        .to_raw();
    let stale_entry = RawIndexEntry::try_from_keys(vec![
        StorageKey::try_from_value(&stale.id.to_value()).expect("stale expression storage key"),
    ])
    .expect("stale expression index entry should encode");

    // Phase 1: seed authoritative rows and intentionally stale expression-index state.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert_raw_for_test(
                first_key,
                RawRow::try_new(first_row)
                    .expect("first expression raw row construction should succeed"),
            );
            data_store.insert_raw_for_test(
                second_key,
                RawRow::try_new(second_row)
                    .expect("second expression raw row construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });

    // Phase 2: startup recovery must rebuild expression index state from row truth only.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild expression indexes from row truth");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after expression startup recovery",
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first expression index key build should succeed")
            .expect("first expression index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second expression index key build should succeed")
            .expect("second expression index key should exist")
            .to_raw()
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale expression index entries and recreate canonical UPPER(email) keys from rows",
    );
}

#[test]
fn recovery_startup_rebuild_rejects_future_row_format_fail_closed() {
    reset_recovery_state();

    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(925),
        group: 34,
    };
    let raw_key = DataKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("row key should build")
        .to_raw()
        .expect("row key should encode");
    let payload = canonical_row_payload_bytes(&entity);
    let future_version = ROW_FORMAT_VERSION_CURRENT.saturating_add(1);
    let future_version_row = serialize_row_payload_with_version(payload, future_version)
        .expect("future-version row envelope should encode");

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert_raw_for_test(
                raw_key,
                RawRow::try_new(future_version_row)
                    .expect("future-version row should fit raw row bounds"),
            );
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    let err = ensure_recovered(&DB).expect_err("recovery should reject future row formats");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery rejects incompatible row format",
    );
    assert!(
        row_bytes_for(&raw_key).is_some(),
        "failed recovery must not discard persisted rows",
    );
    assert!(
        indexed_ids_for(&entity).is_none(),
        "failed recovery must not publish index state for incompatible rows",
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_startup_rebuild_fail_closed_restores_previous_index_state_on_corrupt_row() {
    reset_recovery_state();

    let sentinel = RecoveryIndexedEntity {
        id: Ulid::from_u128(922),
        group: 77,
    };
    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let sentinel_key = IndexKey::new(&sentinel, index)
        .expect("sentinel key build should succeed")
        .expect("sentinel key should exist")
        .to_raw();
    let sentinel_storage_key = StorageKey::try_from_value(&sentinel.id.to_value())
        .expect("sentinel storage key should encode");
    let sentinel_entry = RawIndexEntry::try_from_keys(vec![sentinel_storage_key])
        .expect("sentinel entry should encode");

    with_recovery_store(|store| {
        store.with_index_mut(|index_store| {
            index_store.insert(sentinel_key.clone(), sentinel_entry);
        });
    });
    let before_snapshot = index_key_bytes_snapshot();

    let bad_key = DataKey::try_new::<RecoveryIndexedEntity>(Ulid::from_u128(923))
        .expect("bad data key should build")
        .to_raw()
        .expect("bad data key should encode");
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            data_store.insert_raw_for_test(
                bad_key,
                RawRow::try_new(vec![0xFF, 0x00, 0xAA]).expect("bad row raw construction"),
            );
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB).expect_err("startup rebuild should reject corrupted row bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);

    let after_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        after_snapshot, before_snapshot,
        "failed startup rebuild must restore the prior index snapshot"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed startup rebuild must keep marker persisted for retry"
    );

    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}
