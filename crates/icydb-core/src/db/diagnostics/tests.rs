//! Module: db::diagnostics::tests
//! Responsibility: module-local ownership and contracts for db::diagnostics::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, IntegrityReport, IntegrityStoreSnapshot,
    IntegrityTotals, StorageReport, integrity_report, storage_report,
};
use crate::{
    db::{
        Db, EntityRuntimeHooks,
        codec::ROW_FORMAT_VERSION_CURRENT,
        commit::{
            CommitRowOp, ensure_recovered, init_commit_store_for_tests,
            prepare_row_commit_for_entity,
        },
        data::{DataKey, DataStore, RawDataKey, RawRow, StorageKey},
        index::{IndexId, IndexKey, IndexKeyKind, IndexStore, RawIndexEntry, RawIndexKey},
        registry::StoreRegistry,
        relation::validate_delete_strong_relations_for_source,
        schema::commit_schema_fingerprint_for_entity,
    },
    model::{field::FieldKind, index::IndexModel},
    serialize::serialize,
    testing::test_memory,
    traits::{EntityKind, Path, Storable, StoreKind},
    types::{EntityTag, Ulid},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use serde_cbor::Value as CborValue;
use std::{borrow::Cow, cell::RefCell, collections::BTreeMap};

crate::test_canister! {
    ident = DiagnosticsCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

const STORE_Z_PATH: &str = "diagnostics_tests::z_store";
const STORE_A_PATH: &str = "diagnostics_tests::a_store";
const SINGLE_ENTITY_NAME: &str = "diag_single_entity";
const SINGLE_ENTITY_PATH: &str = "diagnostics_tests::entity::single";
const FIRST_ENTITY_NAME: &str = "diag_first_entity";
const FIRST_ENTITY_PATH: &str = "diagnostics_tests::entity::first";
const SECOND_ENTITY_NAME: &str = "diag_second_entity";
const SECOND_ENTITY_PATH: &str = "diagnostics_tests::entity::second";
const MINMAX_ENTITY_NAME: &str = "diag_minmax_entity";
const MINMAX_ENTITY_PATH: &str = "diagnostics_tests::entity::minmax";
const VALID_ENTITY_NAME: &str = "diag_valid_entity";
const VALID_ENTITY_PATH: &str = "diagnostics_tests::entity::valid";

struct DiagnosticsStoreA;

impl Path for DiagnosticsStoreA {
    const PATH: &'static str = STORE_A_PATH;
}

impl StoreKind for DiagnosticsStoreA {
    type Canister = DiagnosticsCanister;
}

#[derive(
    Clone,
    Debug,
    Default,
    Deserialize,
    FieldProjection,
    PartialEq,
    PersistedRow,
    Serialize,
)]
struct IntegrityIndexedEntity {
    id: Ulid,
    email: String,
}

static INTEGRITY_EMAIL_INDEX_FIELDS: [&str; 1] = ["email"];
static INTEGRITY_EMAIL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
    "email",
    STORE_A_PATH,
    &INTEGRITY_EMAIL_INDEX_FIELDS,
    false,
)];

crate::test_entity_schema! {
    ident = IntegrityIndexedEntity,
    id = Ulid,
    id_field = id,
    entity_name = "DiagnosticsIntegrityIndexedEntity",
    entity_tag = crate::testing::INTEGRITY_INDEXED_ENTITY_TAG,
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("email", FieldKind::Text)],
    indexes = [&INTEGRITY_EMAIL_INDEX_MODELS[0]],
    store = DiagnosticsStoreA,
    canister = DiagnosticsCanister,
}

static DIAGNOSTICS_RUNTIME_HOOKS: &[EntityRuntimeHooks<DiagnosticsCanister>] = &[
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_SINGLE_ENTITY_TAG,
        SINGLE_ENTITY_NAME,
        SINGLE_ENTITY_PATH,
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>,
        prepare_row_commit_for_entity::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_FIRST_ENTITY_TAG,
        FIRST_ENTITY_NAME,
        FIRST_ENTITY_PATH,
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>,
        prepare_row_commit_for_entity::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_SECOND_ENTITY_TAG,
        SECOND_ENTITY_NAME,
        SECOND_ENTITY_PATH,
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>,
        prepare_row_commit_for_entity::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_MINMAX_ENTITY_TAG,
        MINMAX_ENTITY_NAME,
        MINMAX_ENTITY_PATH,
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>,
        prepare_row_commit_for_entity::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_VALID_ENTITY_TAG,
        VALID_ENTITY_NAME,
        VALID_ENTITY_PATH,
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>,
        prepare_row_commit_for_entity::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::for_entity::<IntegrityIndexedEntity>(),
];

thread_local! {
    static STORE_Z_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(153)));
    static STORE_Z_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(154)));
    static STORE_A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(155)));
    static STORE_A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(156)));
    static DIAGNOSTICS_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry
            .register_store(STORE_Z_PATH, &STORE_Z_DATA, &STORE_Z_INDEX)
            .expect("diagnostics test z-store registration should succeed");
        registry
            .register_store(STORE_A_PATH, &STORE_A_DATA, &STORE_A_INDEX)
            .expect("diagnostics test a-store registration should succeed");
        registry
    };
}

static DB: Db<DiagnosticsCanister> =
    Db::new_with_hooks(&DIAGNOSTICS_REGISTRY, DIAGNOSTICS_RUNTIME_HOOKS);
static DB_WITH_HOOKS: Db<DiagnosticsCanister> =
    Db::new_with_hooks(&DIAGNOSTICS_REGISTRY, DIAGNOSTICS_RUNTIME_HOOKS);

fn with_data_store_mut<R>(path: &'static str, f: impl FnOnce(&mut DataStore) -> R) -> R {
    DB.with_store_registry(|registry| {
        registry
            .try_get_store(path)
            .map(|store_handle| store_handle.with_data_mut(f))
    })
    .expect("data store lookup should succeed")
}

fn with_index_store_mut<R>(path: &'static str, f: impl FnOnce(&mut IndexStore) -> R) -> R {
    DB.with_store_registry(|registry| {
        registry
            .try_get_store(path)
            .map(|store_handle| store_handle.with_index_mut(f))
    })
    .expect("index store lookup should succeed")
}

fn reset_stores() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&DB).expect("write-side recovery should succeed");
    DB.with_store_registry(|registry| {
        // Test cleanup only: this clear-all sweep has set semantics, so
        // `StoreRegistry` HashMap iteration order is intentionally irrelevant.
        for (_, store_handle) in registry.iter() {
            store_handle.with_data_mut(DataStore::clear);
            store_handle.with_index_mut(IndexStore::clear);
        }
    });
}

fn diagnostics_entity_tag(entity_name: &str) -> EntityTag {
    match entity_name {
        SINGLE_ENTITY_NAME => crate::testing::DIAGNOSTICS_SINGLE_ENTITY_TAG,
        FIRST_ENTITY_NAME => crate::testing::DIAGNOSTICS_FIRST_ENTITY_TAG,
        SECOND_ENTITY_NAME => crate::testing::DIAGNOSTICS_SECOND_ENTITY_TAG,
        MINMAX_ENTITY_NAME => crate::testing::DIAGNOSTICS_MINMAX_ENTITY_TAG,
        VALID_ENTITY_NAME => crate::testing::DIAGNOSTICS_VALID_ENTITY_TAG,
        "diag_index_entity" | "diag_namespace_entity" => {
            crate::testing::DIAGNOSTICS_UNKNOWN_ENTITY_TAG
        }
        "DiagnosticsIntegrityIndexedEntity" => IntegrityIndexedEntity::ENTITY_TAG,
        "diag_unknown_entity" => crate::testing::DIAGNOSTICS_UNKNOWN_ENTITY_TAG,
        _ => panic!("unknown diagnostics test entity '{entity_name}'"),
    }
}

fn insert_data_row(path: &'static str, entity_name: &str, key: StorageKey, row_len: usize) {
    let entity = diagnostics_entity_tag(entity_name);
    let raw_key = DataKey::raw_from_parts(entity, key)
        .expect("diagnostics test data key should encode from valid parts");
    let row_bytes = vec![0xAB; row_len.max(1)];
    let raw_row = RawRow::try_new(row_bytes).expect("diagnostics test row should encode");

    with_data_store_mut(path, |store| {
        store.insert(raw_key, raw_row);
    });
}

fn insert_corrupted_data_key(path: &'static str) {
    let valid = DataKey::raw_from_parts(
        diagnostics_entity_tag(VALID_ENTITY_NAME),
        StorageKey::Int(1),
    )
    .expect("valid data key should encode");

    let mut corrupted_bytes = valid.as_bytes().to_vec();
    corrupted_bytes[DataKey::ENTITY_TAG_SIZE_USIZE] = 0xFF;
    let corrupted_key = <RawDataKey as Storable>::from_bytes(Cow::Owned(corrupted_bytes));
    let raw_row = RawRow::try_new(vec![0xCD]).expect("diagnostics test row should encode");

    with_data_store_mut(path, |store| {
        store.insert(corrupted_key, raw_row);
    });
}

fn index_id(entity_name: &str, field: &str) -> IndexId {
    let ordinal = match field {
        "email" => 0,
        other => panic!("diagnostics test index field missing ordinal mapping: {other}"),
    };

    IndexId::new(diagnostics_entity_tag(entity_name), ordinal)
}

fn index_key(kind: IndexKeyKind, entity_name: &str, field: &str) -> RawIndexKey {
    let id = index_id(entity_name, field);
    IndexKey::empty_with_kind(&id, kind).to_raw()
}

fn insert_index_entry(path: &'static str, key: RawIndexKey, entry: RawIndexEntry) {
    with_index_store_mut(path, |store| {
        store.insert(key, entry);
    });
}

fn diagnostics_report(name_to_path: &[(&'static str, &'static str)]) -> StorageReport {
    storage_report(&DB, name_to_path).expect("diagnostics snapshot should succeed")
}

fn diagnostics_integrity_report() -> IntegrityReport {
    integrity_report(&DB_WITH_HOOKS).expect("diagnostics integrity scan should succeed")
}

fn insert_integrity_entity_row(entity: &IntegrityIndexedEntity) {
    let raw_key = DataKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let raw_row = RawRow::from_entity(entity).expect("integrity test row should encode");

    with_data_store_mut(STORE_A_PATH, |store| {
        store.insert(raw_key, raw_row);
    });
}

fn insert_integrity_entity_row_with_format_version(entity: &IntegrityIndexedEntity, version: u8) {
    let raw_key = DataKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let payload = serialize(entity).expect("integrity test entity payload should encode");
    let encoded =
        serialize(&(version, payload)).expect("integrity test row envelope should encode");
    let raw_row = RawRow::try_new(encoded).expect("integrity test row envelope should fit bounds");

    with_data_store_mut(STORE_A_PATH, |store| {
        store.insert(raw_key, raw_row);
    });
}

fn insert_integrity_expected_indexes(entity: &IntegrityIndexedEntity) {
    let raw_key = DataKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let raw_row = RawRow::from_entity(entity).expect("integrity test row should encode");
    let row_op = CommitRowOp::new(
        IntegrityIndexedEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(raw_row.as_bytes().to_vec()),
        commit_schema_fingerprint_for_entity::<IntegrityIndexedEntity>(),
    );
    let prepared = DB_WITH_HOOKS
        .prepare_row_commit_op(&row_op)
        .expect("integrity test row op should prepare");

    for index_op in prepared.index_ops {
        let Some(raw_entry) = index_op.value else {
            continue;
        };
        index_op.store.with_borrow_mut(|store| {
            store.insert(index_op.key.clone(), raw_entry);
        });
    }
}

fn integrity_store_snapshot<'a>(
    report: &'a IntegrityReport,
    path: &str,
) -> &'a IntegrityStoreSnapshot {
    report
        .stores()
        .iter()
        .find(|snapshot| snapshot.path() == path)
        .expect("integrity snapshot should contain target store path")
}

fn data_paths(report: &StorageReport) -> Vec<&str> {
    report
        .storage_data()
        .iter()
        .map(DataStoreSnapshot::path)
        .collect()
}

fn index_paths(report: &StorageReport) -> Vec<&str> {
    report
        .storage_index()
        .iter()
        .map(IndexStoreSnapshot::path)
        .collect()
}

fn entity_store_paths(report: &StorageReport) -> Vec<(&str, &str)> {
    report
        .entity_storage()
        .iter()
        .map(|snapshot| (snapshot.store(), snapshot.path()))
        .collect()
}

fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
    let bytes = serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
    serde_cbor::from_slice::<CborValue>(&bytes)
        .expect("test fixtures must deserialize into CBOR value trees")
}

fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
    match value {
        CborValue::Map(map) => map,
        other => panic!("expected CBOR map, got {other:?}"),
    }
}

fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
    map.get(&CborValue::Text(key.to_string()))
}

#[test]
fn storage_report_empty_store_snapshot() {
    reset_stores();

    let report = diagnostics_report(&[]);

    assert_eq!(report.corrupted_keys(), 0);
    assert_eq!(report.corrupted_entries(), 0);
    assert!(report.entity_storage().is_empty());

    assert_eq!(data_paths(&report), vec![STORE_A_PATH, STORE_Z_PATH]);
    assert_eq!(index_paths(&report), vec![STORE_A_PATH, STORE_Z_PATH]);
    assert!(
        report
            .storage_data()
            .iter()
            .all(|snapshot| snapshot.entries() == 0)
    );
    assert!(
        report
            .storage_index()
            .iter()
            .all(|snapshot| snapshot.entries() == 0)
    );
}

#[test]
fn storage_report_single_entity_multiple_rows() {
    reset_stores();

    insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(3), 3);
    insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(1), 1);
    insert_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, StorageKey::Int(2), 2);

    let report = diagnostics_report(&[(SINGLE_ENTITY_NAME, SINGLE_ENTITY_PATH)]);
    let entity_snapshot = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == SINGLE_ENTITY_PATH)
        .expect("single-entity snapshot should exist");

    assert_eq!(entity_snapshot.entries(), 3);
}

#[test]
fn storage_report_multiple_entities_in_same_store() {
    reset_stores();

    insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(10), 1);
    insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(11), 1);
    insert_data_row(STORE_A_PATH, SECOND_ENTITY_NAME, StorageKey::Int(20), 1);

    let report = diagnostics_report(&[
        (FIRST_ENTITY_NAME, FIRST_ENTITY_PATH),
        (SECOND_ENTITY_NAME, SECOND_ENTITY_PATH),
    ]);

    let first = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == FIRST_ENTITY_PATH)
        .expect("first-entity snapshot should exist");
    let second = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == SECOND_ENTITY_PATH)
        .expect("second-entity snapshot should exist");

    assert_eq!(first.entries(), 2);
    assert_eq!(second.entries(), 1);
}

#[test]
fn storage_report_entity_snapshots_are_sorted_by_store_then_path() {
    reset_stores();

    insert_data_row(STORE_Z_PATH, FIRST_ENTITY_NAME, StorageKey::Int(1), 1);
    insert_data_row(STORE_A_PATH, SECOND_ENTITY_NAME, StorageKey::Int(2), 1);
    insert_data_row(STORE_A_PATH, FIRST_ENTITY_NAME, StorageKey::Int(3), 1);

    let report = diagnostics_report(&[
        (FIRST_ENTITY_NAME, "diagnostics_tests::entity::z_first"),
        (SECOND_ENTITY_NAME, "diagnostics_tests::entity::a_second"),
    ]);

    assert_eq!(
        entity_store_paths(&report),
        vec![
            (STORE_A_PATH, "diagnostics_tests::entity::a_second"),
            (STORE_A_PATH, "diagnostics_tests::entity::z_first"),
            (STORE_Z_PATH, "diagnostics_tests::entity::z_first"),
        ]
    );
}

#[test]
fn storage_report_min_max_key_correctness() {
    reset_stores();

    insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(9), 1);
    insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(-5), 1);
    insert_data_row(STORE_A_PATH, MINMAX_ENTITY_NAME, StorageKey::Int(3), 1);

    let report = diagnostics_report(&[(MINMAX_ENTITY_NAME, MINMAX_ENTITY_PATH)]);
    let entity_snapshot = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == MINMAX_ENTITY_PATH)
        .expect("min/max snapshot should exist");

    assert_eq!(
        entity_snapshot.min_key(),
        Some(&StorageKey::Int(-5).as_value())
    );
    assert_eq!(
        entity_snapshot.max_key(),
        Some(&StorageKey::Int(9).as_value())
    );
}

#[test]
fn storage_report_corrupted_key_detection() {
    reset_stores();

    insert_data_row(STORE_A_PATH, VALID_ENTITY_NAME, StorageKey::Int(7), 1);
    insert_corrupted_data_key(STORE_A_PATH);

    let report = diagnostics_report(&[(VALID_ENTITY_NAME, VALID_ENTITY_PATH)]);

    assert_eq!(report.corrupted_keys(), 1);
    let entity_snapshot = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == VALID_ENTITY_PATH)
        .expect("valid-entity snapshot should exist");
    assert_eq!(entity_snapshot.entries(), 1);
}

#[test]
fn storage_report_corrupted_index_value_detection() {
    reset_stores();

    let key = index_key(IndexKeyKind::User, "diag_index_entity", "email");
    let corrupted_entry = <RawIndexEntry as Storable>::from_bytes(Cow::Owned(vec![0, 0, 0, 0]));
    insert_index_entry(STORE_A_PATH, key, corrupted_entry);

    let report = diagnostics_report(&[]);
    let index_snapshot = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_A_PATH)
        .expect("index snapshot should exist");

    assert_eq!(report.corrupted_entries(), 1);
    assert_eq!(index_snapshot.entries(), 1);
    assert_eq!(index_snapshot.user_entries(), 1);
    assert_eq!(index_snapshot.system_entries(), 0);
}

#[test]
fn storage_report_system_vs_user_namespace_split() {
    reset_stores();

    let user_key = index_key(IndexKeyKind::User, "diag_namespace_entity", "email");
    let system_key = index_key(IndexKeyKind::System, "diag_namespace_entity", "email");
    let user_entry =
        RawIndexEntry::try_from_keys([StorageKey::Int(1)]).expect("user entry should encode");
    let system_entry =
        RawIndexEntry::try_from_keys([StorageKey::Int(2)]).expect("system entry should encode");
    insert_index_entry(STORE_A_PATH, user_key, user_entry);
    insert_index_entry(STORE_A_PATH, system_key, system_entry);

    let report = diagnostics_report(&[]);
    let index_snapshot = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_A_PATH)
        .expect("index snapshot should exist");

    assert_eq!(report.corrupted_entries(), 0);
    assert_eq!(index_snapshot.entries(), 2);
    assert_eq!(index_snapshot.user_entries(), 1);
    assert_eq!(index_snapshot.system_entries(), 1);
}

#[test]
fn integrity_report_detects_missing_forward_index_entries() {
    reset_stores();

    let entity = IntegrityIndexedEntity {
        id: Ulid::from_u128(70_001),
        email: "missing@index.local".to_string(),
    };
    insert_integrity_entity_row(&entity);

    let report = diagnostics_integrity_report();
    let store = integrity_store_snapshot(&report, STORE_A_PATH);

    assert_eq!(store.data_rows_scanned(), 1);
    assert_eq!(store.missing_index_entries(), 1);
    assert_eq!(store.divergent_index_entries(), 0);
    assert_eq!(store.orphan_index_references(), 0);
    assert_eq!(report.totals().missing_index_entries(), 1);
}

#[test]
fn integrity_report_detects_orphan_index_references() {
    reset_stores();

    let entity = IntegrityIndexedEntity {
        id: Ulid::from_u128(70_002),
        email: "orphan@index.local".to_string(),
    };
    insert_integrity_expected_indexes(&entity);

    let report = diagnostics_integrity_report();
    let store = integrity_store_snapshot(&report, STORE_A_PATH);

    assert_eq!(store.data_rows_scanned(), 0);
    assert_eq!(store.index_entries_scanned(), 1);
    assert_eq!(store.orphan_index_references(), 1);
    assert_eq!(report.totals().orphan_index_references(), 1);
}

#[test]
fn integrity_report_classifies_unsupported_entity_rows_as_misuse() {
    reset_stores();

    insert_data_row(STORE_A_PATH, "diag_unknown_entity", StorageKey::Int(9), 8);

    let report = diagnostics_integrity_report();
    let store = integrity_store_snapshot(&report, STORE_A_PATH);

    assert_eq!(store.misuse_findings(), 1);
    assert_eq!(store.compatibility_findings(), 0);
    assert_eq!(store.corrupted_data_rows(), 0);
    assert_eq!(report.totals().misuse_findings(), 1);
}

#[test]
fn integrity_report_classifies_incompatible_row_formats() {
    reset_stores();

    let entity = IntegrityIndexedEntity {
        id: Ulid::from_u128(70_003),
        email: "future@index.local".to_string(),
    };
    insert_integrity_entity_row_with_format_version(
        &entity,
        ROW_FORMAT_VERSION_CURRENT.saturating_add(1),
    );

    let report = diagnostics_integrity_report();
    let store = integrity_store_snapshot(&report, STORE_A_PATH);

    assert_eq!(store.compatibility_findings(), 1);
    assert_eq!(store.misuse_findings(), 0);
    assert_eq!(store.corrupted_data_rows(), 0);
    assert_eq!(report.totals().compatibility_findings(), 1);
}

#[test]
fn storage_report_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&StorageReport::new(
        vec![DataStoreSnapshot::new("store_a".to_string(), 2, 64)],
        vec![IndexStoreSnapshot::new("store_a".to_string(), 3, 2, 1, 96)],
        vec![EntitySnapshot::new(
            "store_a".to_string(),
            "entity_a".to_string(),
            2,
            64,
            Some(StorageKey::Int(1).as_value()),
            Some(StorageKey::Int(9).as_value()),
        )],
        5,
        6,
    ));
    let root = expect_cbor_map(&encoded);

    assert!(
        map_field(root, "storage_data").is_some(),
        "StorageReport must keep `storage_data` as serialized field key",
    );
    assert!(
        map_field(root, "storage_index").is_some(),
        "StorageReport must keep `storage_index` as serialized field key",
    );
    assert!(
        map_field(root, "entity_storage").is_some(),
        "StorageReport must keep `entity_storage` as serialized field key",
    );
    assert!(
        map_field(root, "corrupted_keys").is_some(),
        "StorageReport must keep `corrupted_keys` as serialized field key",
    );
    assert!(
        map_field(root, "corrupted_entries").is_some(),
        "StorageReport must keep `corrupted_entries` as serialized field key",
    );
}

#[test]
fn data_store_snapshot_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&DataStoreSnapshot::new("store_a".to_string(), 2, 64));
    let root = expect_cbor_map(&encoded);

    assert!(
        map_field(root, "path").is_some(),
        "DataStoreSnapshot must keep `path` as serialized field key",
    );
    assert!(
        map_field(root, "entries").is_some(),
        "DataStoreSnapshot must keep `entries` as serialized field key",
    );
    assert!(
        map_field(root, "memory_bytes").is_some(),
        "DataStoreSnapshot must keep `memory_bytes` as serialized field key",
    );
}

#[test]
fn index_store_snapshot_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&IndexStoreSnapshot::new("store_a".to_string(), 3, 2, 1, 96));
    let root = expect_cbor_map(&encoded);

    assert!(
        map_field(root, "path").is_some(),
        "IndexStoreSnapshot must keep `path` as serialized field key",
    );
    assert!(
        map_field(root, "entries").is_some(),
        "IndexStoreSnapshot must keep `entries` as serialized field key",
    );
    assert!(
        map_field(root, "user_entries").is_some(),
        "IndexStoreSnapshot must keep `user_entries` as serialized field key",
    );
    assert!(
        map_field(root, "system_entries").is_some(),
        "IndexStoreSnapshot must keep `system_entries` as serialized field key",
    );
    assert!(
        map_field(root, "memory_bytes").is_some(),
        "IndexStoreSnapshot must keep `memory_bytes` as serialized field key",
    );
}

#[test]
fn entity_snapshot_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&EntitySnapshot::new(
        "store_a".to_string(),
        "entity_a".to_string(),
        2,
        64,
        Some(StorageKey::Int(1).as_value()),
        Some(StorageKey::Int(9).as_value()),
    ));
    let root = expect_cbor_map(&encoded);

    assert!(
        map_field(root, "store").is_some(),
        "EntitySnapshot must keep `store` as serialized field key",
    );
    assert!(
        map_field(root, "path").is_some(),
        "EntitySnapshot must keep `path` as serialized field key",
    );
    assert!(
        map_field(root, "entries").is_some(),
        "EntitySnapshot must keep `entries` as serialized field key",
    );
    assert!(
        map_field(root, "memory_bytes").is_some(),
        "EntitySnapshot must keep `memory_bytes` as serialized field key",
    );
    assert!(
        map_field(root, "min_key").is_some(),
        "EntitySnapshot must keep `min_key` as serialized field key",
    );
    assert!(
        map_field(root, "max_key").is_some(),
        "EntitySnapshot must keep `max_key` as serialized field key",
    );
}

#[test]
fn integrity_totals_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&IntegrityTotals {
        data_rows_scanned: 1,
        index_entries_scanned: 2,
        corrupted_data_keys: 3,
        corrupted_data_rows: 4,
        corrupted_index_keys: 5,
        corrupted_index_entries: 6,
        missing_index_entries: 7,
        divergent_index_entries: 8,
        orphan_index_references: 9,
        compatibility_findings: 10,
        misuse_findings: 11,
    });
    let root = expect_cbor_map(&encoded);

    assert!(map_field(root, "data_rows_scanned").is_some());
    assert!(map_field(root, "index_entries_scanned").is_some());
    assert!(map_field(root, "corrupted_data_keys").is_some());
    assert!(map_field(root, "corrupted_data_rows").is_some());
    assert!(map_field(root, "corrupted_index_keys").is_some());
    assert!(map_field(root, "corrupted_index_entries").is_some());
    assert!(map_field(root, "missing_index_entries").is_some());
    assert!(map_field(root, "divergent_index_entries").is_some());
    assert!(map_field(root, "orphan_index_references").is_some());
    assert!(map_field(root, "compatibility_findings").is_some());
    assert!(map_field(root, "misuse_findings").is_some());
}

#[test]
fn integrity_store_snapshot_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&IntegrityStoreSnapshot {
        path: "store_a".to_string(),
        data_rows_scanned: 1,
        index_entries_scanned: 2,
        corrupted_data_keys: 3,
        corrupted_data_rows: 4,
        corrupted_index_keys: 5,
        corrupted_index_entries: 6,
        missing_index_entries: 7,
        divergent_index_entries: 8,
        orphan_index_references: 9,
        compatibility_findings: 10,
        misuse_findings: 11,
    });
    let root = expect_cbor_map(&encoded);

    assert!(map_field(root, "path").is_some());
    assert!(map_field(root, "data_rows_scanned").is_some());
    assert!(map_field(root, "index_entries_scanned").is_some());
    assert!(map_field(root, "corrupted_data_keys").is_some());
    assert!(map_field(root, "corrupted_data_rows").is_some());
    assert!(map_field(root, "corrupted_index_keys").is_some());
    assert!(map_field(root, "corrupted_index_entries").is_some());
    assert!(map_field(root, "missing_index_entries").is_some());
    assert!(map_field(root, "divergent_index_entries").is_some());
    assert!(map_field(root, "orphan_index_references").is_some());
    assert!(map_field(root, "compatibility_findings").is_some());
    assert!(map_field(root, "misuse_findings").is_some());
}

#[test]
fn integrity_report_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&IntegrityReport::new(
        vec![IntegrityStoreSnapshot::new("store_a".to_string())],
        IntegrityTotals::default(),
    ));
    let root = expect_cbor_map(&encoded);

    assert!(
        map_field(root, "stores").is_some(),
        "IntegrityReport must keep `stores` as serialized field key",
    );
    assert!(
        map_field(root, "totals").is_some(),
        "IntegrityReport must keep `totals` as serialized field key",
    );
}
