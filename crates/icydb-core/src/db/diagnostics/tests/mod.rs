//! Module: db::diagnostics::tests
//! Covers diagnostic node and counter behavior.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod execution_trace;

use super::{
    DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, IntegrityReport, IntegrityStoreSnapshot,
    IntegrityTotals, SchemaStoreSnapshot, StorageReport, StoreSnapshotStorageMode,
    integrity_report, storage_report, storage_report_default,
};
use crate::{
    db::{
        Db, EntityRuntimeHooks,
        codec::{
            ROW_FORMAT_VERSION_CURRENT, decode_row_payload_bytes,
            serialize_row_payload_with_version,
        },
        commit::{
            CommitRowOp, ensure_recovered, init_commit_store_for_tests,
            prepare_row_commit_for_entity_with_structural_readers,
        },
        data::{CanonicalRow, DataStore, DecodedDataStoreKey, RawDataStoreKey, RawRow},
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore,
            RawIndexStoreKey,
        },
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreAllocationIdentityCapability,
            StoreCommitParticipation, StoreDurability, StoreRecoveryCapability, StoreRegistry,
            StoreRuntimeStorageCapabilities, StoreSchemaMetadataCapability,
        },
        relation::validate_delete_strong_relations_for_source,
        schema::{
            AcceptedSchemaSnapshot, SchemaStore, accepted_commit_schema_fingerprint,
            compiled_schema_proposal_for_model,
        },
    },
    model::{field::FieldKind, index::IndexModel},
    testing::test_memory,
    traits::{EntityKind, EntitySchema, Path, Storable, StoreKind},
    types::{EntityTag, Ulid},
};
use candid::types::{CandidType, Label, Type, TypeInner};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{borrow::Cow, cell::RefCell};

crate::test_canister! {
    ident = DiagnosticsCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

const STORE_Z_PATH: &str = "diagnostics_tests::z_store";
const STORE_A_PATH: &str = "diagnostics_tests::a_store";
const STORE_HEAP_PATH: &str = "diagnostics_tests::heap_store";
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct IntegrityIndexedEntity {
    id: Ulid,
    email: String,
}

static INTEGRITY_EMAIL_INDEX_FIELDS: [&str; 1] = ["email"];
static INTEGRITY_EMAIL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "email",
    STORE_A_PATH,
    &INTEGRITY_EMAIL_INDEX_FIELDS,
    false,
)];

crate::test_entity! {
    ident = IntegrityIndexedEntity,
    entity_name = "DiagnosticsIntegrityIndexedEntity",
    tag = crate::testing::INTEGRITY_INDEXED_ENTITY_TAG,
    store = DiagnosticsStoreA,
    canister = DiagnosticsCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&INTEGRITY_EMAIL_INDEX_MODELS[0]],
}

static DIAGNOSTICS_RUNTIME_HOOKS: &[EntityRuntimeHooks<DiagnosticsCanister>] = &[
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_SINGLE_ENTITY_TAG,
        <IntegrityIndexedEntity as crate::traits::EntitySchema>::MODEL,
        SINGLE_ENTITY_PATH,
        STORE_A_PATH,
        prepare_row_commit_for_entity_with_structural_readers::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_FIRST_ENTITY_TAG,
        <IntegrityIndexedEntity as crate::traits::EntitySchema>::MODEL,
        FIRST_ENTITY_PATH,
        STORE_A_PATH,
        prepare_row_commit_for_entity_with_structural_readers::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_SECOND_ENTITY_TAG,
        <IntegrityIndexedEntity as crate::traits::EntitySchema>::MODEL,
        SECOND_ENTITY_PATH,
        STORE_A_PATH,
        prepare_row_commit_for_entity_with_structural_readers::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_MINMAX_ENTITY_TAG,
        <IntegrityIndexedEntity as crate::traits::EntitySchema>::MODEL,
        MINMAX_ENTITY_PATH,
        STORE_A_PATH,
        prepare_row_commit_for_entity_with_structural_readers::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        crate::testing::DIAGNOSTICS_VALID_ENTITY_TAG,
        <IntegrityIndexedEntity as crate::traits::EntitySchema>::MODEL,
        VALID_ENTITY_PATH,
        STORE_A_PATH,
        prepare_row_commit_for_entity_with_structural_readers::<IntegrityIndexedEntity>,
        validate_delete_strong_relations_for_source::<IntegrityIndexedEntity>,
    ),
    EntityRuntimeHooks::for_entity::<IntegrityIndexedEntity>(),
];

thread_local! {
    static STORE_Z_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(153)));
    static STORE_Z_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(154)));
    static STORE_Z_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init(test_memory(157)));
    static STORE_A_DATA: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(155)));
    static STORE_A_INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(156)));
    static STORE_A_SCHEMA: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init(test_memory(158)));
    static STORE_HEAP_DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static STORE_HEAP_INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static STORE_HEAP_SCHEMA: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static DIAGNOSTICS_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry
            .register_store(
                STORE_Z_PATH,
                &STORE_Z_DATA,
                &STORE_Z_INDEX,
                &STORE_Z_SCHEMA,
                StoreAllocationIdentities::new(
                    StoreAllocationIdentity::new(153, "icydb.test.store_z.data.v1"),
                    StoreAllocationIdentity::new(154, "icydb.test.store_z.index.v1"),
                    StoreAllocationIdentity::new(157, "icydb.test.store_z.schema.v1"),
                ),
                StoreRuntimeStorageCapabilities::stable(),
            )
            .expect("diagnostics test z-store registration should succeed");
        registry
            .register_store(
                STORE_HEAP_PATH,
                &STORE_HEAP_DATA,
                &STORE_HEAP_INDEX,
                &STORE_HEAP_SCHEMA,
                crate::db::StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            )
            .expect("diagnostics test heap-store registration should succeed");
        registry
            .register_store(
                STORE_A_PATH,
                &STORE_A_DATA,
                &STORE_A_INDEX,
                &STORE_A_SCHEMA,
                StoreAllocationIdentities::new(
                    StoreAllocationIdentity::new(155, "icydb.test.store_a.data.v1"),
                    StoreAllocationIdentity::new(156, "icydb.test.store_a.index.v1"),
                    StoreAllocationIdentity::new(158, "icydb.test.store_a.schema.v1"),
                ),
                StoreRuntimeStorageCapabilities::stable(),
            )
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

fn insert_data_row(
    path: &'static str,
    entity_name: &str,
    key: PrimaryKeyComponent,
    row_len: usize,
) {
    let entity = diagnostics_entity_tag(entity_name);
    let raw_key = DecodedDataStoreKey::new(entity, &key.into())
        .to_raw()
        .expect("diagnostics test data key should encode from valid entity and primary-key inputs");
    insert_raw_data_row(path, raw_key, row_len);
}

fn insert_composite_data_row(
    path: &'static str,
    entity_name: &str,
    key: &CompositePrimaryKeyValue,
    row_len: usize,
) {
    let entity = diagnostics_entity_tag(entity_name);
    let primary_key = PrimaryKeyValue::Composite(*key);
    let raw_key = DecodedDataStoreKey::new_primary_key_value(entity, &primary_key)
        .to_raw()
        .expect("diagnostics test composite data key should encode from valid entity and primary-key inputs");
    insert_raw_data_row(path, raw_key, row_len);
}

fn insert_raw_data_row(path: &'static str, raw_key: RawDataStoreKey, row_len: usize) {
    let row_bytes = vec![0xAB; row_len.max(1)];
    let raw_row = RawRow::try_new(row_bytes).expect("diagnostics test row should encode");

    with_data_store_mut(path, |store| {
        store.insert_raw_for_test(raw_key, raw_row);
    });
}

fn insert_corrupted_data_key(path: &'static str) {
    let valid = DecodedDataStoreKey::new(
        diagnostics_entity_tag(VALID_ENTITY_NAME),
        &PrimaryKeyComponent::Int64(1).into(),
    )
    .to_raw()
    .expect("valid data key should encode");

    let mut corrupted_bytes = valid.as_bytes().to_vec();
    corrupted_bytes[RawDataStoreKey::ENTITY_TAG_SIZE_USIZE] = 0xFF;
    let corrupted_key = <RawDataStoreKey as Storable>::from_bytes(Cow::Owned(corrupted_bytes));
    let raw_row = RawRow::try_new(vec![0xCD]).expect("diagnostics test row should encode");

    with_data_store_mut(path, |store| {
        store.insert_raw_for_test(corrupted_key, raw_row);
    });
}

fn index_id(entity_name: &str, field: &str) -> IndexId {
    let ordinal = match field {
        "email" => 0,
        other => panic!("diagnostics test index field missing ordinal mapping: {other}"),
    };

    IndexId::new(diagnostics_entity_tag(entity_name), ordinal)
}

fn index_key(kind: IndexKeyKind, entity_name: &str, field: &str) -> RawIndexStoreKey {
    let id = index_id(entity_name, field);
    let components: [Vec<u8>; 0] = [];
    IndexKey::new_from_components_with_primary_key_value(
        &id,
        kind,
        &components,
        &PrimaryKeyValue::from(PrimaryKeyComponent::Int64(1)),
    )
    .to_raw()
}

fn insert_index_entry(path: &'static str, key: RawIndexStoreKey, entry: IndexEntryValue) {
    with_index_store_mut(path, |store| {
        store.insert(key, entry);
    });
}

fn diagnostics_report(name_to_path: &[(&'static str, &'static str)]) -> StorageReport {
    storage_report(&DB, name_to_path).expect("diagnostics snapshot should succeed")
}

fn diagnostics_default_report() -> StorageReport {
    storage_report_default(&DB).expect("default diagnostics snapshot should succeed")
}

fn diagnostics_integrity_report() -> IntegrityReport {
    integrity_report(&DB_WITH_HOOKS).expect("diagnostics integrity scan should succeed")
}

fn insert_integrity_entity_row(entity: &IntegrityIndexedEntity) {
    let raw_key = DecodedDataStoreKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let raw_row = CanonicalRow::from_generated_entity_for_test(entity)
        .expect("integrity test row should encode");

    with_data_store_mut(STORE_A_PATH, |store| {
        store.insert(raw_key, raw_row);
    });
}

fn insert_integrity_entity_row_with_format_version(entity: &IntegrityIndexedEntity, version: u8) {
    let raw_key = DecodedDataStoreKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let row = CanonicalRow::from_generated_entity_for_test(entity)
        .expect("integrity test row should encode")
        .into_raw_row();
    let payload = decode_row_payload_bytes(row.as_bytes())
        .expect("integrity test row payload should decode")
        .into_owned();
    let encoded = serialize_row_payload_with_version(payload, version)
        .expect("integrity test row envelope should encode");
    let raw_row = RawRow::try_new(encoded).expect("integrity test row envelope should fit bounds");

    with_data_store_mut(STORE_A_PATH, |store| {
        store.insert_raw_for_test(raw_key, raw_row);
    });
}

fn insert_integrity_expected_indexes(entity: &IntegrityIndexedEntity) {
    let raw_key = DecodedDataStoreKey::try_new::<IntegrityIndexedEntity>(entity.id)
        .expect("integrity test data key should build")
        .to_raw()
        .expect("integrity test data key should encode");
    let raw_row = CanonicalRow::from_generated_entity_for_test(entity)
        .expect("integrity test row should encode")
        .into_raw_row();
    let proposal = compiled_schema_proposal_for_model(IntegrityIndexedEntity::MODEL);
    let accepted = AcceptedSchemaSnapshot::try_new(proposal.initial_persisted_schema_snapshot())
        .expect("integrity test schema snapshot should be accepted");
    let schema_fingerprint = accepted_commit_schema_fingerprint(&accepted)
        .expect("integrity test schema fingerprint should derive");
    let row_op = CommitRowOp::new(
        IntegrityIndexedEntity::PATH,
        raw_key,
        None,
        Some(raw_row.as_bytes().to_vec()),
        schema_fingerprint,
    );
    let prepared = DB_WITH_HOOKS
        .prepare_row_commit_op(&row_op)
        .expect("integrity test row op should prepare");

    for index_op in prepared.index_ops {
        let Some(raw_entry) = index_op.value else {
            continue;
        };
        index_op.index_store.with_borrow_mut(|store| {
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

fn schema_paths(report: &StorageReport) -> Vec<&str> {
    report
        .schema_storage()
        .iter()
        .map(SchemaStoreSnapshot::path)
        .collect()
}

fn entity_store_paths(report: &StorageReport) -> Vec<(&str, &str)> {
    report
        .entity_storage()
        .iter()
        .map(|snapshot| (snapshot.store(), snapshot.path()))
        .collect()
}

fn data_snapshot_rows(report: &StorageReport) -> Vec<(&str, u64, u64)> {
    report
        .storage_data()
        .iter()
        .map(|snapshot| (snapshot.path(), snapshot.entries(), snapshot.memory_bytes()))
        .collect()
}

fn index_snapshot_rows(report: &StorageReport) -> Vec<(&str, u64, u64, u64, u64, IndexState)> {
    report
        .storage_index()
        .iter()
        .map(|snapshot| {
            (
                snapshot.path(),
                snapshot.entries(),
                snapshot.user_entries(),
                snapshot.system_entries(),
                snapshot.memory_bytes(),
                snapshot.state(),
            )
        })
        .collect()
}

fn schema_snapshot<'a>(report: &'a StorageReport, path: &str) -> &'a SchemaStoreSnapshot {
    report
        .schema_storage()
        .iter()
        .find(|snapshot| snapshot.path() == path)
        .expect("schema snapshot should contain target store path")
}

fn assert_heap_store_snapshot_is_volatile(report: &StorageReport) {
    let data_heap = report
        .storage_data()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_HEAP_PATH)
        .expect("data snapshot should contain heap store");
    let index_heap = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_HEAP_PATH)
        .expect("index snapshot should contain heap store");
    let schema_heap = schema_snapshot(report, STORE_HEAP_PATH);
    assert_eq!(data_heap.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(
        data_heap.allocation(),
        StoreAllocationIdentityCapability::Absent
    );
    assert_eq!(data_heap.durability(), StoreDurability::Volatile);
    assert_eq!(data_heap.commit(), StoreCommitParticipation::LiveOnly);
    assert_eq!(data_heap.recovery(), StoreRecoveryCapability::None);
    assert_eq!(
        data_heap.schema_metadata(),
        StoreSchemaMetadataCapability::LiveRebuiltMetadata
    );
    assert_eq!(data_heap.memory_id(), None);
    assert_eq!(data_heap.stable_key(), None);
    assert_eq!(data_heap.schema_version(), None);
    assert_eq!(data_heap.schema_fingerprint(), None);
    assert_eq!(index_heap.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(
        index_heap.allocation(),
        StoreAllocationIdentityCapability::Absent
    );
    assert_eq!(index_heap.durability(), StoreDurability::Volatile);
    assert_eq!(index_heap.commit(), StoreCommitParticipation::LiveOnly);
    assert_eq!(index_heap.recovery(), StoreRecoveryCapability::None);
    assert_eq!(
        index_heap.schema_metadata(),
        StoreSchemaMetadataCapability::LiveRebuiltMetadata
    );
    assert_eq!(index_heap.memory_id(), None);
    assert_eq!(index_heap.stable_key(), None);
    assert_eq!(index_heap.schema_version(), None);
    assert_eq!(index_heap.schema_fingerprint(), None);
    assert_eq!(schema_heap.storage(), StoreSnapshotStorageMode::Heap);
    assert_eq!(
        schema_heap.allocation(),
        StoreAllocationIdentityCapability::Absent
    );
    assert_eq!(schema_heap.durability(), StoreDurability::Volatile);
    assert_eq!(schema_heap.commit(), StoreCommitParticipation::LiveOnly);
    assert_eq!(schema_heap.recovery(), StoreRecoveryCapability::None);
    assert_eq!(
        schema_heap.schema_metadata(),
        StoreSchemaMetadataCapability::LiveRebuiltMetadata
    );
    assert_eq!(schema_heap.memory_id(), None);
    assert_eq!(schema_heap.stable_key(), None);
    assert_eq!(schema_heap.schema_version(), None);
    assert_eq!(schema_heap.schema_fingerprint(), None);
    assert_eq!(schema_heap.entity_count(), 0);
}

fn entity_snapshot_rows(report: &StorageReport) -> Vec<(&str, &str, u64, u64)> {
    report
        .entity_storage()
        .iter()
        .map(|snapshot| {
            (
                snapshot.store(),
                snapshot.path(),
                snapshot.entries(),
                snapshot.memory_bytes(),
            )
        })
        .collect()
}

fn expect_record_fields(ty: Type) -> Vec<String> {
    match ty.as_ref() {
        TypeInner::Record(fields) => fields
            .iter()
            .map(|field| match field.id.as_ref() {
                Label::Named(name) => name.clone(),
                other => panic!("expected named record field, got {other:?}"),
            })
            .collect(),
        other => panic!("expected candid record, got {other:?}"),
    }
}

fn assert_stable_data_capabilities(snapshot: &DataStoreSnapshot) {
    assert_eq!(snapshot.storage(), StoreSnapshotStorageMode::Stable);
    assert_eq!(
        snapshot.allocation(),
        StoreAllocationIdentityCapability::Present
    );
    assert_eq!(snapshot.durability(), StoreDurability::Durable);
    assert_eq!(snapshot.commit(), StoreCommitParticipation::Durable);
    assert_eq!(
        snapshot.recovery(),
        StoreRecoveryCapability::StableCommitReplay
    );
    assert_eq!(
        snapshot.schema_metadata(),
        StoreSchemaMetadataCapability::DurableAcceptedHistory
    );
}

fn assert_stable_index_capabilities(snapshot: &IndexStoreSnapshot) {
    assert_eq!(snapshot.storage(), StoreSnapshotStorageMode::Stable);
    assert_eq!(
        snapshot.allocation(),
        StoreAllocationIdentityCapability::Present
    );
    assert_eq!(snapshot.durability(), StoreDurability::Durable);
    assert_eq!(snapshot.commit(), StoreCommitParticipation::Durable);
    assert_eq!(
        snapshot.recovery(),
        StoreRecoveryCapability::StableCommitReplay
    );
    assert_eq!(
        snapshot.schema_metadata(),
        StoreSchemaMetadataCapability::DurableAcceptedHistory
    );
}

fn assert_stable_schema_capabilities(snapshot: &SchemaStoreSnapshot) {
    assert_eq!(snapshot.storage(), StoreSnapshotStorageMode::Stable);
    assert_eq!(
        snapshot.allocation(),
        StoreAllocationIdentityCapability::Present
    );
    assert_eq!(snapshot.durability(), StoreDurability::Durable);
    assert_eq!(snapshot.commit(), StoreCommitParticipation::Durable);
    assert_eq!(
        snapshot.recovery(),
        StoreRecoveryCapability::StableCommitReplay
    );
    assert_eq!(
        snapshot.schema_metadata(),
        StoreSchemaMetadataCapability::DurableAcceptedHistory
    );
}

fn assert_empty_report_store_paths(report: &StorageReport) {
    assert_eq!(
        data_paths(report),
        vec![STORE_A_PATH, STORE_HEAP_PATH, STORE_Z_PATH]
    );
    assert_eq!(
        index_paths(report),
        vec![STORE_A_PATH, STORE_HEAP_PATH, STORE_Z_PATH]
    );
    assert_eq!(
        schema_paths(report),
        vec![STORE_A_PATH, STORE_HEAP_PATH, STORE_Z_PATH]
    );
}

fn assert_empty_store_rows_have_no_entries(report: &StorageReport) {
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
fn storage_report_empty_store_snapshot() {
    reset_stores();

    let report = diagnostics_report(&[]);

    assert_eq!(report.corrupted_keys(), 0);
    assert_eq!(report.corrupted_entries(), 0);
    assert!(report.entity_storage().is_empty());

    assert_empty_report_store_paths(&report);
    assert_empty_store_rows_have_no_entries(&report);

    let data_a = report
        .storage_data()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_A_PATH)
        .expect("data snapshot should contain store A");
    let index_a = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_A_PATH)
        .expect("index snapshot should contain store A");
    assert_stable_data_capabilities(data_a);
    assert_eq!(data_a.memory_id(), Some(155));
    assert_eq!(data_a.stable_key(), Some("icydb.test.store_a.data.v1"));
    assert_eq!(data_a.schema_version(), Some(1));
    assert!(data_a.schema_fingerprint().is_some());
    assert_stable_index_capabilities(index_a);
    assert_eq!(index_a.memory_id(), Some(156));
    assert_eq!(index_a.stable_key(), Some("icydb.test.store_a.index.v1"));
    assert_eq!(index_a.schema_version(), Some(1));
    assert!(index_a.schema_fingerprint().is_some());

    let populated_schema = schema_snapshot(&report, STORE_A_PATH);
    let empty_schema = schema_snapshot(&report, STORE_Z_PATH);
    assert_stable_schema_capabilities(populated_schema);
    assert_eq!(populated_schema.memory_id(), Some(158));
    assert_eq!(
        populated_schema.stable_key(),
        Some("icydb.test.store_a.schema.v1")
    );
    assert_eq!(populated_schema.schema_version(), Some(1));
    assert!(populated_schema.schema_fingerprint().is_some());
    assert!(populated_schema.entity_count() > 0);
    assert_ne!(
        data_a.schema_fingerprint(),
        index_a.schema_fingerprint(),
        "data and index snapshots should use role-specific schema metadata"
    );
    assert_ne!(
        data_a.schema_fingerprint(),
        populated_schema.schema_fingerprint(),
        "data and schema snapshots should use role-specific schema metadata"
    );
    assert_ne!(
        index_a.schema_fingerprint(),
        populated_schema.schema_fingerprint(),
        "index and schema snapshots should use role-specific schema metadata"
    );
    assert_stable_schema_capabilities(empty_schema);
    assert_eq!(empty_schema.memory_id(), Some(157));
    assert_eq!(
        empty_schema.stable_key(),
        Some("icydb.test.store_z.schema.v1")
    );
    assert_eq!(empty_schema.schema_version(), None);
    assert_eq!(empty_schema.schema_fingerprint(), None);
    assert_eq!(empty_schema.entity_count(), 0);

    assert_heap_store_snapshot_is_volatile(&report);

    let data_z = report
        .storage_data()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_Z_PATH)
        .expect("data snapshot should contain store Z");
    let index_z = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_Z_PATH)
        .expect("index snapshot should contain store Z");
    assert_stable_data_capabilities(data_z);
    assert_eq!(data_z.memory_id(), Some(153));
    assert_eq!(data_z.stable_key(), Some("icydb.test.store_z.data.v1"));
    assert_eq!(data_z.schema_version(), None);
    assert_eq!(data_z.schema_fingerprint(), None);
    assert_stable_index_capabilities(index_z);
    assert_eq!(index_z.memory_id(), Some(154));
    assert_eq!(index_z.stable_key(), Some("icydb.test.store_z.index.v1"));
    assert_eq!(index_z.schema_version(), None);
    assert_eq!(index_z.schema_fingerprint(), None);
}

#[test]
fn store_snapshot_storage_mode_renders_stable_label() {
    assert_eq!(StoreSnapshotStorageMode::Stable.as_str(), "stable");
    assert_eq!(StoreSnapshotStorageMode::Heap.as_str(), "heap");
    assert_eq!(StoreSnapshotStorageMode::Journaled.as_str(), "journaled");
}

#[test]
fn storage_report_default_matches_empty_alias_snapshot() {
    reset_stores();

    insert_data_row(
        STORE_A_PATH,
        FIRST_ENTITY_NAME,
        PrimaryKeyComponent::Int64(1),
        2,
    );
    insert_data_row(
        STORE_A_PATH,
        SECOND_ENTITY_NAME,
        PrimaryKeyComponent::Int64(2),
        3,
    );
    insert_index_entry(
        STORE_A_PATH,
        index_key(IndexKeyKind::User, "diag_index_entity", "email"),
        IndexEntryValue::presence(),
    );

    let default_report = diagnostics_default_report();
    let aliased_report = diagnostics_report(&[]);

    assert_eq!(
        default_report.corrupted_keys(),
        aliased_report.corrupted_keys()
    );
    assert_eq!(
        default_report.corrupted_entries(),
        aliased_report.corrupted_entries()
    );
    assert_eq!(
        data_snapshot_rows(&default_report),
        data_snapshot_rows(&aliased_report)
    );
    assert_eq!(
        index_snapshot_rows(&default_report),
        index_snapshot_rows(&aliased_report)
    );
    assert_eq!(schema_paths(&default_report), schema_paths(&aliased_report));
    assert_eq!(
        entity_snapshot_rows(&default_report),
        entity_snapshot_rows(&aliased_report)
    );
}

#[test]
fn storage_report_single_entity_multiple_rows() {
    reset_stores();

    insert_data_row(
        STORE_A_PATH,
        SINGLE_ENTITY_NAME,
        PrimaryKeyComponent::Int64(3),
        3,
    );
    insert_data_row(
        STORE_A_PATH,
        SINGLE_ENTITY_NAME,
        PrimaryKeyComponent::Int64(1),
        1,
    );
    insert_data_row(
        STORE_A_PATH,
        SINGLE_ENTITY_NAME,
        PrimaryKeyComponent::Int64(2),
        2,
    );

    let report = diagnostics_report(&[(SINGLE_ENTITY_NAME, SINGLE_ENTITY_PATH)]);
    let entity_snapshot = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == SINGLE_ENTITY_PATH)
        .expect("single-entity snapshot should exist");

    assert_eq!(entity_snapshot.entries(), 3);
}

#[test]
fn storage_report_accepts_composite_primary_key_data_keys() {
    reset_stores();

    let first_key = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Ulid(Ulid::from_timestamp_and_randomness(1, 2)),
    ])
    .expect("first composite diagnostics key should construct");
    let second_key = CompositePrimaryKeyValue::try_from_components(&[
        PrimaryKeyComponent::Nat64(1),
        PrimaryKeyComponent::Ulid(Ulid::from_timestamp_and_randomness(1, 3)),
    ])
    .expect("second composite diagnostics key should construct");
    insert_composite_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, &first_key, 1);
    insert_composite_data_row(STORE_A_PATH, SINGLE_ENTITY_NAME, &second_key, 2);

    let report = diagnostics_report(&[(SINGLE_ENTITY_NAME, SINGLE_ENTITY_PATH)]);
    let entity_snapshot = report
        .entity_storage()
        .iter()
        .find(|snapshot| snapshot.store() == STORE_A_PATH && snapshot.path() == SINGLE_ENTITY_PATH)
        .expect("composite-key entity snapshot should exist");

    assert_eq!(report.corrupted_keys(), 0);
    assert_eq!(entity_snapshot.entries(), 2);
}

#[test]
fn storage_report_multiple_entities_in_same_store() {
    reset_stores();

    insert_data_row(
        STORE_A_PATH,
        FIRST_ENTITY_NAME,
        PrimaryKeyComponent::Int64(10),
        1,
    );
    insert_data_row(
        STORE_A_PATH,
        FIRST_ENTITY_NAME,
        PrimaryKeyComponent::Int64(11),
        1,
    );
    insert_data_row(
        STORE_A_PATH,
        SECOND_ENTITY_NAME,
        PrimaryKeyComponent::Int64(20),
        1,
    );

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

    insert_data_row(
        STORE_Z_PATH,
        FIRST_ENTITY_NAME,
        PrimaryKeyComponent::Int64(1),
        1,
    );
    insert_data_row(
        STORE_A_PATH,
        SECOND_ENTITY_NAME,
        PrimaryKeyComponent::Int64(2),
        1,
    );
    insert_data_row(
        STORE_A_PATH,
        FIRST_ENTITY_NAME,
        PrimaryKeyComponent::Int64(3),
        1,
    );

    let report = diagnostics_report(&[
        (FIRST_ENTITY_NAME, "diagnostics_tests::entity::z_first"),
        (SECOND_ENTITY_NAME, "diagnostics_tests::entity::a_second"),
    ]);

    assert_eq!(
        entity_store_paths(&report),
        vec![
            (STORE_A_PATH, FIRST_ENTITY_PATH),
            (STORE_A_PATH, SECOND_ENTITY_PATH),
            (STORE_Z_PATH, FIRST_ENTITY_PATH),
        ]
    );
}

#[test]
fn storage_report_corrupted_key_detection() {
    reset_stores();

    insert_data_row(
        STORE_A_PATH,
        VALID_ENTITY_NAME,
        PrimaryKeyComponent::Int64(7),
        1,
    );
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
    let corrupted_entry = <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![9]));
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
    let user_entry = IndexEntryValue::presence();
    let system_entry = IndexEntryValue::presence();
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
fn storage_report_index_snapshots_include_runtime_state() {
    reset_stores();

    with_index_store_mut(STORE_A_PATH, IndexStore::mark_building);
    with_index_store_mut(STORE_Z_PATH, IndexStore::mark_dropping);

    let report = diagnostics_report(&[]);
    let store_a = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_A_PATH)
        .expect("store A index snapshot should exist");
    let store_z = report
        .storage_index()
        .iter()
        .find(|snapshot| snapshot.path() == STORE_Z_PATH)
        .expect("store Z index snapshot should exist");

    assert_eq!(store_a.state(), IndexState::Building);
    assert_eq!(store_z.state(), IndexState::Dropping);
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

    insert_data_row(
        STORE_A_PATH,
        "diag_unknown_entity",
        PrimaryKeyComponent::Int64(9),
        8,
    );

    let report = diagnostics_integrity_report();
    let store = integrity_store_snapshot(&report, STORE_A_PATH);

    assert_eq!(store.misuse_findings(), 1);
    assert_eq!(store.corrupted_data_rows(), 0);
    assert_eq!(report.totals().misuse_findings(), 1);
}

#[test]
fn integrity_report_classifies_incompatible_row_formats_as_corruption() {
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

    assert_eq!(store.misuse_findings(), 0);
    assert_eq!(store.corrupted_data_rows(), 1);
    assert_eq!(report.totals().corrupted_data_rows(), 1);
}

#[test]
fn storage_report_candid_shape_is_stable() {
    let fields = expect_record_fields(StorageReport::ty());

    for field in [
        "storage_data",
        "storage_index",
        "schema_storage",
        "entity_storage",
        "corrupted_keys",
        "corrupted_entries",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "StorageReport must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn data_store_snapshot_candid_shape_is_stable() {
    let fields = expect_record_fields(DataStoreSnapshot::ty());

    for field in [
        "path",
        "storage",
        "allocation",
        "durability",
        "commit",
        "recovery",
        "schema_metadata",
        "memory_id",
        "stable_key",
        "schema_version",
        "schema_fingerprint",
        "entries",
        "memory_bytes",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "DataStoreSnapshot must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn index_store_snapshot_candid_shape_is_stable() {
    let fields = expect_record_fields(IndexStoreSnapshot::ty());

    for field in [
        "path",
        "storage",
        "allocation",
        "durability",
        "commit",
        "recovery",
        "schema_metadata",
        "memory_id",
        "stable_key",
        "schema_version",
        "schema_fingerprint",
        "entries",
        "user_entries",
        "system_entries",
        "memory_bytes",
        "state",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "IndexStoreSnapshot must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn schema_store_snapshot_candid_shape_is_stable() {
    let fields = expect_record_fields(SchemaStoreSnapshot::ty());

    for field in [
        "path",
        "storage",
        "allocation",
        "durability",
        "commit",
        "recovery",
        "schema_metadata",
        "memory_id",
        "stable_key",
        "schema_version",
        "schema_fingerprint",
        "entity_count",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "SchemaStoreSnapshot must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn entity_snapshot_candid_shape_is_stable() {
    let fields = expect_record_fields(EntitySnapshot::ty());

    for field in ["store", "path", "entries", "memory_bytes"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "EntitySnapshot must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn integrity_totals_candid_shape_is_stable() {
    let fields = expect_record_fields(IntegrityTotals::ty());

    for field in [
        "data_rows_scanned",
        "index_entries_scanned",
        "corrupted_data_keys",
        "corrupted_data_rows",
        "corrupted_index_keys",
        "corrupted_index_entries",
        "missing_index_entries",
        "divergent_index_entries",
        "orphan_index_references",
        "misuse_findings",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "IntegrityTotals must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn integrity_store_snapshot_candid_shape_is_stable() {
    let fields = expect_record_fields(IntegrityStoreSnapshot::ty());

    for field in [
        "path",
        "data_rows_scanned",
        "index_entries_scanned",
        "corrupted_data_keys",
        "corrupted_data_rows",
        "corrupted_index_keys",
        "corrupted_index_entries",
        "missing_index_entries",
        "divergent_index_entries",
        "orphan_index_references",
        "misuse_findings",
    ] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "IntegrityStoreSnapshot must keep `{field}` as Candid field key",
        );
    }
}

#[test]
fn integrity_report_candid_shape_is_stable() {
    let fields = expect_record_fields(IntegrityReport::ty());

    for field in ["stores", "totals"] {
        assert!(
            fields.iter().any(|candidate| candidate == field),
            "IntegrityReport must keep `{field}` as Candid field key",
        );
    }
}
