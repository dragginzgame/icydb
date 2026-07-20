use crate::{
    db::{
        Db, EntityRuntimeHooks,
        commit::{
            CommitFailpoint, CommitFailpointMode, CommitMarker, arm_commit_failpoint_for_tests,
            begin_commit, clear_commit_marker_for_tests, clear_recovery_runtime_state_for_tests,
            commit_marker_present, ensure_recovered, generate_commit_id,
            init_commit_store_for_tests,
        },
        data::{CanonicalRow, DataStore, DecodedDataStoreKey},
        index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore},
        journal::{JournalBatch, JournalRecord, JournalTailStore},
        registry::StoreRegistry,
        schema::{
            AcceptedFieldKind, FieldId, PersistedFieldSnapshot, PersistedIndexSnapshot,
            PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
            SchemaFieldSlot, SchemaRowLayout, SchemaStore, SchemaTransitionPlanKind, SchemaVersion,
            compiled_schema_proposal_for_model,
        },
    },
    entity::{EntityDeclaration, EntityKind},
    error::ErrorClass,
    metrics::{metrics_report, metrics_reset_all},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
        index::IndexModel,
    },
    testing::{entity_model_from_static, test_memory},
    traits::Path,
    types::{EntityTag, Ulid},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cell::RefCell;

fn assert_runtime_unsupported_diagnostic(err: &crate::error::InternalError, context: &str) {
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
        "{context}: compact unsupported diagnostic drifted: {err:?}",
    );
}

crate::test_canister! {
    ident = SchemaReconcileTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = SchemaReconcileTestStore,
    canister = SchemaReconcileTestCanister,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SchemaReconcileEntity {
    id: Ulid,
    name: String,
}

crate::test_entity! {
    ident = SchemaReconcileEntity,
    entity_name = "SchemaReconcileEntity",
    tag = EntityTag::new(0x7465_7374_7363_6865),
    store = SchemaReconcileTestStore,
    canister = SchemaReconcileTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

static INDEXED_SCHEMA_NAME_INDEX: IndexModel = IndexModel::generated_with_ordinal(
    1,
    "by_name",
    SchemaReconcileTestStore::PATH,
    &["name"],
    false,
);

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct IndexedSchemaEntity {
    id: Ulid,
    name: String,
}

crate::test_entity! {
    ident = IndexedSchemaEntity,
    entity_name = "IndexedSchemaEntity",
    tag = EntityTag::new(0x696e_6478_7363_6865),
    store = SchemaReconcileTestStore,
    canister = SchemaReconcileTestCanister,
    version = 2,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&INDEXED_SCHEMA_NAME_INDEX],
    relations = [],
    entity_value = id_field(id),
}

static NESTED_PROFILE_FIELDS: [FieldModel; 1] = [FieldModel::generated("rank", FieldKind::Nat64)];
static NESTED_SCHEMA_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
        "profile",
        FieldKind::Structured { queryable: true },
        FieldStorageDecode::Value,
        false,
        None,
        None,
        &NESTED_PROFILE_FIELDS,
    ),
];
static NESTED_SCHEMA_INDEXES: [&IndexModel; 0] = [];
static NESTED_SCHEMA_MODEL: EntityModel = entity_model_from_static(
    "schema::reconcile::tests::NestedSchemaEntity",
    "NestedSchemaEntity",
    &NESTED_SCHEMA_FIELDS[0],
    0,
    &NESTED_SCHEMA_FIELDS,
    &NESTED_SCHEMA_INDEXES,
);
const NESTED_SCHEMA_ENTITY_TAG: EntityTag = EntityTag::new(0x6e65_7374_7363_6865);
static ADDITIVE_NULLABLE_SCHEMA_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("name", FieldKind::Text { max_len: None }),
    FieldModel::generated_with_storage_decode_and_nullability(
        "nickname",
        FieldKind::Text { max_len: None },
        FieldStorageDecode::ByKind,
        true,
    ),
];
static ADDITIVE_NULLABLE_SCHEMA_INDEXES: [&IndexModel; 0] = [];
static ADDITIVE_NULLABLE_SCHEMA_MODEL: EntityModel = EntityModel::generated(
    "schema::reconcile::tests::AdditiveNullableSchemaEntity",
    "AdditiveNullableSchemaEntity",
    2,
    &ADDITIVE_NULLABLE_SCHEMA_FIELDS[0],
    0,
    &ADDITIVE_NULLABLE_SCHEMA_FIELDS,
    &ADDITIVE_NULLABLE_SCHEMA_INDEXES,
);
const ADDITIVE_NULLABLE_ENTITY_TAG: EntityTag = EntityTag::new(0x6164_6469_7469_7665);
thread_local! {
    static RECONCILE_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(252)));
    static RECONCILE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(253)));
    static RECONCILE_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(254)));
    static RECONCILE_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(250)));
    static RECONCILE_STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry
            .register_journaled_store(
                SchemaReconcileTestStore::PATH,
                &RECONCILE_DATA_STORE,
                &RECONCILE_INDEX_STORE,
                &RECONCILE_SCHEMA_STORE,
                &RECONCILE_JOURNAL_STORE,
                crate::db::StoreAllocationIdentities::new_journaled(
                    crate::db::StoreAllocationIdentity::new(
                        252,
                        "icydb.test.reconcile.data.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        253,
                        "icydb.test.reconcile.index.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        254,
                        "icydb.test.reconcile.schema.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        250,
                        "icydb.test.reconcile.journal.v1",
                    ),
                ),
                crate::db::StoreRuntimeStorageCapabilities::journaled(),
            )
            .expect("schema reconcile test store should register");
        registry
    };
}

static RECONCILE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::for_entity::<SchemaReconcileEntity>()];
static RECONCILE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, RECONCILE_RUNTIME_HOOKS);
static INDEXED_RECONCILE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
static INDEXED_RECONCILE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, INDEXED_RECONCILE_RUNTIME_HOOKS);

fn reset_schema_store() {
    RECONCILE_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
}

fn reset_reconcile_stores() {
    RECONCILE_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    RECONCILE_DATA_STORE.with_borrow_mut(DataStore::clear);
    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });
}

fn indexed_schema_snapshot_without_indexes() -> PersistedSchemaSnapshot {
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_version = SchemaVersion::new(expected.version().get().saturating_sub(1));
    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        stored_version,
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            stored_version,
            expected.row_layout().field_to_slot().to_vec(),
        ),
        expected.fields().to_vec(),
        Vec::new(),
    )
}

fn stage_and_publish_indexed_schema_snapshot_without_indexes() -> PersistedSchemaSnapshot {
    let snapshot = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &snapshot)
            .expect("stored index-free schema snapshot should encode");
        super::publish_test_accepted_schema_snapshot(
            store,
            IndexedSchemaEntity::ENTITY_TAG,
            IndexedSchemaEntity::MODEL.path(),
            SchemaReconcileTestStore::PATH,
            IndexedSchemaEntity::MODEL,
            snapshot.clone(),
        )
        .expect("stored index-free accepted root should publish");
    });
    snapshot
}

fn indexed_schema_snapshot_with_renamed_index(index_name: &str) -> PersistedSchemaSnapshot {
    indexed_schema_snapshot_with_renamed_index_and_extra_indexes(index_name, Vec::new())
}

fn indexed_schema_snapshot_with_renamed_index_and_extra_indexes(
    index_name: &str,
    extra_indexes: Vec<PersistedIndexSnapshot>,
) -> PersistedSchemaSnapshot {
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let [expected_index] = expected.indexes() else {
        panic!("indexed schema fixture should have one generated index");
    };
    let renamed_index = PersistedIndexSnapshot::new(
        expected_index.ordinal(),
        index_name.to_string(),
        expected_index.store().to_string(),
        expected_index.unique(),
        expected_index.key().clone(),
        expected_index.predicate_sql().map(str::to_string),
    );
    let mut indexes = vec![renamed_index];
    indexes.extend(extra_indexes);

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        expected.row_layout().clone(),
        expected.fields().to_vec(),
        indexes,
    )
}

fn indexed_schema_ddl_extra_index() -> PersistedIndexSnapshot {
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let [expected_index] = expected.indexes() else {
        panic!("indexed schema fixture should have one generated index");
    };

    PersistedIndexSnapshot::new(
        expected_index.ordinal() + 1,
        "ddl_name_idx".to_string(),
        SchemaReconcileTestStore::PATH.to_string(),
        false,
        expected_index.key().clone(),
        None,
    )
}

fn insert_indexed_schema_row(id: u128, name: &str) {
    let id = Ulid::from_u128(id);
    let data_key =
        DecodedDataStoreKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
    let raw_key = data_key.to_raw().expect("test key should encode to raw");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(&IndexedSchemaEntity {
        id,
        name: name.to_string(),
    })
    .expect("indexed schema row should encode");
    RECONCILE_DATA_STORE.with_borrow_mut(|store| {
        store
            .fold_recovered_journal_put(raw_key, row.into_raw_row())
            .expect("indexed schema row should enter the canonical test base");
    });
}

#[test]
fn reconcile_runtime_schemas_writes_initial_snapshot_on_first_contact() {
    reset_schema_store();
    metrics_reset_all();

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema reconciliation should write generated snapshot");

    let snapshot = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store
                .get_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, SchemaVersion::initial())
        })
        .expect("persisted schema snapshot should decode");
    let snapshot = snapshot.expect("initial schema snapshot should be persisted");

    assert_eq!(snapshot.entity_path(), SchemaReconcileEntity::PATH);
    assert_eq!(snapshot.fields().len(), 2);
    let bundle = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted schema bundle should decode")
        .expect("first reconciliation should publish one accepted schema root");
    assert_eq!(bundle.revision(), super::AcceptedSchemaRevision::INITIAL);
    assert_eq!(bundle.store_path(), SchemaReconcileTestStore::PATH);

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_first_create(), 1);
    assert_eq!(
        counters.ops().schema_transition_checks(),
        0,
        "first-create reconciliation has no existing schema transition decision",
    );
    assert_eq!(counters.ops().schema_store_snapshots(), 1);
    assert!(counters.ops().schema_store_encoded_bytes() > 0);
    assert_eq!(
        counters.ops().schema_store_latest_snapshot_bytes(),
        counters.ops().schema_store_encoded_bytes(),
    );
    assert_eq!(counters.ops().accepted_schema_fields(), 2);
    assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 0);
}

#[test]
fn reconcile_runtime_schemas_publishes_declared_version_on_first_contact() {
    reset_schema_store();
    metrics_reset_all();

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let declared_version = proposal.declared_schema_version();
    assert_eq!(declared_version, SchemaVersion::new(2));

    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect("initial schema reconciliation should write generated snapshot");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("schema store latest snapshot should decode")
        .expect("initial schema snapshot should be persisted");
    let by_declared_version = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.get_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, declared_version)
        })
        .expect("declared-version schema snapshot should decode")
        .expect("declared-version schema snapshot should be persisted");
    let initial_version_snapshot = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.get_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, SchemaVersion::initial())
        })
        .expect("initial-version schema snapshot lookup should decode");

    assert_eq!(latest.version(), declared_version);
    assert_eq!(latest.row_layout().version(), declared_version);
    assert_eq!(by_declared_version.version(), declared_version);
    assert!(
        initial_version_snapshot.is_none(),
        "first contact must not synthesize an initial-version snapshot when generated code declares v2",
    );
    assert_eq!(latest.indexes().len(), 1);
    assert_eq!(RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::len), 1);

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_first_create(), 1);
}

#[test]
fn reconcile_runtime_schemas_accepts_existing_matching_snapshot() {
    reset_schema_store();
    metrics_reset_all();
    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema reconciliation should write generated snapshot");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("matching persisted schema should be accepted");

    assert_eq!(RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::len), 1);
    let bundle = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted schema bundle should decode")
        .expect("accepted schema root should remain published");
    assert_eq!(
        bundle.revision(),
        super::AcceptedSchemaRevision::INITIAL,
        "an exact semantic no-op must not publish a new revision",
    );

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 2);
    assert_eq!(counters.ops().schema_reconcile_first_create(), 1);
    assert_eq!(counters.ops().schema_reconcile_exact_match(), 1);
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(counters.ops().schema_transition_exact_match(), 1);
    assert_eq!(
        counters.ops().accepted_schema_fields(),
        2,
        "accepted-schema footprint should stay a replaced entity gauge instead of double-counting exact-match reconciliation",
    );
    assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 0);
}

#[test]
fn transition_metrics_preserve_current_plan_identity() {
    assert_eq!(
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::AddExpressionIndex),
        crate::metrics::SchemaTransitionOutcome::AddExpressionIndex,
    );
    assert_eq!(
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::AddFieldPathIndex),
        crate::metrics::SchemaTransitionOutcome::AddFieldPathIndex,
    );
    assert_eq!(
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::AppendOnlyNullableFields),
        crate::metrics::SchemaTransitionOutcome::AppendOnlyNullableFields,
    );
    assert_eq!(
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::ExactMatch),
        crate::metrics::SchemaTransitionOutcome::ExactMatch,
    );
    assert_eq!(
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::MetadataOnlyIndexRename),
        crate::metrics::SchemaTransitionOutcome::MetadataOnlyIndexRename,
    );
}

#[test]
fn accepted_schema_post_root_change_publishes_through_marker_bound_journal() {
    reset_schema_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    metrics_reset_all();
    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema reconciliation should publish revision one");

    let catalog =
        super::build_generated_enum_catalog_candidates(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("generated catalog should build")
            .remove(SchemaReconcileTestStore::PATH)
            .expect("store catalog should exist");
    let changed_snapshot = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL)
        .initial_persisted_schema_snapshot()
        .clone_with_version(SchemaVersion::new(2));
    super::publish_generated_accepted_schema_bundle(
        RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("store should resolve"),
        SchemaReconcileTestStore::PATH,
        catalog,
        std::collections::BTreeMap::from([(SchemaReconcileEntity::ENTITY_TAG, changed_snapshot)]),
        Vec::new(),
    )
    .expect("post-root change should publish through the journal");

    let bundle = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted schema bundle should decode")
        .expect("new accepted root should be visible");
    assert_eq!(bundle.revision(), super::AcceptedSchemaRevision::new(2));
    assert_eq!(
        RECONCILE_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        1
    );
    assert_eq!(
        RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::canonical_len_for_tests),
        3
    );
    assert!(!commit_marker_present().expect("commit marker should decode"));
}

#[test]
fn accepted_schema_marker_recovery_repairs_replays_and_folds_candidate() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_DB)
        .expect("recovery runtime state should clear");
    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema reconciliation should publish revision one");

    let current = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("accepted schema bundle should decode")
        .expect("accepted schema bundle should exist");
    let next_bundle = super::AcceptedSchemaRevisionBundle::new(
        super::AcceptedSchemaRevision::new(2),
        current.store_path(),
        current.enum_catalog().clone(),
        current.entity_snapshots().clone(),
    )
    .expect("next accepted schema bundle should build");
    let candidate = super::CandidateSchemaRevision::new(next_bundle)
        .expect("next accepted schema candidate should encode");
    let record = JournalRecord::accepted_schema_publish(
        SchemaReconcileTestStore::PATH,
        super::AcceptedSchemaRevision::INITIAL,
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )
    .expect("accepted schema journal record should build");
    let marker_id = generate_commit_id().expect("commit id should generate");
    let sequence = RECONCILE_JOURNAL_STORE
        .with_borrow(JournalTailStore::next_append_sequence)
        .expect("journal sequence should allocate");
    let batch = JournalBatch::new(marker_id, marker_id, sequence, vec![record])
        .expect("accepted schema journal batch should build");
    let marker = CommitMarker::from_parts(marker_id, vec![batch])
        .expect("accepted schema commit marker should build");
    let _unfinished = begin_commit(marker).expect("crash fixture marker should persist");

    ensure_recovered(&RECONCILE_DB)
        .expect("recovery should repair, replay, and fold accepted schema publication");

    let recovered = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("recovered accepted schema bundle should decode")
        .expect("recovered accepted schema bundle should exist");
    assert_eq!(recovered.revision(), super::AcceptedSchemaRevision::new(2));
    assert_eq!(
        RECONCILE_SCHEMA_STORE.with_borrow(SchemaStore::canonical_len_for_tests),
        3,
        "recovery should retain only the current entity snapshot, immutable bundle, and selected root",
    );
    assert_eq!(
        RECONCILE_JOURNAL_STORE.with_borrow(JournalTailStore::len),
        0
    );
    assert!(!commit_marker_present().expect("commit marker should decode"));
}

#[test]
fn reconcile_staged_schema_snapshot_accepts_append_only_nullable_field() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(243));
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_version = SchemaVersion::new(expected.version().get().saturating_sub(1));
    let stored_prefix = PersistedSchemaSnapshot::new(
        stored_version,
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            stored_version,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        expected.fields()[..2].to_vec(),
    );
    schema_store
        .insert_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG, &stored_prefix)
        .expect("stored prefix schema snapshot should encode");

    let accepted = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        ADDITIVE_NULLABLE_ENTITY_TAG,
        ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
        &ADDITIVE_NULLABLE_SCHEMA_MODEL,
    )
    .expect("append-only nullable generated field should be accepted");
    let latest = schema_store
        .latest_staged_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG)
        .expect("schema store latest snapshot should decode")
        .expect("schema store should retain accepted additive snapshot");

    assert_eq!(accepted.footprint().fields(), 3);
    assert_eq!(latest.fields().len(), 3);
    assert_eq!(
        schema_store.len(),
        2,
        "schema-versioned publication should retain the stored v1 prefix and accepted v2 snapshot",
    );

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters
            .ops()
            .schema_transition_append_only_nullable_fields(),
        1
    );
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        0
    );
    assert_eq!(counters.ops().accepted_schema_fields(), 3);
}

#[test]
fn valid_version_bump_still_rejects_unsupported_field_contract_transition() {
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_version = SchemaVersion::new(expected.version().get().saturating_sub(1));
    let stored_prefix = PersistedSchemaSnapshot::new(
        stored_version,
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            stored_version,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        expected.fields()[..2].to_vec(),
    );
    let mut unsupported_fields = expected.fields().to_vec();
    unsupported_fields[2] = PersistedFieldSnapshot::new(
        FieldId::new(3),
        "nickname".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        false,
        SchemaFieldDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let unsupported_required_field = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        expected.row_layout().clone(),
        unsupported_fields,
    );

    let err = super::validate_existing_schema_snapshot(
        ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
        &stored_prefix,
        &unsupported_required_field,
    )
    .expect_err("valid version bump must not publish unsupported additive fields");

    assert_runtime_unsupported_diagnostic(
        &err,
        "valid N+1 schema version bump should reach compatibility rejection",
    );

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_schema_version(),
        0,
        "valid version bump should not be bucketed as a schema-version rejection",
    );
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        1,
        "unsupported additive field must stay a compatibility rejection after the gate",
    );
    assert_eq!(
        counters.ops().schema_reconcile_rejected_schema_version(),
        0,
        "schema-version admission should not own valid-bump compatibility failures",
    );
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
}

#[test]
fn reconcile_staged_schema_snapshot_publishes_metadata_only_index_rename() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(240));
    let stored = indexed_schema_snapshot_with_renamed_index("IndexedSchemaEntity|name");
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored)
        .expect("stored renamed-index schema snapshot should encode");

    let accepted = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect("metadata-only generated index rename should be accepted");
    let latest = schema_store
        .latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        .expect("schema store latest snapshot should decode")
        .expect("schema store should retain accepted renamed-index snapshot");

    assert_eq!(accepted.persisted_snapshot().indexes()[0].name(), "by_name");
    assert_eq!(latest.indexes()[0].name(), "by_name");
    assert_eq!(schema_store.len(), 1);
}

#[test]
fn reconcile_staged_schema_snapshot_preserves_ddl_indexes_during_generated_index_rename() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(239));
    let stored = indexed_schema_snapshot_with_renamed_index_and_extra_indexes(
        "IndexedSchemaEntity|name",
        vec![indexed_schema_ddl_extra_index()],
    );
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored)
        .expect("stored renamed-index schema snapshot should encode");

    let accepted = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect("generated index rename with extra DDL indexes should be accepted");
    let latest = schema_store
        .latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        .expect("schema store latest snapshot should decode")
        .expect("schema store should retain accepted merged snapshot");

    assert_eq!(accepted.persisted_snapshot().indexes().len(), 2);
    assert_eq!(accepted.persisted_snapshot().indexes()[0].name(), "by_name");
    assert_eq!(
        accepted.persisted_snapshot().indexes()[1].name(),
        "ddl_name_idx",
    );
    assert_eq!(latest.indexes().len(), 2);
    assert_eq!(latest.indexes()[0].name(), "by_name");
    assert_eq!(latest.indexes()[1].name(), "ddl_name_idx");
}

#[test]
fn reconcile_runtime_schemas_executes_supported_field_path_index_addition() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    metrics_reset_all();

    let _stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();

    let id = Ulid::from_u128(15_401);
    let data_key =
        DecodedDataStoreKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
    let raw_key = data_key.to_raw().expect("test key should encode to raw");
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(&IndexedSchemaEntity {
        id,
        name: "Ada".to_string(),
    })
    .expect("indexed schema row should encode");
    RECONCILE_DATA_STORE.with_borrow_mut(|store| {
        let _ = store.insert(raw_key, row);
    });

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect("supported field-path index addition should rebuild and publish");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("latest schema snapshot should decode")
        .expect("indexed schema snapshot should be published");
    assert_eq!(latest.indexes().len(), 1);
    assert_eq!(latest.indexes()[0].name(), "by_name");
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), crate::db::index::IndexState::Ready);
    });

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_exact_match(), 1);
    assert_eq!(counters.ops().schema_transition_add_field_path_index(), 1);
    assert_eq!(counters.ops().accepted_schema_fields(), 2);
}

#[test]
fn reconcile_runtime_schemas_keeps_zero_write_stage_when_marker_persistence_rejects() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    metrics_reset_all();

    let stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();
    insert_indexed_schema_row(15_401, "Ada");
    arm_commit_failpoint_for_tests(
        CommitFailpoint::BeforeMarkerWrite,
        CommitFailpointMode::ReturnError,
    );

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect_err("marker persistence failure should reject staged reconciliation");

    assert!(!commit_marker_present().expect("commit marker should decode"));
    let staged = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("staged schema snapshot should decode")
        .expect("accepted-before staged snapshot should remain");
    let accepted = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.current_accepted_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("accepted schema snapshot should decode")
        .expect("accepted-before root should remain");
    assert_eq!(staged, stored_without_index);
    assert_eq!(accepted, stored_without_index);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 0);
        assert_eq!(store.state(), IndexState::Ready);
    });

    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect("retry should rebuild and publish the field-path index");
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), IndexState::Ready);
    });
}

#[test]
fn reconcile_runtime_schemas_recovers_marker_authorized_index_domain_after_interruption() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&INDEXED_RECONCILE_DB)
        .expect("indexed recovery runtime state should clear");
    metrics_reset_all();

    let stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();
    insert_indexed_schema_row(15_401, "Ada");
    arm_commit_failpoint_for_tests(
        CommitFailpoint::AfterMarkerWrite,
        CommitFailpointMode::ReturnError,
    );

    let error =
        super::reconcile_runtime_schemas(&INDEXED_RECONCILE_DB, INDEXED_RECONCILE_RUNTIME_HOOKS)
            .expect_err("marker interruption should stop before live index-domain apply");

    assert!(
        commit_marker_present().expect("commit marker should decode"),
        "startup publication should reach the armed marker boundary, got class={:?}, detail={:?}",
        error.class,
        error.detail(),
    );
    let accepted_before = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.current_accepted_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("accepted schema should decode before recovery")
        .expect("accepted-before schema should remain published");
    assert_eq!(accepted_before, stored_without_index);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert!(store.is_empty());
        assert_eq!(store.state(), IndexState::Ready);
    });

    ensure_recovered(&INDEXED_RECONCILE_DB).unwrap_or_else(|error| {
        let (index_len, index_state) =
            RECONCILE_INDEX_STORE.with_borrow(|store| (store.len(), store.state()));
        let accepted_index_count = RECONCILE_SCHEMA_STORE
            .with_borrow(|store| {
                store.current_accepted_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
            })
            .ok()
            .flatten()
            .map_or(usize::MAX, |snapshot| snapshot.indexes().len());
        panic!(
            "marker-authorized startup publication should recover: diagnostic={error:?}, class={:?}, origin={:?}, detail={:?}, index_len={index_len}, index_state={index_state:?}, accepted_index_count={accepted_index_count}",
            error.class(),
            error.origin(),
            error.detail(),
        );
    });

    assert!(!commit_marker_present().expect("commit marker should decode"));
    let accepted_after = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.current_accepted_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("accepted schema should decode after recovery")
        .expect("accepted-after schema should be published by recovery");
    assert_eq!(accepted_after.indexes().len(), 1);
    assert_eq!(accepted_after.indexes()[0].name(), "by_name");
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), IndexState::Ready);
    });
}

#[test]
fn reconcile_runtime_schemas_rejects_field_path_index_addition_with_populated_target_index() {
    reset_reconcile_stores();
    metrics_reset_all();

    let _stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();

    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        let sentinel_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, 1);
        let sentinel_key = IndexKey::empty_with_kind(&sentinel_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let sentinel_entry = IndexEntryValue::presence();
        store.insert(sentinel_key, sentinel_entry);
    });

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect_err("populated target physical index should fail closed");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), IndexState::Ready);
    });
}

#[test]
fn reconcile_runtime_schemas_rejects_field_path_index_addition_with_building_index_store() {
    reset_reconcile_stores();
    metrics_reset_all();

    let _stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();
    insert_indexed_schema_row(15_401, "Ada");

    RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_building);

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect_err("building physical index store should fail closed before schema publish");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 0);
        assert_eq!(store.state(), IndexState::Building);
    });

    RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_ready);
}

#[test]
fn reconcile_runtime_schemas_preserves_other_entity_index_entries() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    metrics_reset_all();

    let _stored_without_index = stage_and_publish_indexed_schema_snapshot_without_indexes();

    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        let unrelated_id = IndexId::new(EntityTag::new(99), 1);
        let unrelated_key = IndexKey::empty_with_kind(&unrelated_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let unrelated_entry = IndexEntryValue::presence();
        store.insert(unrelated_key, unrelated_entry);
    });

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect("another entity's physical index entries should remain outside the replacement");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        })
        .expect("latest schema snapshot should decode")
        .expect("indexed schema snapshot should be published");
    assert_eq!(latest.indexes().len(), 1);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), IndexState::Ready);
    });
}

#[test]
fn reconcile_staged_schema_snapshot_rejects_field_path_index_addition_without_runtime_store() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(244));
    metrics_reset_all();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
        .expect("stored index-free schema snapshot should encode");

    let err = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect_err("metadata-only reconciliation must not execute physical index addition");

    assert_eq!(err.class, ErrorClass::Unsupported);
    let latest = schema_store
        .latest_staged_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
}

#[test]
fn reconcile_staged_schema_snapshot_records_nested_leaf_footprint() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(241));
    metrics_reset_all();

    let accepted = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        NESTED_SCHEMA_ENTITY_TAG,
        NESTED_SCHEMA_MODEL.path(),
        &NESTED_SCHEMA_MODEL,
    )
    .expect("nested schema snapshot should be accepted on first contact");

    let footprint = accepted.footprint();
    assert_eq!(footprint.fields(), 2);
    assert_eq!(footprint.nested_leaf_facts(), 1);

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("accepted nested schema should record metrics");
    assert_eq!(counters.ops().accepted_schema_fields(), 2);
    assert_eq!(counters.ops().accepted_schema_nested_leaf_facts(), 1);

    let summary = report
        .entity_counters()
        .iter()
        .find(|summary| summary.path() == NESTED_SCHEMA_MODEL.path())
        .expect("accepted nested schema should record an entity summary");
    assert_eq!(summary.accepted_schema_fields(), 2);
    assert_eq!(summary.accepted_schema_nested_leaf_facts(), 1);
}

#[test]
fn reconcile_staged_schema_snapshot_rejects_nested_leaf_drift_as_field_contract() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(242));
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(&NESTED_SCHEMA_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let mut stored_fields = expected.fields().to_vec();
    let profile = &expected.fields()[1];
    stored_fields[1] = PersistedFieldSnapshot::new(
        profile.id(),
        profile.name().to_string(),
        profile.slot(),
        profile.kind().clone(),
        vec![PersistedNestedLeafSnapshot::new(
            vec!["removed_rank".to_string()],
            AcceptedFieldKind::Nat64,
            false,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )],
        profile.nullable(),
        profile.default().clone(),
        profile.storage_decode(),
        profile.leaf_codec(),
    );
    let stored_with_nested_leaf_drift = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        expected.row_layout().clone(),
        stored_fields,
    );
    schema_store
        .insert_persisted_snapshot(NESTED_SCHEMA_ENTITY_TAG, &stored_with_nested_leaf_drift)
        .expect("stored nested-leaf drift snapshot should encode");

    let err = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        NESTED_SCHEMA_ENTITY_TAG,
        NESTED_SCHEMA_MODEL.path(),
        &NESTED_SCHEMA_MODEL,
    )
    .expect_err("nested leaf schema drift should still be rejected");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported,
        "nested leaf drift should remain a compact unsupported transition diagnostic",
    );

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 0);
    assert_eq!(
        counters.ops().schema_reconcile_rejected_schema_version(),
        1,
        "schema-version admission is the primary reconcile bucket while preserving field-contract detail",
    );
    assert_eq!(
        counters.ops().schema_reconcile_rejected_row_layout(),
        0,
        "nested leaf drift should stay in field-contract transition buckets",
    );
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        0
    );
    assert_eq!(
        counters.ops().schema_transition_rejected_schema_version(),
        1,
        "schema-version admission is the primary rejection bucket while preserving field-contract detail",
    );
}

#[test]
fn reconcile_runtime_schemas_rejects_changed_initial_snapshot() {
    reset_schema_store();
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        "ChangedSchemaReconcileEntity".to_string(),
        expected.first_primary_key_field_id(),
        expected.row_layout().clone(),
        expected.fields().to_vec(),
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &changed)
            .expect("changed schema snapshot should encode");
    });

    let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect_err("schema reconciliation should reject changed persisted snapshot");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_runtime_unsupported_diagnostic(
        &err,
        "changed persisted snapshot should fail at evolution boundary",
    );

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 1);
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_entity_identity(),
        1
    );
}

#[test]
fn reconcile_runtime_schemas_rejects_generated_additive_field_as_field_contract() {
    reset_schema_store();
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_prefix = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            expected.row_layout().version(),
            vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
        ),
        expected.fields()[..1].to_vec(),
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &stored_prefix)
            .expect("stored prefix schema snapshot should encode");
    });

    let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect_err("additive generated schema drift should still be rejected");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_runtime_unsupported_diagnostic(&err, "additive generated schema drift");

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 0);
    assert_eq!(
        counters.ops().schema_reconcile_rejected_schema_version(),
        1,
        "schema-version admission should reject generated removed-field drift before compatibility publication",
    );
    assert_eq!(
        counters.ops().schema_reconcile_rejected_row_layout(),
        0,
        "append-only generated fields should no longer be bucketed as generic row-layout drift",
    );
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        0
    );
    assert_eq!(
        counters.ops().schema_transition_rejected_schema_version(),
        1,
        "schema-version admission should reject generated additive drift before compatibility publication",
    );
}

#[test]
fn reconcile_runtime_schemas_rejects_generated_removed_field_as_field_contract() {
    reset_schema_store();
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let mut stored_fields = expected.fields().to_vec();
    stored_fields.push(PersistedFieldSnapshot::new(
        FieldId::new(3),
        "removed_score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaFieldDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let stored_with_removed_field = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            expected.row_layout().version(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        stored_fields,
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(
                SchemaReconcileEntity::ENTITY_TAG,
                &stored_with_removed_field,
            )
            .expect("stored removed-field schema snapshot should encode");
    });

    let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect_err("generated field removal should still be rejected");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_runtime_unsupported_diagnostic(&err, "generated removed-field drift");

    let report = metrics_report(None);
    let counters = report
        .counters()
        .expect("schema reconciliation should record metrics");
    assert_eq!(counters.ops().schema_reconcile_checks(), 1);
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 0);
    assert_eq!(
        counters.ops().schema_reconcile_rejected_schema_version(),
        1,
        "schema-version admission should reject generated removed-field drift before compatibility publication",
    );
    assert_eq!(
        counters.ops().schema_reconcile_rejected_row_layout(),
        0,
        "append-only stored fields should no longer be bucketed as generic row-layout drift",
    );
    assert_eq!(counters.ops().schema_transition_checks(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        0
    );
    assert_eq!(
        counters.ops().schema_transition_rejected_schema_version(),
        1,
        "schema-version admission should reject generated removed-field drift before compatibility publication",
    );
}

#[test]
fn reconcile_runtime_schemas_preserves_newer_matching_accepted_snapshot() {
    reset_schema_store();

    let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let newer_row_layout = SchemaRowLayout::new(
        SchemaVersion::new(2),
        expected.row_layout().field_to_slot().to_vec(),
    );
    let newer = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        newer_row_layout,
        expected.fields().to_vec(),
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &newer)
            .expect("newer schema snapshot should encode");
        super::publish_test_accepted_schema_snapshot(
            store,
            SchemaReconcileEntity::ENTITY_TAG,
            SchemaReconcileEntity::MODEL.path(),
            SchemaReconcileTestStore::PATH,
            SchemaReconcileEntity::MODEL,
            newer.clone(),
        )
        .expect("newer accepted schema snapshot should publish");
    });

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("matching generated metadata must not roll back newer accepted authority");

    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        assert_eq!(
            store
                .current_accepted_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG)
                .expect("accepted schema snapshot should decode"),
            Some(newer),
            "reconciliation must preserve the newer accepted schema and layout version",
        );
    });
}

#[test]
fn runtime_schema_reads_ignore_unpublished_staged_snapshot() {
    reset_schema_store();
    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial accepted root should publish");

    let proposal = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL);
    let accepted = proposal.initial_persisted_schema_snapshot();
    let staged_row_layout = SchemaRowLayout::new(
        SchemaVersion::new(2),
        accepted.row_layout().field_to_slot().to_vec(),
    );
    let staged = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        accepted.entity_path().to_string(),
        accepted.entity_name().to_string(),
        accepted.primary_key_field_ids().to_vec(),
        staged_row_layout,
        accepted.fields().to_vec(),
    );

    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG, &staged)
            .expect("unpublished candidate snapshot should stage");

        assert_eq!(
            store
                .latest_staged_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG)
                .expect("staged snapshot should decode"),
            Some(staged),
        );
        assert_eq!(
            store
                .current_accepted_persisted_snapshot(SchemaReconcileEntity::ENTITY_TAG)
                .expect("accepted root snapshot should decode"),
            Some(accepted),
            "runtime authority must remain pinned to the published root",
        );
    });
}
