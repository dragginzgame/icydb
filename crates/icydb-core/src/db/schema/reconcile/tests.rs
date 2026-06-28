use super::{
    startup_expression,
    startup_field_path::{self, SchemaPublicationGate},
};
use crate::{
    db::{
        Db, EntityRuntimeHooks,
        data::{CanonicalRow, DataStore, DecodedDataStoreKey, StructuralRowContract},
        index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore},
        journal::JournalTailStore,
        registry::StoreRegistry,
        schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
            PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
            PersistedSchemaSnapshot, SchemaExpressionIndexRebuildRow,
            SchemaExpressionIndexStagedRebuild, SchemaFieldDefault, SchemaFieldPathIndexRebuildRow,
            SchemaFieldPathIndexRunner, SchemaFieldSlot, SchemaMutationRequest,
            SchemaMutationRunnerInput, SchemaRowLayout, SchemaStore, SchemaVersion,
            compiled_schema_proposal_for_model,
        },
    },
    error::ErrorClass,
    metrics::{metrics_report, metrics_reset_all},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec},
        index::IndexModel,
    },
    testing::{entity_model_from_static, test_memory},
    traits::{EntityKind, EntitySchema, Path},
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
}

static INDEXED_SCHEMA_NAME_INDEX: IndexModel = IndexModel::generated_with_ordinal(
    1,
    "by_name",
    "schema::reconcile::tests::IndexedSchemaEntity::by_name",
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
        RefCell::new(JournalTailStore::init(test_memory(255)));
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
                        255,
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
        SchemaRowLayout::new_with_retired_slots(
            stored_version,
            expected.row_layout().field_to_slot().to_vec(),
            expected.row_layout().retired_field_slots().to_vec(),
        ),
        expected.fields().to_vec(),
        Vec::new(),
    )
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
        "schema::reconcile::tests::IndexedSchemaEntity::ddl_name_idx".to_string(),
        false,
        expected_index.key().clone(),
        None,
    )
}

fn indexed_schema_lower_name_expression_index() -> PersistedIndexSnapshot {
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let name_field = expected
        .fields()
        .iter()
        .find(|field| field.name() == "name")
        .expect("indexed schema fixture should have a name field");
    let source = PersistedIndexFieldPathSnapshot::new(
        name_field.id(),
        name_field.slot(),
        vec!["name".to_string()],
        name_field.kind().clone(),
        name_field.nullable(),
    );
    let expression = PersistedIndexExpressionSnapshot::new(
        PersistedIndexExpressionOp::Lower,
        source.clone(),
        source.kind().clone(),
        source.kind().clone(),
        "expr:v1:LOWER(name)".to_string(),
    );

    PersistedIndexSnapshot::new_sql_ddl(
        2,
        "ddl_lower_name_idx".to_string(),
        "schema::reconcile::tests::IndexedSchemaEntity::ddl_lower_name_idx".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
            Box::new(expression),
        )]),
        None,
    )
}

fn insert_indexed_schema_row(id: u128, name: &str) {
    let id = Ulid::from_u128(id);
    let data_key =
        DecodedDataStoreKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
    let raw_key = data_key.to_raw().expect("test key should encode to raw");
    let row = CanonicalRow::from_generated_entity_for_test(&IndexedSchemaEntity {
        id,
        name: name.to_string(),
    })
    .expect("indexed schema row should encode");
    RECONCILE_DATA_STORE.with_borrow_mut(|store| {
        let _ = store.insert(raw_key, row);
    });
}

fn indexed_schema_field_path_publication_context() -> (
    PersistedSchemaSnapshot,
    PersistedSchemaSnapshot,
    super::SchemaTransitionPlan,
) {
    let stored_without_index = indexed_schema_snapshot_without_indexes();
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });
    insert_indexed_schema_row(15_401, "Ada");

    let plan = super::validate_existing_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        &expected,
    )
    .expect("single field-path index addition should produce a transition plan");

    (stored_without_index, expected, plan)
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
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
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
fn ensure_accepted_schema_snapshot_accepts_append_only_nullable_field() {
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

    let accepted = super::ensure_accepted_schema_snapshot(
        &mut schema_store,
        ADDITIVE_NULLABLE_ENTITY_TAG,
        ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
        &ADDITIVE_NULLABLE_SCHEMA_MODEL,
    )
    .expect("append-only nullable generated field should be accepted");
    let latest = schema_store
        .latest_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG)
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
        PersistedFieldKind::Text { max_len: None },
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
fn ensure_accepted_schema_snapshot_publishes_metadata_only_index_rename() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(240));
    let stored = indexed_schema_snapshot_with_renamed_index("IndexedSchemaEntity|name");
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored)
        .expect("stored renamed-index schema snapshot should encode");

    let accepted = super::ensure_accepted_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect("metadata-only generated index rename should be accepted");
    let latest = schema_store
        .latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        .expect("schema store latest snapshot should decode")
        .expect("schema store should retain accepted renamed-index snapshot");

    assert_eq!(accepted.persisted_snapshot().indexes()[0].name(), "by_name");
    assert_eq!(latest.indexes()[0].name(), "by_name");
    assert_eq!(schema_store.len(), 1);
}

#[test]
fn ensure_accepted_schema_snapshot_preserves_ddl_indexes_during_generated_index_rename() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(239));
    let stored = indexed_schema_snapshot_with_renamed_index_and_extra_indexes(
        "IndexedSchemaEntity|name",
        vec![indexed_schema_ddl_extra_index()],
    );
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored)
        .expect("stored renamed-index schema snapshot should encode");

    let accepted = super::ensure_accepted_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect("generated index rename with extra DDL indexes should be accepted");
    let latest = schema_store
        .latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
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
    metrics_reset_all();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });

    let id = Ulid::from_u128(15_401);
    let data_key =
        DecodedDataStoreKey::try_new::<IndexedSchemaEntity>(id).expect("test key should encode");
    let raw_key = data_key.to_raw().expect("test key should encode to raw");
    let row = CanonicalRow::from_generated_entity_for_test(&IndexedSchemaEntity {
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
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
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
    assert_eq!(counters.ops().accepted_schema_fields(), 2);
}

#[test]
fn reconcile_runtime_schemas_rejects_field_path_index_addition_with_populated_target_index() {
    reset_reconcile_stores();
    metrics_reset_all();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });

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
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
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

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });
    insert_indexed_schema_row(15_401, "Ada");

    RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_building);

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect_err("building physical index store should fail closed before schema publish");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
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
fn reconcile_runtime_schemas_accepts_field_path_index_addition_with_unrelated_index_entries() {
    reset_reconcile_stores();
    metrics_reset_all();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });

    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        let unrelated_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, 99);
        let unrelated_key = IndexKey::empty_with_kind(&unrelated_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let unrelated_entry = IndexEntryValue::presence();
        store.insert(unrelated_key, unrelated_entry);
    });

    let hooks = [EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect("unrelated physical index entries should not block target index addition");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
        .expect("latest schema snapshot should decode")
        .expect("indexed schema snapshot should be published");
    assert_eq!(latest.indexes().len(), 1);
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(store.len(), 1);
        assert_eq!(store.state(), IndexState::Ready);
    });
}

#[test]
fn field_path_startup_index_store_preflight_classifies_target_and_other_entries() {
    reset_reconcile_stores();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let plan = super::validate_existing_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        &expected,
    )
    .expect("single field-path index addition should produce a transition plan");
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let target = supported.target();

    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        let target_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, target.ordinal());
        let target_key = IndexKey::empty_with_kind(&target_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let target_entry = IndexEntryValue::presence();
        store.insert(target_key, target_entry);

        let other_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, target.ordinal() + 1);
        let other_key = IndexKey::empty_with_kind(&other_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let other_entry = IndexEntryValue::presence();
        store.insert(other_key, other_entry);
    });

    let preflight = RECONCILE_INDEX_STORE
        .with_borrow(|store| {
            startup_field_path::field_path_startup_index_store_preflight(
                store,
                IndexedSchemaEntity::ENTITY_TAG,
                target,
                IndexedSchemaEntity::MODEL.path(),
            )
        })
        .expect("preflight should decode canonical index keys");

    assert_eq!(preflight.target_index_entries(), 1);
    assert_eq!(preflight.other_index_entries(), 1);
    assert_eq!(preflight.total_entries(), 2);
}

#[test]
fn expression_index_store_batch_rolls_back_on_post_insert_validation_failure() {
    reset_reconcile_stores();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index)
        .expect("index-free snapshot should be accepted");
    let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &accepted,
    )
    .expect("accepted row contract should build");
    insert_indexed_schema_row(15_403, "Ada");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let rows = startup_field_path::decode_field_path_rebuild_rows(
        raw_rows.as_slice(),
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        row_contract,
    )
    .expect("accepted rows should decode");
    let request = SchemaMutationRequest::from_accepted_expression_index(
        &indexed_schema_lower_name_expression_index(),
    )
    .expect("expression index snapshot should lower to an accepted mutation request");
    let SchemaMutationRequest::AddExpressionIndex { target } = request else {
        panic!("expression index snapshot should produce an expression target");
    };
    let rebuild_rows = rows
        .iter()
        .map(|row| SchemaExpressionIndexRebuildRow::new(row.primary_key_value, &row.slots));
    let staged = SchemaExpressionIndexStagedRebuild::from_rows(
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::ENTITY_TAG,
        target.clone(),
        None,
        rebuild_rows,
    )
    .expect("accepted expression rows should stage");
    assert_eq!(staged.entries().len(), 1);

    let wrong_target_id = IndexId::new(IndexedSchemaEntity::ENTITY_TAG, target.ordinal() + 1);
    let err = RECONCILE_INDEX_STORE
        .with_borrow_mut(|index_store| {
            startup_expression::publish_expression_index_store_batch_for_test(
                index_store,
                IndexedSchemaEntity::MODEL.path(),
                &target,
                &wrong_target_id,
                staged.entries(),
            )
        })
        .expect_err("post-insert validation failure should reject publication");
    assert_runtime_unsupported_diagnostic(&err, "expression index rollback");

    RECONCILE_INDEX_STORE.with_borrow(|store| {
        assert_eq!(
            store.state(),
            IndexState::Ready,
            "failed expression publication should restore planner-visible store state",
        );
        assert_eq!(
            store.len(),
            0,
            "failed expression publication should remove staged physical keys",
        );
    });
}

#[test]
fn field_path_startup_rebuild_gate_accepts_unchanged_rows_and_schema() {
    reset_reconcile_stores();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });
    insert_indexed_schema_row(15_401, "Ada");

    let plan = super::validate_existing_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        &expected,
    )
    .expect("single field-path index addition should produce a transition plan");
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");

    gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
        .expect("unchanged rows and schema should keep startup rebuild gate valid");
}

#[test]
fn field_path_startup_rebuild_gate_rejects_row_changes_before_physical_work() {
    reset_reconcile_stores();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });
    insert_indexed_schema_row(15_401, "Ada");

    let plan = super::validate_existing_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        &expected,
    )
    .expect("single field-path index addition should produce a transition plan");
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");
    insert_indexed_schema_row(15_402, "Grace");

    gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
        .expect_err("row changes after scan should fail closed before physical work");
}

#[test]
fn field_path_startup_rebuild_gate_rejects_schema_changes_before_physical_work() {
    reset_reconcile_stores();

    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_without_index = indexed_schema_snapshot_without_indexes();
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
            .expect("stored index-free schema snapshot should encode");
    });
    insert_indexed_schema_row(15_401, "Ada");

    let plan = super::validate_existing_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        &expected,
    )
    .expect("single field-path index addition should produce a transition plan");
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|store| {
        store
            .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &expected)
            .expect("moved schema snapshot should encode");
    });

    gate.validate_before_physical_work(store, supported.target(), raw_rows.len())
        .expect_err("schema changes after planning should fail closed before physical work");
}

#[test]
fn field_path_startup_publication_decision_publishes_after_runner_and_gate() {
    reset_reconcile_stores();

    let (stored_without_index, expected, plan) = indexed_schema_field_path_publication_context();
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
        .expect("index-free snapshot should be accepted");
    let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &accepted,
    )
    .expect("accepted row contract should build");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");
    let rows = startup_field_path::decode_field_path_rebuild_rows(
        raw_rows.as_slice(),
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        row_contract,
    )
    .expect("accepted rows should decode");
    let input =
        SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
            .expect("runner input should bind accepted snapshots");
    let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
    let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
    let report = RECONCILE_INDEX_STORE
        .with_borrow_mut(|index_store| {
            let rebuild_rows = rows
                .iter()
                .map(|row| SchemaFieldPathIndexRebuildRow::new(row.primary_key_value, &row.slots));
            SchemaFieldPathIndexRunner::run(
                &input,
                IndexedSchemaEntity::ENTITY_TAG,
                supported.target().clone(),
                None,
                rebuild_rows,
                index_store,
                &mut invalidation_sink,
                &mut publication_sink,
            )
        })
        .expect("field-path runner should publish physical work");

    let decision = startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
        store,
        &rebuild_gate,
        supported.target(),
        &report,
    )
    .expect("publishable runner report and valid gate should allow schema publication");
    decision
        .publish_accepted_snapshot(
            store,
            SchemaPublicationGate::startup(IndexedSchemaEntity::ENTITY_TAG),
            &expected,
        )
        .expect("publication decision should write accepted schema");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
        .expect("latest schema snapshot should decode")
        .expect("indexed schema snapshot should be published");
    assert_eq!(latest.indexes().len(), 1);
}

#[test]
fn field_path_startup_publication_decision_rejects_gate_drift_without_schema_publish() {
    reset_reconcile_stores();

    let (stored_without_index, expected, plan) = indexed_schema_field_path_publication_context();
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
        .expect("index-free snapshot should be accepted");
    let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &accepted,
    )
    .expect("accepted row contract should build");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");
    let rows = startup_field_path::decode_field_path_rebuild_rows(
        raw_rows.as_slice(),
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        row_contract,
    )
    .expect("accepted rows should decode");
    let input =
        SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
            .expect("runner input should bind accepted snapshots");
    let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
    let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
    let report = RECONCILE_INDEX_STORE
        .with_borrow_mut(|index_store| {
            let rebuild_rows = rows
                .iter()
                .map(|row| SchemaFieldPathIndexRebuildRow::new(row.primary_key_value, &row.slots));
            SchemaFieldPathIndexRunner::run(
                &input,
                IndexedSchemaEntity::ENTITY_TAG,
                supported.target().clone(),
                None,
                rebuild_rows,
                index_store,
                &mut invalidation_sink,
                &mut publication_sink,
            )
        })
        .expect("field-path runner should publish physical work");
    insert_indexed_schema_row(15_402, "Grace");

    startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
        store,
        &rebuild_gate,
        supported.target(),
        &report,
    )
    .expect_err("row drift after runner should reject schema publication");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
}

#[test]
fn field_path_startup_publication_decision_rejects_physical_store_drift_without_schema_publish() {
    reset_reconcile_stores();

    let (stored_without_index, expected, plan) = indexed_schema_field_path_publication_context();
    let supported = plan
        .supported_developer_physical_path()
        .expect("single field-path index addition should be the supported path");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("reconcile store should be registered");
    let accepted = AcceptedSchemaSnapshot::try_new(stored_without_index.clone())
        .expect("index-free snapshot should be accepted");
    let row_contract = StructuralRowContract::from_accepted_schema_snapshot(
        IndexedSchemaEntity::MODEL.path(),
        &accepted,
    )
    .expect("accepted row contract should build");
    let raw_rows = startup_field_path::field_path_rebuild_raw_rows_for_entity(
        store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
    )
    .expect("indexed rows should scan");
    let rebuild_gate = startup_field_path::StartupFieldPathRebuildGate::from_raw_rows(
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        &stored_without_index,
        raw_rows.as_slice(),
    )
    .expect("startup rebuild gate should capture scanned rows");
    let rows = startup_field_path::decode_field_path_rebuild_rows(
        raw_rows.as_slice(),
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        row_contract,
    )
    .expect("accepted rows should decode");
    let input =
        SchemaMutationRunnerInput::new(&stored_without_index, &expected, plan.execution_plan())
            .expect("runner input should bind accepted snapshots");
    let mut invalidation_sink = startup_field_path::StartupSchemaMutationInvalidationSink;
    let mut publication_sink = startup_field_path::StartupSchemaMutationPublicationSink;
    let report = RECONCILE_INDEX_STORE
        .with_borrow_mut(|index_store| {
            let rebuild_rows = rows
                .iter()
                .map(|row| SchemaFieldPathIndexRebuildRow::new(row.primary_key_value, &row.slots));
            SchemaFieldPathIndexRunner::run(
                &input,
                IndexedSchemaEntity::ENTITY_TAG,
                supported.target().clone(),
                None,
                rebuild_rows,
                index_store,
                &mut invalidation_sink,
                &mut publication_sink,
            )
        })
        .expect("field-path runner should publish physical work");

    let decision = startup_field_path::StartupFieldPathPublicationDecision::from_runner_report(
        store,
        &rebuild_gate,
        supported.target(),
        &report,
    )
    .expect("publishable runner report and valid gate should allow a decision");
    RECONCILE_INDEX_STORE.with_borrow_mut(|store| {
        let target_id = IndexId::new(
            IndexedSchemaEntity::ENTITY_TAG,
            supported.target().ordinal(),
        );
        let extra_key = IndexKey::empty_with_kind(&target_id, IndexKeyKind::User)
            .to_raw()
            .expect("test index key should encode");
        let extra_entry = IndexEntryValue::presence();
        store.insert(extra_key, extra_entry);
    });

    decision
        .publish_accepted_snapshot(
            store,
            SchemaPublicationGate::startup(IndexedSchemaEntity::ENTITY_TAG),
            &expected,
        )
        .expect_err("physical store drift after runner should reject schema publication");

    let latest = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| store.latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG))
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
}

#[test]
fn ensure_accepted_schema_snapshot_rejects_field_path_index_addition_without_runtime_store() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(244));
    metrics_reset_all();

    let stored_without_index = indexed_schema_snapshot_without_indexes();
    schema_store
        .insert_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG, &stored_without_index)
        .expect("stored index-free schema snapshot should encode");

    let err = super::ensure_accepted_schema_snapshot(
        &mut schema_store,
        IndexedSchemaEntity::ENTITY_TAG,
        IndexedSchemaEntity::MODEL.path(),
        IndexedSchemaEntity::MODEL,
    )
    .expect_err("metadata-only reconciliation must not execute physical index addition");

    assert_eq!(err.class, ErrorClass::Unsupported);
    let latest = schema_store
        .latest_persisted_snapshot(IndexedSchemaEntity::ENTITY_TAG)
        .expect("latest schema snapshot should decode")
        .expect("index-free schema snapshot should remain accepted");
    assert_eq!(latest.indexes().len(), 0);
}

#[test]
fn ensure_accepted_schema_snapshot_records_nested_leaf_footprint() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(241));
    metrics_reset_all();

    let accepted = super::ensure_accepted_schema_snapshot(
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
fn ensure_accepted_schema_snapshot_rejects_nested_leaf_drift_as_field_contract() {
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
            vec!["legacy_rank".to_string()],
            PersistedFieldKind::Nat64,
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

    let err = super::ensure_accepted_schema_snapshot(
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
        "0.177 schema-version gate is the primary reconcile bucket while preserving field-contract detail",
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
        "0.177 schema-version gate is the primary rejection bucket while preserving field-contract detail",
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
        "0.177 schema-version gate should reject generated removed-field drift before compatibility publication",
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
        "0.177 schema-version gate should reject generated additive drift before compatibility publication",
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
        "legacy_score".to_string(),
        SchemaFieldSlot::new(2),
        PersistedFieldKind::Nat64,
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
        "0.177 schema-version gate should reject generated removed-field drift before compatibility publication",
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
        "0.177 schema-version gate should reject generated removed-field drift before compatibility publication",
    );
}

#[test]
fn reconcile_runtime_schemas_rejects_newer_schema_snapshot() {
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
    });

    let err = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect_err("schema reconciliation must not ignore newer persisted versions");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_runtime_unsupported_diagnostic(
        &err,
        "newer accepted snapshot should reject through admission gate",
    );
}
