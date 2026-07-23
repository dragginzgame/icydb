use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks, Predicate, QueryError,
        commit::{
            CommitFailpoint, CommitFailpointMode, CommitMarker, CommitRowOp,
            arm_commit_failpoint_for_tests, begin_commit, clear_commit_failpoint_for_tests,
            clear_commit_marker_for_tests, clear_recovery_runtime_state_for_tests,
            commit_marker_present, ensure_recovered, generate_commit_id,
            init_commit_store_for_tests, publish_accepted_schema_candidate,
            publish_accepted_schema_candidate_with_constraint_validation_job_removal,
        },
        data::{
            CanonicalRow, DataStore, DecodedDataStoreKey, emit_raw_row_from_slot_payloads,
            encode_persisted_scalar_slot_payload,
        },
        index::{
            IndexEntryValue, IndexId, IndexKey, IndexKeyKind, IndexState, IndexStore,
            IndexStoreVisit,
        },
        journal::{JournalBatch, JournalRecord, JournalTailStore},
        registry::StoreRegistry,
        schema::{
            AcceptedConstraintKind, AcceptedFieldKind, CandidateSchemaRevision,
            ConstraintActivationState, ConstraintValidationJob, ConstraintValidationPhase,
            ConstraintValidationProgress, FieldId, PersistedFieldOrigin, PersistedFieldSnapshot,
            PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            RowLayoutVersion, SchemaFieldSlot, SchemaHistoricalFill, SchemaIndexId,
            SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaTransitionPlanKind,
            SchemaVersion, compiled_schema_proposal_for_model,
            derive_sql_ddl_field_nullability_persisted_after,
        },
    },
    entity::{EntityDeclaration, EntityKind},
    error::{ConstraintDiagnosticContext, ConstraintDiagnosticKind, ErrorClass},
    metrics::{metrics_report, metrics_reset_all},
    model::{
        entity::{CheckConstraintModel, EntityModel, PrimaryKeyModel},
        field::{
            CompositeCodec, CompositeFieldModel, CompositeShapeModel, FieldDatabaseDefault,
            FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec,
        },
        index::IndexModel,
    },
    testing::{entity_model_from_static, test_memory},
    traits::Path,
    types::{EntityTag, Id, Ulid},
    value::Value,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{cell::RefCell, sync::LazyLock};

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

crate::test_store! {
    ident = SchemaReconcileRelationTargetStore,
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SchemaReconcileRelationTargetEntity {
    id: Ulid,
}

crate::test_entity! {
    ident = SchemaReconcileRelationTargetEntity,
    entity_name = "SchemaReconcileRelationTargetEntity",
    tag = EntityTag::new(0x7265_6c5f_7461_7267),
    store = SchemaReconcileRelationTargetStore,
    canister = SchemaReconcileTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

static ADDITIVE_RELATION_TARGET_DEFAULT_PAYLOAD: &[u8] =
    &[0xFF, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct AdditiveRelationSourceEntity {
    id: Ulid,
    name: String,
    target: Ulid,
}

crate::test_entity! {
    ident = AdditiveRelationSourceEntity,
    entity_name = "AdditiveRelationSourceEntity",
    tag = EntityTag::new(0x6164_645f_7265_6c73),
    store = SchemaReconcileTestStore,
    canister = SchemaReconcileTestCanister,
    version = 2,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            target: Ulid => FieldKind::Relation {
                target_path: SchemaReconcileRelationTargetEntity::PATH,
                target_entity_name: <SchemaReconcileRelationTargetEntity as crate::entity::EntityDeclaration>::MODEL.name(),
                target_entity_tag: SchemaReconcileRelationTargetEntity::ENTITY_TAG,
                target_store_path: SchemaReconcileRelationTargetStore::PATH,
                key_kind: &FieldKind::Ulid,
            },
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_database_default(
                FieldDatabaseDefault::EncodedSlotPayload(
                    ADDITIVE_RELATION_TARGET_DEFAULT_PAYLOAD,
                ),
            ),
        },
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
static NESTED_PROFILE_COMPOSITE_FIELDS: [CompositeFieldModel; 1] =
    [CompositeFieldModel::generated(
        "rank",
        FieldKind::Nat64,
        false,
    )];
static NESTED_PROFILE_COMPOSITE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&NESTED_PROFILE_COMPOSITE_FIELDS);
static NESTED_PROFILE_COMPOSITE_KIND: FieldKind = FieldKind::Composite {
    path: "schema::reconcile::tests::Profile",
    codec: CompositeCodec::StructuralV1,
    shape: &NESTED_PROFILE_COMPOSITE_SHAPE,
};
static NESTED_SCHEMA_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
        "profile",
        NESTED_PROFILE_COMPOSITE_KIND,
        FieldStorageDecode::CatalogValue,
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
static GENERATED_DEFAULT_BEFORE_PAYLOAD: &[u8] = &[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0];
static GENERATED_DEFAULT_AFTER_PAYLOAD: &[u8] = &[0xFF, 0x01, 9, 0, 0, 0, 0, 0, 0, 0];
static GENERATED_DEFAULT_BEFORE_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
        "score",
        FieldKind::Nat64,
        FieldStorageDecode::ByKind,
        false,
        None,
        None,
        FieldDatabaseDefault::EncodedSlotPayload(GENERATED_DEFAULT_BEFORE_PAYLOAD),
        &[],
    ),
];
static GENERATED_DEFAULT_AFTER_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
        "score",
        FieldKind::Nat64,
        FieldStorageDecode::ByKind,
        false,
        None,
        None,
        FieldDatabaseDefault::EncodedSlotPayload(GENERATED_DEFAULT_AFTER_PAYLOAD),
        &[],
    ),
];
static GENERATED_DEFAULT_INDEXES: [&IndexModel; 0] = [];
static GENERATED_DEFAULT_BEFORE_MODEL: EntityModel = EntityModel::generated(
    "schema::reconcile::tests::GeneratedDefaultSchemaEntity",
    "GeneratedDefaultSchemaEntity",
    1,
    &GENERATED_DEFAULT_BEFORE_FIELDS[0],
    0,
    &GENERATED_DEFAULT_BEFORE_FIELDS,
    &GENERATED_DEFAULT_INDEXES,
);
static GENERATED_DEFAULT_AFTER_MODEL: EntityModel = EntityModel::generated(
    "schema::reconcile::tests::GeneratedDefaultSchemaEntity",
    "GeneratedDefaultSchemaEntity",
    2,
    &GENERATED_DEFAULT_AFTER_FIELDS[0],
    0,
    &GENERATED_DEFAULT_AFTER_FIELDS,
    &GENERATED_DEFAULT_INDEXES,
);
const GENERATED_DEFAULT_ENTITY_TAG: EntityTag = EntityTag::new(0x6765_6e64_6465_6661);
thread_local! {
    static RECONCILE_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(252)));
    static RECONCILE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(253)));
    static RECONCILE_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(254)));
    static RECONCILE_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(250)));
    static RECONCILE_RELATION_TARGET_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(245)));
    static RECONCILE_RELATION_TARGET_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(246)));
    static RECONCILE_RELATION_TARGET_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(247)));
    static RECONCILE_RELATION_TARGET_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(248)));
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
            .register_journaled_store(
                SchemaReconcileRelationTargetStore::PATH,
                &RECONCILE_RELATION_TARGET_DATA_STORE,
                &RECONCILE_RELATION_TARGET_INDEX_STORE,
                &RECONCILE_RELATION_TARGET_SCHEMA_STORE,
                &RECONCILE_RELATION_TARGET_JOURNAL_STORE,
                crate::db::StoreAllocationIdentities::new_journaled(
                    crate::db::StoreAllocationIdentity::new(
                        245,
                        "icydb.test.reconcile.relation_target.data.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        246,
                        "icydb.test.reconcile.relation_target.index.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        247,
                        "icydb.test.reconcile.relation_target.schema.v1",
                    ),
                    crate::db::StoreAllocationIdentity::new(
                        248,
                        "icydb.test.reconcile.relation_target.journal.v1",
                    ),
                ),
                crate::db::StoreRuntimeStorageCapabilities::journaled(),
            )
            .expect("schema reconcile relation target store should register");
        registry
    };
}

static RECONCILE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::for_entity::<SchemaReconcileEntity>()];
static RECONCILE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, RECONCILE_RUNTIME_HOOKS);

fn generated_nonempty_name_check() -> &'static Predicate {
    static CHECK: LazyLock<Predicate> =
        LazyLock::new(|| Predicate::ne("name".to_string(), Value::Text(String::new())));

    &CHECK
}

static RECONCILE_CHECKS: [CheckConstraintModel; 1] = [CheckConstraintModel::generated(
    "name_nonempty",
    "name <> ''",
    generated_nonempty_name_check,
)];
static RECONCILE_CHECK_MODEL: EntityModel =
    EntityModel::generated_with_primary_key_model_relations_and_checks(
        SchemaReconcileEntity::MODEL.path(),
        SchemaReconcileEntity::MODEL.name(),
        2,
        PrimaryKeyModel::scalar(SchemaReconcileEntity::MODEL.primary_key()),
        SchemaReconcileEntity::MODEL.primary_key_slot(),
        SchemaReconcileEntity::MODEL.fields(),
        SchemaReconcileEntity::MODEL.indexes(),
        &[],
        &RECONCILE_CHECKS,
    );
static RECONCILE_CHECK_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::new(
        SchemaReconcileEntity::ENTITY_TAG,
        &RECONCILE_CHECK_MODEL,
        SchemaReconcileEntity::PATH,
        SchemaReconcileTestStore::PATH,
        crate::db::relation::validate_delete_relations_for_source::<SchemaReconcileEntity>,
    )];
static RECONCILE_CHECK_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, RECONCILE_CHECK_RUNTIME_HOOKS);
static RECONCILE_UNIQUE_NAME_INDEX: IndexModel = IndexModel::generated_with_ordinal(
    1,
    "unique_name",
    SchemaReconcileTestStore::PATH,
    &["name"],
    true,
);
static RECONCILE_UNIQUE_INDEXES: [&IndexModel; 1] = [&RECONCILE_UNIQUE_NAME_INDEX];
static RECONCILE_UNIQUE_MODEL: EntityModel =
    EntityModel::generated_with_primary_key_model_relations_and_checks(
        SchemaReconcileEntity::MODEL.path(),
        SchemaReconcileEntity::MODEL.name(),
        2,
        PrimaryKeyModel::scalar(SchemaReconcileEntity::MODEL.primary_key()),
        SchemaReconcileEntity::MODEL.primary_key_slot(),
        SchemaReconcileEntity::MODEL.fields(),
        &RECONCILE_UNIQUE_INDEXES,
        &[],
        &[],
    );
static RECONCILE_UNIQUE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::new(
        SchemaReconcileEntity::ENTITY_TAG,
        &RECONCILE_UNIQUE_MODEL,
        SchemaReconcileEntity::PATH,
        SchemaReconcileTestStore::PATH,
        crate::db::relation::validate_delete_relations_for_source::<SchemaReconcileEntity>,
    )];
static RECONCILE_UNIQUE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, RECONCILE_UNIQUE_RUNTIME_HOOKS);
static INDEXED_RECONCILE_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] =
    &[EntityRuntimeHooks::for_entity::<IndexedSchemaEntity>()];
static INDEXED_RECONCILE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, INDEXED_RECONCILE_RUNTIME_HOOKS);
static ADDITIVE_RELATION_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaReconcileTestCanister>] = &[
    EntityRuntimeHooks::for_entity::<AdditiveRelationSourceEntity>(),
    EntityRuntimeHooks::for_entity::<SchemaReconcileRelationTargetEntity>(),
];
static ADDITIVE_RELATION_RECONCILE_DB: Db<SchemaReconcileTestCanister> =
    Db::new_with_hooks(&RECONCILE_STORE_REGISTRY, ADDITIVE_RELATION_RUNTIME_HOOKS);

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

fn reset_reconcile_relation_target_store() {
    RECONCILE_RELATION_TARGET_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    RECONCILE_RELATION_TARGET_DATA_STORE.with_borrow_mut(DataStore::clear);
    RECONCILE_RELATION_TARGET_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });
    RECONCILE_RELATION_TARGET_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
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
        expected.row_layout().clone(),
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
        expected_index.schema_id(),
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
    .with_constraint_catalog(expected.constraint_catalog().clone())
    .with_relations(expected.relations().to_vec())
}

fn indexed_schema_ddl_extra_index() -> PersistedIndexSnapshot {
    let proposal = compiled_schema_proposal_for_model(IndexedSchemaEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let [expected_index] = expected.indexes() else {
        panic!("indexed schema fixture should have one generated index");
    };

    PersistedIndexSnapshot::new_sql_ddl(
        SchemaIndexId::new(u32::from(expected_index.ordinal() + 1))
            .expect("test index identity should be non-zero"),
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

fn index_entry_count_for_generation(entity_tag: EntityTag, generation: u64) -> usize {
    RECONCILE_INDEX_STORE.with_borrow(|store| {
        let mut count = 0usize;
        store
            .visit_entries::<crate::error::InternalError>(|raw, _| {
                let key = IndexKey::try_from_raw(raw)
                    .map_err(|_| crate::error::InternalError::index_invariant())?;
                if key.index_id().entity_tag() == entity_tag
                    && key.index_id().generation() == generation
                {
                    count = count.saturating_add(1);
                }
                Ok(IndexStoreVisit::Continue)
            })
            .expect("candidate index entries should decode");
        count
    })
}

fn relation_target_entry_count_for_generation(entity_tag: EntityTag, generation: u64) -> usize {
    relation_target_keys_for_generation(entity_tag, generation).len()
}

fn relation_target_keys_for_generation(entity_tag: EntityTag, generation: u64) -> Vec<Vec<u8>> {
    RECONCILE_RELATION_TARGET_INDEX_STORE.with_borrow(|store| {
        let mut keys = Vec::new();
        store
            .visit_entries::<crate::error::InternalError>(|raw, _| {
                let key = IndexKey::try_from_raw(raw)
                    .map_err(|_| crate::error::InternalError::index_invariant())?;
                if key.index_id().entity_tag() == entity_tag
                    && key.index_id().generation() == generation
                {
                    keys.push(raw.as_bytes().to_vec());
                }
                Ok(IndexStoreVisit::Continue)
            })
            .expect("candidate relation entries should decode");
        keys
    })
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
fn generated_check_activation_advances_through_durable_validation_and_promotion() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    super::reconcile_runtime_schemas(&RECONCILE_CHECK_DB, RECONCILE_CHECK_RUNTIME_HOOKS)
        .expect("generated check should publish its activation and durable job");

    let (constraint_id, state) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("accepted activation bundle should decode")
            .expect("accepted activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        let [activation] = snapshot.constraint_activations() else {
            panic!("generated addition should own one activation");
        };
        (activation.id(), activation.state())
    });
    assert_eq!(state, ConstraintActivationState::Validating);
    let started = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
        })
        .expect("started validation job should decode")
        .expect("started validation job should exist");
    assert_eq!(started.phase(), ConstraintValidationPhase::Forward);

    super::reconcile_runtime_schemas(&RECONCILE_CHECK_DB, RECONCILE_CHECK_RUNTIME_HOOKS)
        .expect("empty Forward sweep should enter Verify");
    let verifying = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
        })
        .expect("Verify validation job should decode")
        .expect("Verify validation job should exist");
    assert_eq!(verifying.phase(), ConstraintValidationPhase::Verify);
    assert!(verifying.captured_store_revisions().is_some());

    super::reconcile_runtime_schemas(&RECONCILE_CHECK_DB, RECONCILE_CHECK_RUNTIME_HOOKS)
        .expect("stable empty Verify sweep should promote the check");
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("promoted accepted bundle should decode")
            .expect("promoted accepted bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.constraint_activations().is_empty());
        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && constraint.name() == "name_nonempty"
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        }));
        assert!(
            store
                .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
                .expect("promoted job lookup should decode")
                .is_none(),
            "promotion must remove the durable job through the same marker",
        );
    });
}

#[test]
fn generated_check_activation_rejects_removed_proposal_without_changing_durable_state() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    super::reconcile_runtime_schemas(&RECONCILE_CHECK_DB, RECONCILE_CHECK_RUNTIME_HOOKS)
        .expect("generated check should publish its activation and durable job");

    let (activation_before, job_before) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("accepted activation bundle should decode")
            .expect("accepted activation bundle should exist");
        let activation = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist")
            .constraint_activations()[0]
            .clone();
        let job = store
            .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, activation.id())
            .expect("validation job should decode")
            .expect("validation job should exist");
        (activation, job)
    });

    let error = super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect_err(
            "removing a generated proposal must reject until its live activation is explicitly aborted",
        );
    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Store);
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary:
                icydb_diagnostic_code::RuntimeBoundaryCode::GeneratedConstraintActivationStale,
        }),
    );

    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("rejected reconciliation should leave accepted bytes readable")
            .expect("rejected reconciliation should preserve accepted authority");
        let activation_after = &bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should remain present")
            .constraint_activations()[0];
        let job_after = store
            .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, activation_before.id())
            .expect("preserved validation job should decode")
            .expect("preserved validation job should remain present");

        assert_eq!(activation_after, &activation_before);
        assert_eq!(job_after, job_before);
    });
}

#[test]
fn generated_unique_activation_stages_isolated_generation_before_promotion() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_UNIQUE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the unique index");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(2, "Grace");
    super::reconcile_runtime_schemas(&RECONCILE_UNIQUE_DB, RECONCILE_UNIQUE_RUNTIME_HOOKS)
        .expect("unique activation and job should publish");

    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.indexes().is_empty());
        let [candidate] = snapshot.candidate_indexes() else {
            panic!("unique activation should reserve one candidate index");
        };
        let [activation] = snapshot.constraint_activations() else {
            panic!("unique activation should reserve one constraint");
        };
        (activation.id(), candidate.physical_generation())
    });
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        0,
    );

    super::advance_unique_constraint_activation(
        &RECONCILE_UNIQUE_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("Forward should stage the bounded candidate generation");
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        2,
    );
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let job = store
            .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
            .expect("validation job should decode")
            .expect("validation job should exist");
        assert_eq!(job.phase(), ConstraintValidationPhase::Verify);
    });

    assert!(matches!(
        super::advance_unique_constraint_activation(
            &RECONCILE_UNIQUE_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("stable Verify should promote the candidate"),
        ConstraintValidationProgress::Promoted { .. },
    ));
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("promoted bundle should decode")
            .expect("promoted bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("promoted entity should exist");
        assert_eq!(snapshot.indexes().len(), 1);
        assert!(snapshot.candidate_indexes().is_empty());
        assert!(snapshot.constraint_activations().is_empty());
    });
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        2,
    );
}

#[test]
fn generated_unique_activation_retains_duplicate_findings_without_promotion() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_UNIQUE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the unique index");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(2, "Ada");
    super::reconcile_runtime_schemas(&RECONCILE_UNIQUE_DB, RECONCILE_UNIQUE_RUNTIME_HOOKS)
        .expect("unique activation and job should publish");
    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        (
            snapshot.constraint_activations()[0].id(),
            snapshot.candidate_indexes()[0].physical_generation(),
        )
    });

    let ConstraintValidationProgress::Findings { receipt: first, .. } =
        super::advance_unique_constraint_activation(
            &RECONCILE_UNIQUE_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("duplicate candidate keys should produce a retained finding")
    else {
        panic!("duplicate candidate keys must prevent promotion");
    };
    assert_eq!(first.findings().len(), 1);
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        1,
    );
    let ConstraintValidationProgress::Findings {
        receipt: replayed, ..
    } = super::advance_unique_constraint_activation(
        &RECONCILE_UNIQUE_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("unacknowledged duplicate finding should replay")
    else {
        panic!("duplicate finding must remain retained");
    };
    assert_eq!(replayed, first);
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.indexes().is_empty());
        assert_eq!(snapshot.candidate_indexes().len(), 1);
    });
}

#[test]
fn source_delete_removes_its_staged_unique_candidate_entry() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_UNIQUE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the unique index");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(2, "Grace");
    super::reconcile_runtime_schemas(&RECONCILE_UNIQUE_DB, RECONCILE_UNIQUE_RUNTIME_HOOKS)
        .expect("unique activation and job should publish");
    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        (
            snapshot.constraint_activations()[0].id(),
            snapshot.candidate_indexes()[0].physical_generation(),
        )
    });
    super::advance_unique_constraint_activation(
        &RECONCILE_UNIQUE_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("Forward should stage the candidate generation");
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        2,
    );

    let key = DecodedDataStoreKey::try_new::<SchemaReconcileEntity>(Ulid::from_u128(1))
        .expect("test key should encode")
        .to_raw()
        .expect("test key should encode to raw");
    let before = RECONCILE_DATA_STORE
        .with_borrow(|store| store.get(&key))
        .expect("staged source row should exist");
    let schema_fingerprint = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_catalog_selection(
                SchemaReconcileEntity::ENTITY_TAG,
                SchemaReconcileEntity::PATH,
                SchemaReconcileTestStore::PATH,
            )
            .expect("accepted selection should decode")
            .expect("accepted selection should exist")
            .identity()
            .accepted_schema_fingerprint()
    });
    RECONCILE_UNIQUE_DB
        .prepare_row_commit_op(&CommitRowOp::new(
            SchemaReconcileEntity::PATH,
            key,
            Some(before.as_bytes().to_vec()),
            None,
            schema_fingerprint,
        ))
        .expect("source delete should prepare against candidate staging")
        .apply();

    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        1,
    );
}

#[test]
fn unique_activation_blocks_typed_insert_at_the_shared_save_boundary() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_UNIQUE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the unique index");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(3, "Ada");
    super::reconcile_runtime_schemas(&RECONCILE_UNIQUE_DB, RECONCILE_UNIQUE_RUNTIME_HOOKS)
        .expect("unique activation and job should publish");

    let error = DbSession::new(RECONCILE_UNIQUE_DB)
        .insert(SchemaReconcileEntity {
            id: Ulid::from_u128(2),
            name: "Grace".to_string(),
        })
        .expect_err("typed insert must not bypass an incomplete unique candidate");
    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeConflict,
    );
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ConstraintActivationWriteBlocked,
        }),
        "unexpected target-delete diagnostic: {:#?}",
        error.diagnostic(),
    );
    let key = DecodedDataStoreKey::try_new::<SchemaReconcileEntity>(Ulid::from_u128(2))
        .expect("rejected key should encode")
        .to_raw()
        .expect("rejected key should encode to raw");
    assert!(RECONCILE_DATA_STORE.with_borrow(|store| store.get(&key).is_none()));
}

#[test]
fn interrupted_unique_staging_rebuilds_exact_candidate_generation() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_commit_failpoint_for_tests();
    clear_recovery_runtime_state_for_tests(&RECONCILE_UNIQUE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the unique index");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(2, "Grace");
    super::reconcile_runtime_schemas(&RECONCILE_UNIQUE_DB, RECONCILE_UNIQUE_RUNTIME_HOOKS)
        .expect("unique activation and job should publish");
    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        (
            snapshot.constraint_activations()[0].id(),
            snapshot.candidate_indexes()[0].physical_generation(),
        )
    });

    arm_commit_failpoint_for_tests(
        CommitFailpoint::AfterMarkerWrite,
        CommitFailpointMode::ReturnError,
    );
    super::advance_unique_constraint_activation(
        &RECONCILE_UNIQUE_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect_err("interruption after marker write should leave recovery authority");
    assert!(commit_marker_present().expect("marker presence should decode"));
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        0,
    );

    ensure_recovered(&RECONCILE_UNIQUE_DB).expect("unique staging marker should recover");
    assert!(!commit_marker_present().expect("marker presence should decode"));
    assert_eq!(
        index_entry_count_for_generation(SchemaReconcileEntity::ENTITY_TAG, generation),
        2,
    );
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("recovered bundle should decode")
            .expect("recovered bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("recovered entity should exist");
        assert_eq!(snapshot.indexes().len(), 1);
        assert!(snapshot.candidate_indexes().is_empty());
        assert!(snapshot.constraint_activations().is_empty());
        assert!(
            store
                .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
                .expect("recovered job lookup should decode")
                .is_none(),
        );
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the test keeps the complete not-null activation and promotion timeline visible"
)]
fn not_null_activation_gates_and_promotes_the_nullable_accepted_field() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish");
    let store = RECONCILE_DB
        .store_handle(SchemaReconcileTestStore::PATH)
        .expect("test store should be registered");
    let current = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_schema_bundle()
            .expect("accepted bundle should decode")
            .expect("accepted bundle should exist")
    });
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("accepted entity should exist");
    let nullable = derive_sql_ddl_field_nullability_persisted_after(
        before,
        FieldId::new(2),
        true,
        SchemaVersion::new(before.version().get() + 1),
    )
    .expect("nullable accepted-before fixture should derive");
    let mut nullable_entities = current.entity_snapshots().clone();
    nullable_entities.insert(SchemaReconcileEntity::ENTITY_TAG, nullable);
    let nullable_bundle = super::AcceptedSchemaRevisionBundle::new(
        current
            .revision()
            .checked_next()
            .expect("test accepted revision should advance"),
        current.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        nullable_entities,
    )
    .expect("nullable accepted bundle should close");
    let nullable_candidate =
        CandidateSchemaRevision::new(nullable_bundle).expect("nullable candidate should encode");
    publish_accepted_schema_candidate(
        SchemaReconcileTestStore::PATH,
        store,
        current.revision(),
        &nullable_candidate,
    )
    .expect("nullable accepted-before should publish");

    let current = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_schema_bundle()
            .expect("nullable bundle should decode")
            .expect("nullable bundle should exist")
    });
    let root = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_root)
        .expect("accepted root should decode")
        .expect("accepted root should exist");
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("nullable accepted entity should exist");
    let target = before
        .fields()
        .iter()
        .find(|field| field.id() == FieldId::new(2))
        .expect("nullable target field should exist");
    let catalog = before
        .constraint_catalog()
        .clone()
        .with_added_not_null_activation(
            target,
            root.root().fingerprint(),
            root.root()
                .revision()
                .checked_next()
                .expect("activation epoch should advance")
                .get(),
        )
        .expect("not-null activation should reserve identity");
    let constraint_id = catalog.activations()[0].id();
    let mut activation_entities = current.entity_snapshots().clone();
    activation_entities.insert(
        SchemaReconcileEntity::ENTITY_TAG,
        before.clone().with_constraint_catalog(catalog),
    );
    let activation_bundle = super::AcceptedSchemaRevisionBundle::new(
        current
            .revision()
            .checked_next()
            .expect("activation revision should advance"),
        current.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        activation_entities,
    )
    .expect("activation bundle should close");
    let activation_candidate = CandidateSchemaRevision::new(activation_bundle)
        .expect("not-null activation candidate should encode");
    publish_accepted_schema_candidate(
        SchemaReconcileTestStore::PATH,
        store,
        current.revision(),
        &activation_candidate,
    )
    .expect("not-null activation should publish");

    assert!(matches!(
        super::advance_not_null_constraint_activation(
            &RECONCILE_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("not-null validation should start"),
        ConstraintValidationProgress::Started,
    ));
    let _forward = super::advance_not_null_constraint_activation(
        &RECONCILE_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("empty Forward should reach Verify");
    assert!(matches!(
        super::advance_not_null_constraint_activation(
            &RECONCILE_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("empty Verify should promote"),
        ConstraintValidationProgress::Promoted { .. },
    ));

    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("promoted bundle should decode")
            .expect("promoted bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("promoted entity should exist");
        assert!(!snapshot.fields()[1].nullable());
        assert!(snapshot.constraint_activations().is_empty());
        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::NotNull { field_id }
                        if *field_id == FieldId::new(2)
                )
        }));
        assert!(
            store
                .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
                .expect("promoted job lookup should decode")
                .is_none()
        );
    });
}

#[test]
fn generated_check_validation_replays_findings_until_exact_acknowledgement() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    insert_schema_reconcile_row(1, "Ada");
    insert_schema_reconcile_row(2, "");
    super::reconcile_runtime_schemas(&RECONCILE_CHECK_DB, RECONCILE_CHECK_RUNTIME_HOOKS)
        .expect("generated check should publish its activation and durable job");
    let constraint_id = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist")
            .constraint_activations()[0]
            .id()
    });

    let ConstraintValidationProgress::Findings { receipt: first, .. } =
        super::advance_check_constraint_activation(
            &RECONCILE_CHECK_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("Forward page should report the historical violation")
    else {
        panic!("historical violation should retain a finding receipt");
    };
    let [finding] = first.findings() else {
        panic!("one invalid row should produce one finding");
    };
    assert_eq!(finding.field_ids(), &[FieldId::new(2)]);
    assert_eq!(
        finding.error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION.raw(),
    );

    let ConstraintValidationProgress::Findings {
        receipt: replayed, ..
    } = super::advance_check_constraint_activation(
        &RECONCILE_CHECK_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("unacknowledged retry should replay its receipt")
    else {
        panic!("unacknowledged retry must not advance");
    };
    assert_eq!(replayed, first);

    let ConstraintValidationProgress::Findings { receipt: next, .. } =
        super::advance_check_constraint_activation(
            &RECONCILE_CHECK_DB,
            SchemaReconcileEntity::ENTITY_TAG,
            constraint_id,
            Some(first.page_sequence()),
        )
        .expect("acknowledged retry may advance one bounded page")
    else {
        panic!("unrepaired row should be reported again after acknowledgement");
    };
    assert!(next.page_sequence() > first.page_sequence());
    assert_eq!(next.findings(), first.findings());
}

fn insert_schema_reconcile_row(id: u128, name: &str) {
    let entity = SchemaReconcileEntity {
        id: Ulid::from_u128(id),
        name: name.to_string(),
    };
    let raw_key = DecodedDataStoreKey::try_new::<SchemaReconcileEntity>(entity.id)
        .expect("test key should encode")
        .to_raw()
        .expect("test key should encode to raw");
    let raw_row = CanonicalRow::from_entity_with_model_proposal_for_test(&entity)
        .expect("test row should encode")
        .into_raw_row();
    RECONCILE_DATA_STORE.with_borrow_mut(|store| {
        store
            .fold_recovered_journal_put(raw_key, raw_row)
            .expect("test row should enter the canonical base");
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the recovery test keeps one complete activation marker timeline visible"
)]
fn validation_job_markers_recover_start_progress_and_promotion_atomically() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_commit_failpoint_for_tests();
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    super::reconcile_runtime_schemas_before_recovery_rebuild(
        &RECONCILE_CHECK_DB,
        RECONCILE_CHECK_RUNTIME_HOOKS,
    )
    .expect("recovery-side reconciliation should publish only the activation");
    let constraint_id = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        let [activation] = snapshot.constraint_activations() else {
            panic!("generated addition should own one activation");
        };
        assert_eq!(
            activation.state(),
            ConstraintActivationState::EnforcingNewWrites
        );
        activation.id()
    });

    let current = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("Enforcing bundle should decode")
        .expect("Enforcing bundle should exist");
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("accepted entity should exist");
    let validating_catalog = before
        .constraint_catalog()
        .clone()
        .with_validation_started(constraint_id)
        .expect("activation should enter validation");
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(
        SchemaReconcileEntity::ENTITY_TAG,
        before.clone().with_constraint_catalog(validating_catalog),
    );
    let validating_candidate = super::CandidateSchemaRevision::new(
        super::AcceptedSchemaRevisionBundle::new(
            current
                .revision()
                .checked_next()
                .expect("test accepted revision should advance"),
            current.store_path(),
            current.enum_catalog().clone(),
            current.composite_catalog().clone(),
            snapshots,
        )
        .expect("validating bundle should close"),
    )
    .expect("validating candidate should encode");
    let validating_activation = validating_candidate
        .bundle()
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("validating entity should exist")
        .constraint_catalog()
        .activation(constraint_id)
        .expect("validating activation should exist");
    let started_job = ConstraintValidationJob::start(
        SchemaReconcileEntity::ENTITY_TAG,
        SchemaReconcileEntity::PATH.to_string(),
        validating_activation,
        None,
    )
    .expect("validation job should build");
    begin_interrupted_validation_marker(
        vec![
            JournalRecord::accepted_schema_publish(
                SchemaReconcileTestStore::PATH,
                current.revision(),
                validating_candidate.encoded_bundle().to_vec(),
                validating_candidate.encoded_root().to_vec(),
            )
            .expect("validating schema record should build"),
            JournalRecord::constraint_validation_job_put(
                SchemaReconcileTestStore::PATH,
                &started_job,
            )
            .expect("validation job record should build"),
        ],
        false,
    );
    ensure_recovered(&RECONCILE_CHECK_DB).expect("start marker should recover");
    assert_validation_job_state(constraint_id, ConstraintValidationPhase::Verify);

    let mut restarted_job = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
        })
        .expect("Verify job should decode")
        .expect("Verify job should exist");
    restarted_job
        .restart_forward(0, Vec::new())
        .expect("test job should restart at Forward");
    begin_interrupted_validation_marker(
        vec![
            JournalRecord::constraint_validation_job_put(
                SchemaReconcileTestStore::PATH,
                &restarted_job,
            )
            .expect("restarted job record should build"),
        ],
        true,
    );
    ensure_recovered(&RECONCILE_CHECK_DB).expect("job progress marker should recover");
    assert_validation_job_state(constraint_id, ConstraintValidationPhase::Verify);
    let recovered_job = RECONCILE_SCHEMA_STORE
        .with_borrow(|store| {
            store.constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
        })
        .expect("recovered job should decode")
        .expect("recovered job should exist");
    assert_eq!(recovered_job.restarts(), 1);

    let current = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("Verify bundle should decode")
        .expect("Verify bundle should exist");
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("accepted entity should exist");
    let promoted_catalog = before
        .constraint_catalog()
        .clone()
        .with_promoted_activation(constraint_id)
        .expect("validated activation should promote");
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(
        SchemaReconcileEntity::ENTITY_TAG,
        before.clone().with_constraint_catalog(promoted_catalog),
    );
    let promoted_candidate = super::CandidateSchemaRevision::new(
        super::AcceptedSchemaRevisionBundle::new(
            current
                .revision()
                .checked_next()
                .expect("test accepted revision should advance"),
            current.store_path(),
            current.enum_catalog().clone(),
            current.composite_catalog().clone(),
            snapshots,
        )
        .expect("promoted bundle should close"),
    )
    .expect("promoted candidate should encode");
    begin_interrupted_validation_marker(
        vec![
            JournalRecord::accepted_schema_publish(
                SchemaReconcileTestStore::PATH,
                current.revision(),
                promoted_candidate.encoded_bundle().to_vec(),
                promoted_candidate.encoded_root().to_vec(),
            )
            .expect("promoted schema record should build"),
            JournalRecord::constraint_validation_job_delete(
                SchemaReconcileTestStore::PATH,
                SchemaReconcileEntity::ENTITY_TAG,
                constraint_id,
            )
            .expect("job deletion record should build"),
        ],
        false,
    );
    ensure_recovered(&RECONCILE_CHECK_DB).expect("promotion marker should recover");

    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("promoted bundle should decode")
            .expect("promoted bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.constraint_activations().is_empty());
        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        }));
        assert!(
            store
                .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
                .expect("promoted job lookup should decode")
                .is_none(),
        );
    });
}

fn begin_interrupted_validation_marker(records: Vec<JournalRecord>, append_before_recovery: bool) {
    let marker_id = generate_commit_id().expect("test commit id should generate");
    let sequence = RECONCILE_JOURNAL_STORE
        .with_borrow(JournalTailStore::next_mutation_append_sequence)
        .expect("test journal sequence should allocate");
    let batch = JournalBatch::new(marker_id, marker_id, sequence, records)
        .expect("test validation batch should build");
    let marker = CommitMarker::from_parts(marker_id, vec![batch.clone()])
        .expect("test validation marker should build");
    let _unfinished = begin_commit(marker).expect("interrupted validation marker should persist");
    if append_before_recovery {
        RECONCILE_JOURNAL_STORE
            .with_borrow_mut(|store| store.append_batch(&batch))
            .expect("interrupted validation batch should append");
    }
}

#[test]
fn accepted_publication_blocks_unrelated_entity_change_during_live_activation() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    super::reconcile_runtime_schemas_before_recovery_rebuild(
        &RECONCILE_CHECK_DB,
        RECONCILE_CHECK_RUNTIME_HOOKS,
    )
    .expect("recovery-side reconciliation should publish only the activation");

    let current = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("activation bundle should decode")
        .expect("activation bundle should exist");
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("accepted entity should exist");
    let changed = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        "ChangedWhileValidating".to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        before.fields().to_vec(),
        before.indexes().to_vec(),
    )
    .with_constraint_catalog(before.constraint_catalog().clone())
    .with_relations(before.relations().to_vec());
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(SchemaReconcileEntity::ENTITY_TAG, changed);
    let next_revision = current
        .revision()
        .checked_next()
        .expect("test accepted revision should advance");
    let candidate = super::CandidateSchemaRevision::new(
        super::AcceptedSchemaRevisionBundle::new(
            next_revision,
            current.store_path(),
            current.enum_catalog().clone(),
            current.composite_catalog().clone(),
            snapshots,
        )
        .expect("unrelated candidate should be internally representable"),
    )
    .expect("unrelated candidate should encode");

    assert!(
        publish_accepted_schema_candidate(
            SchemaReconcileTestStore::PATH,
            RECONCILE_DB
                .store_handle(SchemaReconcileTestStore::PATH)
                .expect("test store should resolve"),
            current.revision(),
            &candidate,
        )
        .is_err(),
        "accepted publication must own the live-activation schema freeze",
    );
    let retained_revision = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_revision)
        .expect("current accepted revision should decode")
        .expect("current accepted revision should exist");
    assert_eq!(retained_revision, current.revision());
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the abort test keeps marker publication and identity retirement in one scenario"
)]
fn validating_activation_abort_marker_retires_identity_and_job_atomically() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_commit_failpoint_for_tests();
    clear_recovery_runtime_state_for_tests(&RECONCILE_CHECK_DB)
        .expect("recovery runtime state should clear");

    super::reconcile_runtime_schemas(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
        .expect("initial schema should publish without the generated check");
    super::reconcile_runtime_schemas_before_recovery_rebuild(
        &RECONCILE_CHECK_DB,
        RECONCILE_CHECK_RUNTIME_HOOKS,
    )
    .expect("recovery-side reconciliation should publish only the activation");
    let constraint_id = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("activation bundle should decode")
            .expect("activation bundle should exist");
        bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist")
            .constraint_activations()[0]
            .id()
    });
    super::advance_check_constraint_activation(
        &RECONCILE_CHECK_DB,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect("validation job should start");

    let current = RECONCILE_SCHEMA_STORE
        .with_borrow(SchemaStore::current_accepted_schema_bundle)
        .expect("validating bundle should decode")
        .expect("validating bundle should exist");
    let before = current
        .entity_snapshots()
        .get(&SchemaReconcileEntity::ENTITY_TAG)
        .expect("accepted entity should exist");
    let retired_high_water = before.constraint_id_allocator().high_water();
    let aborted_catalog = before
        .constraint_catalog()
        .clone()
        .with_aborted_activation(constraint_id)
        .expect("live activation should abort");
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(
        SchemaReconcileEntity::ENTITY_TAG,
        before.clone().with_constraint_catalog(aborted_catalog),
    );
    let next_revision = current
        .revision()
        .checked_next()
        .expect("test accepted revision should advance");
    let candidate = super::CandidateSchemaRevision::new(
        super::AcceptedSchemaRevisionBundle::new(
            next_revision,
            current.store_path(),
            current.enum_catalog().clone(),
            current.composite_catalog().clone(),
            snapshots,
        )
        .expect("abort bundle should close"),
    )
    .expect("abort candidate should encode");
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        store
            .validate_live_activation_transition(candidate.bundle())
            .expect("abort should be one exact live-activation transition");
        store
            .validate_constraint_validation_job_closure_with_change(
                candidate.bundle(),
                None,
                Some((SchemaReconcileEntity::ENTITY_TAG, constraint_id)),
            )
            .expect("abort candidate and job deletion should close");
    });

    publish_accepted_schema_candidate_with_constraint_validation_job_removal(
        SchemaReconcileTestStore::PATH,
        RECONCILE_CHECK_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("test store should resolve"),
        current.revision(),
        &candidate,
        SchemaReconcileEntity::ENTITY_TAG,
        constraint_id,
    )
    .expect("abort and job removal should publish atomically");

    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("aborted bundle should decode")
            .expect("aborted bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.constraint_activations().is_empty());
        assert_eq!(
            snapshot.constraint_id_allocator().high_water(),
            retired_high_water,
        );
        assert!(
            snapshot
                .constraints()
                .iter()
                .all(|constraint| { constraint.id() != constraint_id })
        );
        assert!(
            store
                .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
                .expect("aborted job lookup should decode")
                .is_none(),
        );
    });
}

fn assert_validation_job_state(
    constraint_id: crate::db::schema::ConstraintId,
    phase: ConstraintValidationPhase,
) {
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let bundle = store
            .current_accepted_schema_bundle()
            .expect("validation bundle should decode")
            .expect("validation bundle should exist");
        let snapshot = bundle
            .entity_snapshots()
            .get(&SchemaReconcileEntity::ENTITY_TAG)
            .expect("accepted entity should exist");
        assert!(snapshot.constraint_activations().iter().any(|activation| {
            activation.id() == constraint_id
                && activation.state() == ConstraintActivationState::Validating
        }));
        let job = store
            .constraint_validation_job(SchemaReconcileEntity::ENTITY_TAG, constraint_id)
            .expect("validation job should decode")
            .expect("validation job should exist");
        assert_eq!(job.phase(), phase);
    });
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
    assert_eq!(
        latest.row_layout().current_version(),
        crate::db::schema::RowLayoutVersion::INITIAL
    );
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
        super::schema_transition_plan_outcome(SchemaTransitionPlanKind::AppendOnlyFields),
        crate::metrics::SchemaTransitionOutcome::AppendOnlyFields,
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

    let catalogs =
        super::build_generated_catalog_candidates(&RECONCILE_DB, RECONCILE_RUNTIME_HOOKS)
            .expect("generated catalog should build")
            .remove(SchemaReconcileTestStore::PATH)
            .expect("store catalog should exist");
    let changed_snapshot = compiled_schema_proposal_for_model(SchemaReconcileEntity::MODEL)
        .initial_persisted_schema_snapshot()
        .clone_with_version(SchemaVersion::new(2));
    super::publish_generated_accepted_schema_bundle(
        &RECONCILE_DB,
        RECONCILE_DB
            .store_handle(SchemaReconcileTestStore::PATH)
            .expect("store should resolve"),
        SchemaReconcileTestStore::PATH,
        catalogs.enum_catalog,
        catalogs.composite_catalog,
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
        current.composite_catalog().clone(),
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
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
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
        latest.row_layout().current_version().get(),
        2,
        "generated addition should allocate one accepted physical layout",
    );
    assert_eq!(latest.row_layout().history_floor().get(), 1);
    assert_eq!(latest.fields()[2].introduced_in_layout().get(), 2);
    assert_eq!(
        latest.fields()[2].historical_fill(),
        &crate::db::schema::SchemaHistoricalFill::Null,
    );
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
    assert_eq!(counters.ops().schema_transition_append_only_fields(), 1);
    assert_eq!(
        counters.ops().schema_transition_rejected_field_contract(),
        0
    );
    assert_eq!(counters.ops().accepted_schema_fields(), 3);
}

#[test]
fn reconcile_staged_schema_snapshot_publishes_generated_default_change_without_layout_change() {
    let mut schema_store = SchemaStore::init_journaled(test_memory(242));
    metrics_reset_all();

    let stored = compiled_schema_proposal_for_model(&GENERATED_DEFAULT_BEFORE_MODEL)
        .initial_persisted_schema_snapshot();
    let stored_layout = stored.row_layout().clone();
    let stored_historical_fill = stored.fields()[1].historical_fill().clone();
    schema_store
        .insert_persisted_snapshot(GENERATED_DEFAULT_ENTITY_TAG, &stored)
        .expect("stored generated-default schema should encode");

    let accepted = super::reconcile_staged_schema_snapshot(
        &mut schema_store,
        GENERATED_DEFAULT_ENTITY_TAG,
        GENERATED_DEFAULT_AFTER_MODEL.path(),
        &GENERATED_DEFAULT_AFTER_MODEL,
    )
    .expect("generated default change should publish as metadata only");
    let latest = schema_store
        .latest_staged_persisted_snapshot(GENERATED_DEFAULT_ENTITY_TAG)
        .expect("latest generated-default snapshot should decode")
        .expect("generated-default snapshot should remain staged");

    assert_eq!(accepted.persisted_snapshot().row_layout(), &stored_layout);
    assert_eq!(latest.row_layout(), &stored_layout);
    assert_eq!(
        latest.fields()[1].historical_fill(),
        &stored_historical_fill,
        "future default changes must not rewrite historical interpretation",
    );
    assert_eq!(
        latest.fields()[1].insert_default(),
        &SchemaInsertDefault::SlotPayload(GENERATED_DEFAULT_AFTER_PAYLOAD.to_vec()),
    );
    let report = metrics_report(None);
    assert_eq!(
        report
            .counters()
            .expect("metadata transition should emit counters")
            .ops()
            .schema_transition_metadata_only_field_default(),
        1,
    );
}

#[test]
fn validate_existing_schema_snapshot_preserves_generated_after_ddl_diagnostic() {
    let expected = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL)
        .initial_persisted_schema_snapshot();
    let generated = &expected.fields()[2];
    let ddl_field = PersistedFieldSnapshot::new_with_write_policy_and_origin(
        generated.id(),
        "ddl_nickname".to_string(),
        generated.slot(),
        generated.kind().clone(),
        generated.nested_leaves().to_vec(),
        true,
        RowLayoutVersion::new(2).expect("test DDL layout should be valid"),
        SchemaInsertDefault::None,
        SchemaHistoricalFill::Null,
        generated.write_policy(),
        PersistedFieldOrigin::SqlDdl,
        generated.storage_decode(),
        generated.leaf_codec(),
    );
    let mut actual_fields = expected.fields()[..2].to_vec();
    actual_fields.push(ddl_field);
    let actual = PersistedSchemaSnapshot::new(
        SchemaVersion::new(1),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            RowLayoutVersion::new(2).expect("test DDL layout should be valid"),
            RowLayoutVersion::INITIAL,
            expected.row_layout().field_to_slot().to_vec(),
        ),
        actual_fields,
    );

    let error = super::validate_existing_schema_snapshot(
        ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
        &actual,
        &expected,
    )
    .expect_err("a generated field must not claim an accepted DDL-owned slot");

    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Store);
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::GeneratedFieldAfterDdlField,
        },),
    );
}

#[test]
fn ordinary_generated_addition_requires_ready_complete_domain_staging() {
    reset_reconcile_stores();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");

    let proposal = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_prefix = PersistedSchemaSnapshot::new(
        SchemaVersion::new(expected.version().get().saturating_sub(1)),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        expected.fields()[..2].to_vec(),
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|schema_store| {
        super::publish_test_accepted_schema_snapshot(
            schema_store,
            ADDITIVE_NULLABLE_ENTITY_TAG,
            ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
            SchemaReconcileTestStore::PATH,
            &ADDITIVE_NULLABLE_SCHEMA_MODEL,
            stored_prefix.clone(),
        )
        .expect("accepted prefix should publish");
    });
    RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_building);

    let hooks = [EntityRuntimeHooks::new(
        ADDITIVE_NULLABLE_ENTITY_TAG,
        &ADDITIVE_NULLABLE_SCHEMA_MODEL,
        ADDITIVE_NULLABLE_SCHEMA_MODEL.path(),
        SchemaReconcileTestStore::PATH,
        crate::db::relation::validate_delete_relations_for_source::<SchemaReconcileEntity>,
    )];
    super::reconcile_runtime_schemas(&RECONCILE_DB, &hooks)
        .expect_err("ordinary additive publication must not bypass a non-ready index domain");

    let accepted = RECONCILE_SCHEMA_STORE
        .with_borrow(|schema_store| {
            schema_store.current_accepted_persisted_snapshot(ADDITIVE_NULLABLE_ENTITY_TAG)
        })
        .expect("accepted prefix should decode")
        .expect("accepted prefix should remain selected");
    assert_eq!(accepted, stored_prefix);
    RECONCILE_INDEX_STORE.with_borrow_mut(IndexStore::mark_ready);
}

fn install_additive_relation_accepted_prefixes() {
    let source_proposal = compiled_schema_proposal_for_model(AdditiveRelationSourceEntity::MODEL);
    let source_expected = source_proposal.initial_persisted_schema_snapshot();
    let source_prefix = PersistedSchemaSnapshot::new(
        SchemaVersion::new(source_expected.version().get().saturating_sub(1)),
        source_expected.entity_path().to_string(),
        source_expected.entity_name().to_string(),
        source_expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        source_expected.fields()[..2].to_vec(),
    );
    RECONCILE_SCHEMA_STORE.with_borrow_mut(|schema_store| {
        super::publish_test_accepted_schema_snapshot(
            schema_store,
            AdditiveRelationSourceEntity::ENTITY_TAG,
            AdditiveRelationSourceEntity::PATH,
            SchemaReconcileTestStore::PATH,
            AdditiveRelationSourceEntity::MODEL,
            source_prefix,
        )
        .expect("accepted relation-source prefix should publish");
    });

    let target_expected =
        compiled_schema_proposal_for_model(SchemaReconcileRelationTargetEntity::MODEL)
            .initial_persisted_schema_snapshot();
    RECONCILE_RELATION_TARGET_SCHEMA_STORE.with_borrow_mut(|schema_store| {
        super::publish_test_accepted_schema_snapshot(
            schema_store,
            SchemaReconcileRelationTargetEntity::ENTITY_TAG,
            SchemaReconcileRelationTargetEntity::PATH,
            SchemaReconcileRelationTargetStore::PATH,
            SchemaReconcileRelationTargetEntity::MODEL,
            target_expected,
        )
        .expect("accepted relation target should publish");
    });
}

fn insert_additive_relation_target_fixture_row_with_id(target_id: u128) {
    let target_id = Ulid::from_u128(target_id);
    let target_key = DecodedDataStoreKey::try_new::<SchemaReconcileRelationTargetEntity>(target_id)
        .expect("relation target key should encode")
        .to_raw()
        .expect("relation target raw key should encode");
    let target_row = CanonicalRow::from_entity_with_model_proposal_for_test(
        &SchemaReconcileRelationTargetEntity { id: target_id },
    )
    .expect("relation target row should encode");
    RECONCILE_RELATION_TARGET_DATA_STORE.with_borrow_mut(|store| {
        store
            .fold_recovered_journal_put(target_key.clone(), target_row.into_raw_row())
            .expect("relation target row should enter the canonical test base");
    });
}

fn insert_additive_relation_target_fixture_row() {
    insert_additive_relation_target_fixture_row_with_id(1);
}

fn insert_additive_relation_source_fixture_row() {
    let source_id = Ulid::from_u128(2);
    let source_key = DecodedDataStoreKey::try_new::<AdditiveRelationSourceEntity>(source_id)
        .expect("relation source key should encode")
        .to_raw()
        .expect("relation source raw key should encode");
    let source_row = emit_raw_row_from_slot_payloads(
        crate::db::schema::RowLayoutVersion::INITIAL,
        2,
        &[
            encode_persisted_scalar_slot_payload(&source_id, "id")
                .expect("source id should encode"),
            encode_persisted_scalar_slot_payload(&"source".to_string(), "name")
                .expect("source name should encode"),
        ],
    )
    .expect("accepted-prefix source row should encode");
    RECONCILE_DATA_STORE.with_borrow_mut(|store| {
        store
            .fold_recovered_journal_put(source_key, source_row.into_raw_row())
            .expect("relation source row should enter the canonical test base");
    });
}

fn insert_additive_relation_fixture_rows() {
    insert_additive_relation_target_fixture_row();
    insert_additive_relation_source_fixture_row();
}

#[test]
fn generated_additive_relation_activation_retains_missing_target_finding() {
    reset_reconcile_stores();
    reset_reconcile_relation_target_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("recovery runtime state should clear");
    install_additive_relation_accepted_prefixes();
    insert_additive_relation_source_fixture_row();

    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect("candidate relation and its durable validation job should publish");

    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let accepted = store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("candidate relation source should decode")
            .expect("candidate relation source should exist");
        let [candidate] = accepted.candidate_relations() else {
            panic!("generated addition should reserve one candidate relation");
        };
        let [activation] = accepted.constraint_activations() else {
            panic!("generated addition should own one relation activation");
        };
        assert_eq!(accepted.fields().len(), 3);
        assert!(accepted.relations().is_empty());
        assert_eq!(activation.state(), ConstraintActivationState::Validating);
        (activation.id(), candidate.physical_generation())
    });
    let ConstraintValidationProgress::Findings { receipt, .. } =
        super::advance_relation_constraint_activation(
            &ADDITIVE_RELATION_RECONCILE_DB,
            AdditiveRelationSourceEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("missing target should produce a retained validation finding")
    else {
        panic!("missing relation target must prevent promotion");
    };
    assert_eq!(receipt.findings().len(), 1);
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        0,
    );
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let accepted = store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("candidate relation source should remain readable")
            .expect("candidate relation source should remain selected");
        assert!(accepted.relations().is_empty());
        assert_eq!(accepted.candidate_relations().len(), 1);
        assert_eq!(accepted.constraint_activations().len(), 1);
        assert!(
            store
                .constraint_validation_job(AdditiveRelationSourceEntity::ENTITY_TAG, constraint_id,)
                .expect("candidate relation job should decode")
                .and_then(|job| job.last_receipt().cloned())
                .is_some(),
        );
    });
    assert_eq!(RECONCILE_DATA_STORE.with_borrow(DataStore::len), 1);
    assert_eq!(
        RECONCILE_RELATION_TARGET_DATA_STORE.with_borrow(DataStore::len),
        0,
    );
    assert!(
        !commit_marker_present().expect("commit marker probe should succeed"),
        "completed candidate publication must leave no pending marker",
    );
}

#[test]
fn generated_additive_relation_activation_stages_then_promotes() {
    reset_reconcile_stores();
    reset_reconcile_relation_target_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("recovery runtime state should clear");
    install_additive_relation_accepted_prefixes();
    insert_additive_relation_fixture_rows();

    arm_commit_failpoint_for_tests(
        CommitFailpoint::BeforeMarkerWrite,
        CommitFailpointMode::ReturnError,
    );
    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect_err("marker rejection must keep the relation candidate unpublished");
    assert_eq!(
        RECONCILE_RELATION_TARGET_INDEX_STORE.with_borrow(IndexStore::len),
        0,
        "rejected candidate publication must not apply reverse relation membership",
    );
    let accepted_before_retry = RECONCILE_SCHEMA_STORE
        .with_borrow(|schema_store| {
            schema_store
                .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
        })
        .expect("accepted relation-source prefix should decode")
        .expect("accepted relation-source prefix should remain selected");
    assert_eq!(accepted_before_retry.fields().len(), 2);

    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect("additive relation activation and its validation job should publish");

    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let accepted = store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("candidate relation source should decode")
            .expect("candidate relation source should exist");
        let [candidate] = accepted.candidate_relations() else {
            panic!("generated addition should reserve one candidate relation");
        };
        let [activation] = accepted.constraint_activations() else {
            panic!("generated addition should own one relation activation");
        };
        assert!(accepted.relations().is_empty());
        (activation.id(), candidate.physical_generation())
    });
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        0,
        "candidate generation stays isolated until its bounded Forward page",
    );
    assert!(matches!(
        super::advance_relation_constraint_activation(
            &ADDITIVE_RELATION_RECONCILE_DB,
            AdditiveRelationSourceEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("Forward should stage the candidate reverse generation"),
        ConstraintValidationProgress::Advanced {
            phase: ConstraintValidationPhase::Verify,
            rows_scanned: 1,
        },
    ));
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        1,
    );
    assert!(matches!(
        super::advance_relation_constraint_activation(
            &ADDITIVE_RELATION_RECONCILE_DB,
            AdditiveRelationSourceEntity::ENTITY_TAG,
            constraint_id,
            None,
        )
        .expect("stable Verify should promote the candidate relation"),
        ConstraintValidationProgress::Promoted { .. },
    ));
    let accepted = RECONCILE_SCHEMA_STORE
        .with_borrow(|schema_store| {
            schema_store
                .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
        })
        .expect("accepted relation-source schema should decode")
        .expect("accepted relation-source schema should exist");
    assert_eq!(accepted.fields().len(), 3);
    assert_eq!(accepted.relations().len(), 1);
    assert!(accepted.candidate_relations().is_empty());
    assert!(accepted.constraint_activations().is_empty());
}

#[test]
fn generated_relation_activation_dual_writes_live_source_lifecycle() {
    reset_reconcile_stores();
    reset_reconcile_relation_target_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("recovery runtime state should clear");
    install_additive_relation_accepted_prefixes();
    insert_additive_relation_target_fixture_row_with_id(1);
    insert_additive_relation_target_fixture_row_with_id(2);
    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect("candidate relation and its durable validation job should publish");
    let generation = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("candidate relation source should decode")
            .expect("candidate relation source should exist")
            .candidate_relations()[0]
            .physical_generation()
    });

    let session = DbSession::new(ADDITIVE_RELATION_RECONCILE_DB);
    let source_id = Ulid::from_u128(3);
    session
        .insert(AdditiveRelationSourceEntity {
            id: source_id,
            name: "source".to_string(),
            target: Ulid::from_u128(1),
        })
        .unwrap_or_else(|error| {
            panic!(
                "live insert should validate and stage the candidate relation: {:#?}",
                error.diagnostic()
            )
        });
    let before_update =
        relation_target_keys_for_generation(AdditiveRelationSourceEntity::ENTITY_TAG, generation);
    assert_eq!(before_update.len(), 1);

    session
        .update(AdditiveRelationSourceEntity {
            id: source_id,
            name: "source".to_string(),
            target: Ulid::from_u128(2),
        })
        .expect("live relation update should move candidate reverse membership");
    let after_update =
        relation_target_keys_for_generation(AdditiveRelationSourceEntity::ENTITY_TAG, generation);
    assert_eq!(after_update.len(), 1);
    assert_ne!(after_update, before_update);

    assert_eq!(
        session
            .delete::<AdditiveRelationSourceEntity>()
            .by_id(Id::from_key(source_id))
            .execute()
            .expect("source delete should remove candidate reverse membership"),
        1,
    );
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        0,
    );
}

#[test]
fn generated_relation_activation_blocks_target_delete_until_promotion() {
    reset_reconcile_stores();
    reset_reconcile_relation_target_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_recovery_runtime_state_for_tests(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("recovery runtime state should clear");
    install_additive_relation_accepted_prefixes();
    insert_additive_relation_target_fixture_row();
    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect("candidate relation and its durable validation job should publish");

    let target_id = Ulid::from_u128(1);
    let error = DbSession::new(ADDITIVE_RELATION_RECONCILE_DB)
        .delete::<SchemaReconcileRelationTargetEntity>()
        .by_id(Id::from_key(target_id))
        .execute()
        .expect_err("target delete must not bypass an incomplete reverse generation");
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ConstraintActivationWriteBlocked,
        }),
    );
    let QueryError::Execute(execute) = &error else {
        panic!("candidate relation barrier should preserve execution authority");
    };
    let diagnostic = execute
        .as_internal()
        .constraint_diagnostic()
        .expect("candidate relation barrier should retain accepted identity");
    assert_ne!(diagnostic.constraint_id(), 0);
    assert!(!diagnostic.constraint_name().is_empty());
    assert_eq!(
        diagnostic.constraint_kind(),
        ConstraintDiagnosticKind::Relation
    );
    assert_eq!(diagnostic.entity(), AdditiveRelationSourceEntity::PATH);
    assert_eq!(diagnostic.primary_key(), None);
    assert_eq!(diagnostic.field_paths(), &["target".to_string()]);
    assert_eq!(
        diagnostic.context(),
        ConstraintDiagnosticContext::WriteAdmission
    );
    let target_key = DecodedDataStoreKey::try_new::<SchemaReconcileRelationTargetEntity>(target_id)
        .expect("target key should encode")
        .to_raw()
        .expect("target key should encode to raw");
    assert!(
        RECONCILE_RELATION_TARGET_DATA_STORE.with_borrow(|store| store.get(&target_key).is_some())
    );
}

#[test]
fn interrupted_relation_staging_rebuilds_exact_candidate_generation() {
    reset_reconcile_stores();
    reset_reconcile_relation_target_store();
    RECONCILE_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    init_commit_store_for_tests().expect("commit store should initialize");
    clear_commit_marker_for_tests().expect("commit marker should clear");
    clear_commit_failpoint_for_tests();
    clear_recovery_runtime_state_for_tests(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("recovery runtime state should clear");
    install_additive_relation_accepted_prefixes();
    insert_additive_relation_fixture_rows();
    super::reconcile_runtime_schemas(
        &ADDITIVE_RELATION_RECONCILE_DB,
        ADDITIVE_RELATION_RUNTIME_HOOKS,
    )
    .expect("candidate relation and its durable validation job should publish");
    let (constraint_id, generation) = RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let accepted = store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("candidate relation source should decode")
            .expect("candidate relation source should exist");
        (
            accepted.constraint_activations()[0].id(),
            accepted.candidate_relations()[0].physical_generation(),
        )
    });

    arm_commit_failpoint_for_tests(
        CommitFailpoint::AfterMarkerWrite,
        CommitFailpointMode::ReturnError,
    );
    super::advance_relation_constraint_activation(
        &ADDITIVE_RELATION_RECONCILE_DB,
        AdditiveRelationSourceEntity::ENTITY_TAG,
        constraint_id,
        None,
    )
    .expect_err("interruption after marker write should leave recovery authority");
    assert!(commit_marker_present().expect("marker presence should decode"));
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        0,
    );

    ensure_recovered(&ADDITIVE_RELATION_RECONCILE_DB)
        .expect("relation staging marker should recover");
    assert!(!commit_marker_present().expect("marker presence should decode"));
    assert_eq!(
        relation_target_entry_count_for_generation(
            AdditiveRelationSourceEntity::ENTITY_TAG,
            generation,
        ),
        1,
    );
    RECONCILE_SCHEMA_STORE.with_borrow(|store| {
        let accepted = store
            .current_accepted_persisted_snapshot(AdditiveRelationSourceEntity::ENTITY_TAG)
            .expect("recovered relation source should decode")
            .expect("recovered relation source should exist");
        assert_eq!(accepted.relations().len(), 1);
        assert!(accepted.candidate_relations().is_empty());
        assert!(accepted.constraint_activations().is_empty());
        assert!(
            store
                .constraint_validation_job(AdditiveRelationSourceEntity::ENTITY_TAG, constraint_id,)
                .expect("recovered relation job lookup should decode")
                .is_none(),
            "recovery-owned startup Verify should promote and remove the durable job",
        );
    });
}

#[test]
fn valid_version_bump_still_rejects_unlowered_additive_layout() {
    metrics_reset_all();

    let proposal = compiled_schema_proposal_for_model(&ADDITIVE_NULLABLE_SCHEMA_MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_version = SchemaVersion::new(expected.version().get().saturating_sub(1));
    let stored_prefix = PersistedSchemaSnapshot::new(
        stored_version,
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        expected.fields()[..2].to_vec(),
    );
    let mut unsupported_fields = expected.fields().to_vec();
    unsupported_fields[2] = PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "nickname".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        false,
        SchemaInsertDefault::None,
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
    .expect_err("valid version bump must not publish unlowered additive layout facts");

    assert_runtime_unsupported_diagnostic(
        &err,
        "valid N+1 schema version bump should reach row-layout rejection",
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
        0,
        "an unlowered proposal must not be classified as an accepted field contract",
    );
    assert_eq!(counters.ops().schema_transition_rejected_row_layout(), 1);
    assert_eq!(
        counters.ops().schema_reconcile_rejected_schema_version(),
        0,
        "schema-version admission should not own valid-bump compatibility failures",
    );
    assert_eq!(counters.ops().schema_reconcile_rejected_other(), 0);
    assert_eq!(counters.ops().schema_reconcile_rejected_row_layout(), 1);
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
    stored_fields[1] = PersistedFieldSnapshot::new_initial(
        profile.id(),
        profile.name().to_string(),
        profile.slot(),
        profile.kind().clone(),
        vec![PersistedNestedLeafSnapshot::new(
            vec!["removed_rank".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        )],
        profile.nullable(),
        profile.insert_default().clone(),
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
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
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
    stored_fields.push(PersistedFieldSnapshot::new_initial(
        FieldId::new(3),
        "removed_score".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    ));
    let stored_with_removed_field = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
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
    let newer_row_layout = expected.row_layout().clone();
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
    let staged_row_layout = accepted.row_layout().clone();
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
