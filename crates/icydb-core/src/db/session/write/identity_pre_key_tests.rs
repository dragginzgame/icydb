mod commit_identity_tests;
#[cfg(feature = "sql")]
mod grouped_count_tests;
#[cfg(feature = "sql")]
mod historical_scalar_tests;
#[cfg(feature = "sql")]
mod historical_update_tests;
mod index_readiness_tests;
#[cfg(feature = "sql")]
mod mutation_job_authority_tests;
mod nested_relation_tests;
#[cfg(feature = "sql")]
mod raw_order_tests;
mod replacement_key_tests;
mod replay_construction_tests;
mod result_boundary_tests;
#[cfg(feature = "sql")]
mod schema_publication_tests;
mod storage_report_tests;

use super::DynamicTypedEntityBinding;
use super::{
    AcceptedMutationIntentPatch, AcceptedStructuralMutation, AcceptedStructuralMutationPacking,
    AcceptedStructuralMutationStagedAdmission, AcceptedStructuralMutationTarget, DbSession,
    DynamicMutation, DynamicStructuralPatch, DynamicTypedMutation, DynamicWriteCell, FieldSlot,
    MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS, MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES,
    MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES, MutationProgressRecordOp, TypedEntityDescriptor,
    TypedFieldType, add_structural_mutation_staged_bytes, admit_structural_mutation_staged_charge,
    checked_pre_key_candidate_count, insert_key_exists_after_generation,
    structural_mutation_staged_charge, validate_structural_mutation_result_bytes,
};
#[cfg(feature = "sql")]
use crate::db::data::DecodedDataStoreKey;
use crate::db::mutation_job::{MutationJobRecord, MutationJobTransition};
#[cfg(feature = "sql")]
use crate::db::resource::budget::{
    HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
    with_execution_budget_for_tests, with_query_execution_budget_for_tests,
};
#[cfg(feature = "sql")]
use crate::db::{
    CompareProofAndAdvanceError, ExhaustiveReadError, MutationJobError, MutationJobRestartReason,
    PrimaryKeyComponent, PrimaryKeyValue, RawDataStoreKey, ReadSetRevisionError,
    ResumableJobAdvance, ResumableJobAdvanceRequest, ResumableJobAdvanceStatus, ResumableJobError,
    ResumableJobId, ResumableJobIdempotencyKey, ResumableJobStatus, asc,
};
use crate::db::{DynamicQuery, QueryExecutionError};
use crate::{
    db::{
        GeneratedStartupDriverStep, MutationJobAdvanceRequest, MutationJobId,
        MutationJobIdempotencyKey, MutationJobPhase, MutationJobStatus, TypedFieldDescriptor,
        commit::{
            database_incarnation_id, forget_recovered_domain_for_tests,
            install_startup_recovery_wakeup,
        },
        data::DataStore,
        drive_generated_startup_recovery_page,
        executor::{MutationCommitInterruption, interrupt_next_mutation_commit_for_tests},
        index::{IndexId, IndexKey, IndexKeyKind, IndexStore, IndexStoreVisit},
        integrity::{
            InsertMutationJobResult, PhysicalUnitCheckpoint, QuickIntegrityStatus,
            RowInspectionLimits, execute_quick_integrity, execute_row_integrity_page,
            with_mutation_progress_store,
        },
        journal::{
            JournalBatch, JournalRecord, JournalSequence, JournalTailControl, JournalTailStore,
            encode_journal_batch,
        },
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreHandle, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedSchemaRevision, ConstraintId,
            FieldId, FieldInsertGeneration, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot, RelationId, ScalarCodec,
            SchemaFieldSlot, SchemaFieldWritePolicy, SchemaIndexId, SchemaInsertDefault,
            SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_with_field_bindings_for_tests,
            cardinality_build::{
                CardinalityBuildAuthority, CardinalityGenerationPageOutcome,
                drive_cardinality_generation_page,
            },
            cardinality_generation::{CardinalityGenerationHeader, CardinalityGenerationState},
        },
        write_context::MutationMode,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    testing::test_memory,
    traits::{CanisterKind, Path},
    types::{EntityTag, Timestamp},
    value::{InputValue, OutputValue, Value},
};
use icydb_schema::{FieldSourceKey, ScalarType};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};

const STORE_PATH: &str = "session::write::identity_pre_key_tests::Store";
const ENTITY_SOURCE: &str = "session::write::identity_pre_key_tests::Entity";
const ID_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::id";
const PAYLOAD_SOURCE: &str = "session::write::identity_pre_key_tests::Entity::payload";
const ENTITY_NAME: &str = "IdentityRow";
const ENTITY_TAG: EntityTag = EntityTag::new(93);
const TYPED_DESCRIPTOR: TypedEntityDescriptor = TypedEntityDescriptor::new(
    ENTITY_SOURCE,
    &[ID_SOURCE],
    &[
        TypedFieldDescriptor::new(ID_SOURCE, TypedFieldType::Scalar(ScalarType::Nat64), false),
        TypedFieldDescriptor::new(
            PAYLOAD_SOURCE,
            TypedFieldType::Scalar(ScalarType::Nat64),
            false,
        ),
    ],
);
const SECOND_ENTITY_SOURCE: &str = "session::write::identity_pre_key_tests::SecondEntity";
const SECOND_ID_SOURCE: &str = "session::write::identity_pre_key_tests::SecondEntity::id";
const SECOND_PAYLOAD_SOURCE: &str = "session::write::identity_pre_key_tests::SecondEntity::payload";
const SECOND_TARGET_SOURCE: &str =
    "session::write::identity_pre_key_tests::SecondEntity::target_id";
const SECOND_ENTITY_NAME: &str = "SecondIdentityRow";
const SECOND_ENTITY_TAG: EntityTag = EntityTag::new(96);
const THIRD_ENTITY_SOURCE: &str = "session::write::identity_pre_key_tests::ThirdEntity";
const THIRD_ID_SOURCE: &str = "session::write::identity_pre_key_tests::ThirdEntity::id";
const THIRD_PAYLOAD_SOURCE: &str = "session::write::identity_pre_key_tests::ThirdEntity::payload";
const THIRD_ENTITY_NAME: &str = "ThirdIdentityRow";
const THIRD_ENTITY_TAG: EntityTag = EntityTag::new(97);
const JOURNALED_STORE_PATH: &str = "session::write::identity_pre_key_tests::JournaledStore";
const UNRELATED_STORE_PATH: &str = "session::write::identity_pre_key_tests::UnrelatedStore";

fn batch_rows(results: &[crate::db::DynamicMutationResult]) -> Vec<Vec<OutputValue>> {
    results
        .iter()
        .flat_map(|result| result.rows.iter().cloned())
        .collect()
}

struct TestCanister;

impl Path for TestCanister {
    const PATH: &'static str = "session::write::identity_pre_key_tests::Canister";
}

impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(45)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.identity_pre_key_tests.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(49)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.identity_pre_key_tests.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(46)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.identity_pre_key_tests.integrity.progress.v1";
}

thread_local! {
    static STARTUP_WAKEUPS: Cell<u32> = const { Cell::new(0) };
    static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
    static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
    static SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static UNRELATED_DATA_STORE: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static UNRELATED_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static UNRELATED_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            STORE_PATH,
            &DATA_STORE,
            &INDEX_STORE,
            &SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("identity pre-key test store should register");
        registry.register_store(
            UNRELATED_STORE_PATH,
            &UNRELATED_DATA_STORE,
            &UNRELATED_INDEX_STORE,
            &UNRELATED_SCHEMA_STORE,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("unrelated identity test store should register");
        registry
    };
    static JOURNALED_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(186)));
    static JOURNALED_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(187)));
    static JOURNALED_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(188)));
    static JOURNALED_TAIL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(189)));
    static JOURNALED_STORE_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            JOURNALED_STORE_PATH,
            &JOURNALED_DATA_STORE,
            &JOURNALED_INDEX_STORE,
            &JOURNALED_SCHEMA_STORE,
            &JOURNALED_TAIL_STORE,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(186, "icydb.test.identity_range.data.v1"),
                StoreAllocationIdentity::new(187, "icydb.test.identity_range.index.v1"),
                StoreAllocationIdentity::new(188, "icydb.test.identity_range.schema.v1"),
                StoreAllocationIdentity::new(189, "icydb.test.identity_range.journal.v1"),
            ),
            StoreRuntimeStorageCapabilities::journaled(),
        ).expect("identity range journaled store should register");
        registry
    };
}

fn record_startup_wakeup() {
    STARTUP_WAKEUPS.with(|wakeups| wakeups.set(wakeups.get().saturating_add(1)));
}

struct JournaledTestCanister;

impl Path for JournaledTestCanister {
    const PATH: &'static str = "session::write::identity_pre_key_tests::JournaledCanister";
}

impl CanisterKind for JournaledTestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(190)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.identity_range_tests.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(192)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.identity_range_tests.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(191)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.identity_range_tests.integrity.progress.v1";
}

fn source_key(source: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(source).expect("identity test field source should admit")
}

fn identity_snapshot(store_path: &str, payload_unique: bool) -> PersistedSchemaSnapshot {
    identity_snapshot_for_entity(
        store_path,
        payload_unique,
        false,
        false,
        ENTITY_SOURCE,
        ENTITY_NAME,
        None,
    )
}

fn identity_snapshot_with_nullable_payload(store_path: &str) -> PersistedSchemaSnapshot {
    identity_snapshot_for_entity(
        store_path,
        false,
        false,
        true,
        ENTITY_SOURCE,
        ENTITY_NAME,
        None,
    )
}

fn identity_snapshot_with_payload_index(
    store_path: &str,
    payload_unique: bool,
    composite: bool,
) -> PersistedSchemaSnapshot {
    identity_snapshot_for_entity(
        store_path,
        payload_unique,
        composite,
        false,
        ENTITY_SOURCE,
        ENTITY_NAME,
        None,
    )
}

fn identity_snapshot_for_entity(
    store_path: &str,
    payload_unique: bool,
    composite: bool,
    payload_nullable: bool,
    entity_source: &str,
    entity_name: &str,
    relation_target: Option<&str>,
) -> PersistedSchemaSnapshot {
    let mut fields = vec![
        PersistedFieldSnapshot::new_initial_with_write_policy(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            SchemaFieldWritePolicy::from_model_policies(
                Some(FieldInsertGeneration::Identity),
                None,
            ),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
        PersistedFieldSnapshot::new_initial(
            FieldId::new(2),
            "payload".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            payload_nullable,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ),
    ];
    if relation_target.is_some() {
        fields.push(PersistedFieldSnapshot::new_initial(
            FieldId::new(3),
            "target_id".to_string(),
            SchemaFieldSlot::new(2),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ));
    }
    let mut index_fields = vec![PersistedIndexFieldPathSnapshot::new(
        FieldId::new(2),
        SchemaFieldSlot::new(1),
        vec!["payload".to_string()],
        AcceptedFieldKind::Nat64,
        payload_nullable,
    )];
    if composite {
        index_fields.push(PersistedIndexFieldPathSnapshot::new(
            FieldId::new(1),
            SchemaFieldSlot::new(0),
            vec!["id".to_string()],
            AcceptedFieldKind::Nat64,
            false,
        ));
    }
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        entity_source.to_string(),
        entity_name.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("identity test index ID should admit"),
            1,
            if composite {
                "by_payload_id".to_string()
            } else {
                "by_payload".to_string()
            },
            store_path.to_string(),
            payload_unique,
            PersistedIndexKeySnapshot::from_field_paths(index_fields),
            None,
        )],
    );
    let Some(relation_target) = relation_target else {
        return snapshot;
    };
    let snapshot = snapshot.with_relations(vec![PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).expect("mixed recovery relation identity should be non-zero"),
        "target".to_string(),
        relation_target.to_string(),
        vec![FieldId::new(3)],
    )]);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .expect("mixed recovery relation constraints should close");
    snapshot.with_constraint_catalog(constraints)
}

fn initialize() -> DbSession<TestCanister> {
    initialize_with_snapshot(identity_snapshot(STORE_PATH, false))
}

fn initialize_with_composite_payload_index() -> DbSession<TestCanister> {
    initialize_with_snapshot(identity_snapshot_with_payload_index(
        STORE_PATH, false, true,
    ))
}

fn initialize_with_snapshot(snapshot: PersistedSchemaSnapshot) -> DbSession<TestCanister> {
    DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    UNRELATED_DATA_STORE.with(|store| *store.borrow_mut() = DataStore::init_heap());
    UNRELATED_INDEX_STORE.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    UNRELATED_SCHEMA_STORE.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    let session = DbSession::<TestCanister>::new(
        &STORE_REGISTRY,
        &crate::db::RequestExecutionRoot::__new_runtime_root(),
    );
    session
        .db
        .drive_startup_recovery_page()
        .expect("identity pre-key test database should initialize");
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(ENTITY_TAG, snapshot)]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
        ]),
    );
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("identity pre-key test store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("identity candidate should publish with explicit zero state");
    session
}

fn initialize_journaled_with_root_and_payload_uniqueness(
    payload_unique: bool,
) -> (
    DbSession<JournaledTestCanister>,
    crate::db::RequestExecutionRoot,
) {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
    session
        .db
        .drive_startup_recovery_page()
        .expect("journaled identity database should initialize");
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(
            ENTITY_TAG,
            identity_snapshot(JOURNALED_STORE_PATH, payload_unique),
        )]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
        ]),
    );
    let store = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled identity store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("journaled identity candidate should publish");
    (session, root)
}

fn initialize_journaled_with_root() -> (
    DbSession<JournaledTestCanister>,
    crate::db::RequestExecutionRoot,
) {
    initialize_journaled_with_root_and_payload_uniqueness(false)
}

fn initialize_journaled_multi_entity() -> DbSession<JournaledTestCanister> {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = DbSession::<JournaledTestCanister>::new(&JOURNALED_STORE_REGISTRY, &root);
    session
        .db
        .drive_startup_recovery_page()
        .expect("multi-entity journaled database should initialize");
    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([
            (ENTITY_TAG, identity_snapshot(JOURNALED_STORE_PATH, false)),
            (
                SECOND_ENTITY_TAG,
                identity_snapshot_for_entity(
                    JOURNALED_STORE_PATH,
                    false,
                    false,
                    false,
                    SECOND_ENTITY_SOURCE,
                    SECOND_ENTITY_NAME,
                    Some(ENTITY_SOURCE),
                ),
            ),
            (
                THIRD_ENTITY_TAG,
                identity_snapshot_for_entity(
                    JOURNALED_STORE_PATH,
                    false,
                    false,
                    false,
                    THIRD_ENTITY_SOURCE,
                    THIRD_ENTITY_NAME,
                    None,
                ),
            ),
        ]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_ID_SOURCE)),
                FieldId::new(1),
            ),
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_PAYLOAD_SOURCE)),
                FieldId::new(2),
            ),
            (
                (SECOND_ENTITY_TAG, source_key(SECOND_TARGET_SOURCE)),
                FieldId::new(3),
            ),
            (
                (THIRD_ENTITY_TAG, source_key(THIRD_ID_SOURCE)),
                FieldId::new(1),
            ),
            (
                (THIRD_ENTITY_TAG, source_key(THIRD_PAYLOAD_SOURCE)),
                FieldId::new(2),
            ),
        ]),
    );
    let store = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("multi-entity journaled store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        store,
        AcceptedSchemaRevision::NONE,
        &candidate,
    )
    .expect("multi-entity journaled candidate should publish");
    session
}

fn initialize_journaled() -> DbSession<JournaledTestCanister> {
    initialize_journaled_with_root().0
}

fn initialize_journaled_with_unique_payload() -> DbSession<JournaledTestCanister> {
    initialize_journaled_with_root_and_payload_uniqueness(true).0
}

fn drive_journaled_recovery_to_completion(session: &DbSession<JournaledTestCanister>) {
    for _ in 0..8 {
        if session
            .db
            .drive_startup_recovery_page()
            .expect("dedicated driver recovery should remain valid")
        {
            return;
        }
    }
    panic!("dedicated driver recovery should quiesce within eight complete batches");
}

fn drive_journaled_cardinality_to_ready(session: &DbSession<JournaledTestCanister>) {
    let handle = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled cardinality store should resolve");
    for _ in 0..8 {
        let outcome = handle
            .with_data(|data| {
                handle.with_index(|index| {
                    handle.with_schema_mut(|schema| {
                        drive_cardinality_generation_page(data, index, schema, |schema| {
                            let watermark =
                                JOURNALED_TAIL_STORE.with(|tail| tail.borrow().fold_watermark())?;
                            CardinalityBuildAuthority::derive(
                                schema,
                                database_incarnation_id()?,
                                handle.allocation_identities(),
                                watermark,
                            )
                        })
                    })
                })
            })
            .expect("bounded cardinality generation should advance");
        if outcome == CardinalityGenerationPageOutcome::Quiescent {
            return;
        }
    }
    panic!("cardinality generation should become Ready within eight bounded pages");
}

fn journaled_user_index_prefix() -> (IndexId, Vec<Vec<u8>>) {
    JOURNALED_INDEX_STORE.with(|store| {
        let mut selected = None;
        store
            .borrow()
            .visit_entries(|raw_key, _value| {
                let key =
                    IndexKey::try_from_raw(raw_key).expect("accepted user index key should decode");
                if key.key_kind() != IndexKeyKind::User {
                    return Ok::<_, InternalError>(IndexStoreVisit::Continue);
                }
                let components = (0..key.component_count())
                    .map(|index| {
                        key.component(index)
                            .expect("accepted index component should exist")
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                selected = Some((*key.index_id(), components));
                Ok(IndexStoreVisit::Stop)
            })
            .expect("accepted user index should be inspectable");
        selected.expect("the cardinality fixture should contain one user index entry")
    })
}

fn reset_journaled_cardinality_projections() -> u64 {
    JOURNALED_DATA_STORE.with(|store| {
        store
            .borrow_mut()
            .reset_journaled_live_projection()
            .expect("row projection should reset without a count scan");
    });
    let data_generation = JOURNALED_DATA_STORE.with(|store| store.borrow().generation());
    let fold_watermark = JOURNALED_TAIL_STORE
        .with(|store| store.borrow().fold_watermark())
        .expect("journal watermark should remain current-form");
    JOURNALED_INDEX_STORE.with(|store| {
        store
            .borrow_mut()
            .reset_journaled_live_projection(data_generation, fold_watermark)
            .expect("index projection should reset without a count scan");
    });
    data_generation
}

fn assert_journaled_cardinality(
    handle: StoreHandle,
    index_id: IndexId,
    prefix_components: &[Vec<u8>],
    expected: u64,
) {
    assert_eq!(handle.exact_entity_count(ENTITY_TAG), Some(expected));
    let data_generation = JOURNALED_DATA_STORE.with(|store| store.borrow().generation());
    assert_eq!(
        handle.exact_user_index_prefix_count(
            data_generation,
            IndexKeyKind::User,
            index_id,
            prefix_components,
        ),
        Some(expected),
    );
}

fn mark_journaled_cardinality_building() {
    let current = JOURNALED_SCHEMA_STORE.with(|store| {
        store
            .borrow()
            .cardinality_generation_header()
            .expect("Ready header should decode")
            .expect("Ready header should exist")
    });
    JOURNALED_SCHEMA_STORE.with(|store| {
        store
            .borrow_mut()
            .write_cardinality_generation_header(CardinalityGenerationHeader::new(
                current.generation(),
                CardinalityGenerationState::Building,
                current.slot(),
                current.source(),
            ))
            .expect("Building fallback fixture should persist");
    });
}

fn payload_patch(value: u64) -> AcceptedMutationIntentPatch {
    AcceptedMutationIntentPatch::new()
        .set_authored(FieldSlot::from_validated_index(1), InputValue::nat64(value))
}

fn dynamic_payload_patch(value: u64) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![(
        "payload".to_string(),
        DynamicWriteCell::Value(InputValue::nat64(value)),
    )])
}

fn related_dynamic_payload_patch(value: u64, target_id: u64) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        (
            "payload".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(value)),
        ),
        (
            "target_id".to_string(),
            DynamicWriteCell::Value(InputValue::nat64(target_id)),
        ),
    ])
}

fn expected_dynamic_row(id: u64, payload: u64) -> Vec<OutputValue> {
    vec![OutputValue::nat64(id), OutputValue::nat64(payload)]
}

fn exact_key_binding<C: CanisterKind>(session: &DbSession<C>) -> DynamicTypedEntityBinding {
    session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
        .expect("exact-key test binding should issue")
}

fn typed_payload_insert(binding: &DynamicTypedEntityBinding, payload: u64) -> DynamicTypedMutation {
    let patch = binding
        .bind_write_ordinals(vec![(
            1,
            DynamicWriteCell::Value(InputValue::nat64(payload)),
        )])
        .expect("typed payload patch should bind");
    DynamicTypedMutation::Insert { patch }
}

fn typed_payload_delete(id: u64) -> DynamicTypedMutation {
    DynamicTypedMutation::Delete {
        key: InputValue::nat64(id),
    }
}

fn insert_exact_key_fixture<C: CanisterKind>(session: &DbSession<C>, payload: u64) -> u64 {
    let output = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(payload),
        })
        .expect("exact-key fixture insert should commit");
    match output.rows.as_slice() {
        [row] => match row.as_slice() {
            [id, actual_payload] if matches!(actual_payload.as_public(), crate::value::PublicValue::Nat64(value) if *value == payload) =>
            {
                let crate::value::PublicValue::Nat64(id) = id.as_public() else {
                    panic!("exact-key fixture should return a natural identity");
                };
                *id
            }
            _ => panic!("exact-key fixture should return its identity and payload"),
        },
        _ => panic!("exact-key fixture insert should return one row"),
    }
}

#[cfg(feature = "sql")]
fn sql_projection_rows(session: &DbSession<TestCanister>, sql: &str) -> Vec<Vec<OutputValue>> {
    let crate::db::SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query(sql)
        .expect("focused SQL projection should execute")
    else {
        panic!("focused SQL projection should return rows")
    };

    rows
}

#[cfg(feature = "sql")]
#[test]
fn secondary_ordered_covering_limit_stops_at_the_present_row_window() {
    let session = initialize_with_composite_payload_index();
    for payload in [30, 10, 20, 20, 40] {
        insert_exact_key_fixture(&session, payload);
    }

    assert_eq!(
        sql_projection_rows(
            &session,
            "SELECT payload FROM IdentityRow ORDER BY payload ASC, id ASC LIMIT 1",
        ),
        vec![vec![OutputValue::nat64(10)]],
    );
    #[cfg(feature = "sql")]
    assert_sql_query_fits_resource_limit(
        &session,
        "SELECT payload FROM IdentityRow ORDER BY payload ASC, id ASC LIMIT 1",
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        1,
    );

    assert_eq!(
        sql_projection_rows(
            &session,
            "SELECT payload FROM IdentityRow ORDER BY payload DESC, id DESC LIMIT 1",
        ),
        vec![vec![OutputValue::nat64(40)]],
    );
    #[cfg(feature = "sql")]
    assert_sql_query_fits_resource_limit(
        &session,
        "SELECT payload FROM IdentityRow ORDER BY payload DESC, id DESC LIMIT 1",
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        1,
    );

    assert_eq!(
        sql_projection_rows(
            &session,
            "SELECT id, payload FROM IdentityRow \
             ORDER BY payload ASC, id ASC LIMIT 2 OFFSET 1",
        ),
        vec![
            vec![OutputValue::nat64(3), OutputValue::nat64(20)],
            vec![OutputValue::nat64(4), OutputValue::nat64(20)],
        ],
    );
    #[cfg(feature = "sql")]
    assert_sql_query_fits_resource_limit(
        &session,
        "SELECT id, payload FROM IdentityRow \
         ORDER BY payload ASC, id ASC LIMIT 2 OFFSET 1",
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
        3,
    );

    assert_eq!(
        sql_projection_rows(
            &session,
            "SELECT payload FROM IdentityRow ORDER BY payload ASC, id ASC",
        ),
        [10, 20, 20, 30, 40]
            .into_iter()
            .map(|payload| vec![OutputValue::nat64(payload)])
            .collect::<Vec<_>>(),
    );
}

#[cfg(feature = "sql")]
#[test]
fn secondary_ordered_covering_limit_fails_on_an_accessed_missing_row() {
    let session = initialize_with_composite_payload_index();
    let first = insert_exact_key_fixture(&session, 10);
    insert_exact_key_fixture(&session, 20);
    let raw_key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(first))
        .expect("missing-row fixture key should decode")
        .to_raw()
        .expect("missing-row fixture key should encode");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("missing-row fixture store should resolve");
    assert!(
        store.with_data_mut(|data| data.remove(&raw_key)).is_some(),
        "fixture must remove only the authoritative row",
    );

    let error = session
        .execute_trusted_sql_query(
            "SELECT payload FROM IdentityRow ORDER BY payload ASC, id ASC LIMIT 1",
        )
        .expect_err("an accessed accepted-index row must remain fail-closed");
    assert!(matches!(
        error,
        crate::db::QueryError::Execute(QueryExecutionError::Corruption(_))
    ));
}

#[cfg(feature = "sql")]
#[test]
fn secondary_indexed_max_uses_one_descending_edge_across_ties() {
    let session = initialize_with_composite_payload_index();
    let mut inserted = Vec::new();
    for payload in [30, 10, 20, 20, 40, 40] {
        inserted.push(insert_exact_key_fixture(&session, payload));
    }

    let sql = "SELECT MAX(payload) FROM IdentityRow";
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    assert_eq!(
        sql_projection_rows(&session, sql),
        vec![vec![OutputValue::nat64(40)]],
    );
    assert_eq!(DataStore::current_get_call_count() - data_reads_before, 1);
    assert!(IndexStore::current_entry_read_count() - index_reads_before <= 1);

    let range_sql = "SELECT MAX(payload) FROM IdentityRow WHERE payload < 40";
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    assert_eq!(
        sql_projection_rows(&session, range_sql),
        vec![vec![OutputValue::nat64(30)]],
    );
    assert_eq!(DataStore::current_get_call_count() - data_reads_before, 1);
    assert!(IndexStore::current_entry_read_count() - index_reads_before <= 1);

    let last = inserted
        .last()
        .copied()
        .expect("secondary MAX fixture should retain its last identity");
    let raw_key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(last))
        .expect("missing-row fixture key should decode")
        .to_raw()
        .expect("missing-row fixture key should encode");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("missing-row fixture store should resolve");
    assert!(
        store.with_data_mut(|data| data.remove(&raw_key)).is_some(),
        "fixture must remove only the descending edge row",
    );

    let error = session
        .execute_trusted_sql_query("SELECT MAX(payload) FROM IdentityRow")
        .expect_err("an accessed accepted-index row must remain fail-closed");
    assert!(matches!(
        error,
        crate::db::QueryError::Execute(QueryExecutionError::Corruption(_))
    ));
}

#[cfg(feature = "sql")]
#[test]
fn secondary_indexed_max_upper_range_fails_on_an_accessed_missing_row() {
    let session = initialize_with_composite_payload_index();
    let upper_range_edge = insert_exact_key_fixture(&session, 30);
    for payload in [10, 20, 40] {
        insert_exact_key_fixture(&session, payload);
    }
    let raw_key =
        DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(upper_range_edge))
            .expect("missing-row fixture key should decode")
            .to_raw()
            .expect("missing-row fixture key should encode");
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("missing-row fixture store should resolve");
    assert!(
        store.with_data_mut(|data| data.remove(&raw_key)).is_some(),
        "fixture must remove only the upper-range edge row",
    );

    let error = session
        .execute_trusted_sql_query("SELECT MAX(payload) FROM IdentityRow WHERE payload < 40")
        .expect_err("an accessed upper-range edge row must remain fail-closed");
    assert!(matches!(
        error,
        crate::db::QueryError::Execute(QueryExecutionError::Corruption(_))
    ));
}

#[test]
fn exact_counts_use_entity_and_bounded_index_metadata_without_physical_reads() {
    let session = initialize();
    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    let binding = exact_key_binding(&session);
    let entity = DynamicQuery::new(ENTITY_NAME);
    let tens =
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FieldRef::new("payload").eq(10_u64));
    let selected = DynamicQuery::new(ENTITY_NAME)
        .filter(crate::db::FieldRef::new("payload").in_list([10_u64, 10, 20, 99]));
    let missing =
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FieldRef::new("payload").eq(99_u64));
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();

    assert_eq!(session.execute_public_exact_count(&entity).unwrap(), 3);
    assert_eq!(session.execute_public_exact_count(&tens).unwrap(), 2);
    assert_eq!(session.execute_public_exact_count(&selected).unwrap(), 3);
    assert_eq!(session.execute_public_exact_count(&missing).unwrap(), 0);
    assert_eq!(
        session
            .execute_public_exact_count_for_typed_binding(&binding, &tens)
            .unwrap(),
        Some(2),
    );
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);

    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            (0..64).map(|_| dynamic_payload_patch(10)).collect(),
        )
        .expect("a larger matching population should commit");
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    assert_eq!(session.execute_public_exact_count(&tens).unwrap(), 66);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);
}

#[test]
fn exact_count_accepts_the_leading_field_of_a_composite_user_index() {
    let session = initialize_with_composite_payload_index();
    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    let tens =
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FieldRef::new("payload").eq(10_u64));
    let selected = DynamicQuery::new(ENTITY_NAME)
        .filter(crate::db::FieldRef::new("payload").in_list([10_u64, 20, 99]));
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();

    assert_eq!(session.execute_public_exact_count(&tens).unwrap(), 2);
    assert_eq!(session.execute_public_exact_count(&selected).unwrap(), 3);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);
}

#[cfg(feature = "sql")]
#[test]
fn exact_count_shared_executor_preserves_sql_direct_count_results() {
    let session = initialize();
    let data_reads_before = DataStore::current_get_call_count();
    let crate::db::SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query("SELECT COUNT(*) FROM IdentityRow")
        .expect("empty SQL direct count should succeed")
    else {
        panic!("empty SQL direct count should return one projection row")
    };
    assert_eq!(rows, vec![vec![OutputValue::nat64(0)]]);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);

    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }

    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    for sql in [
        "SELECT COUNT(*) FROM IdentityRow",
        "SELECT COUNT(payload) FROM IdentityRow",
        "SELECT COUNT(1) FROM IdentityRow",
        "SELECT COUNT(*) FROM IdentityRow WHERE true",
        "SELECT COUNT(*) FROM IdentityRow WHERE payload IN (10, 10, 20, 99)",
    ] {
        let crate::db::SqlStatementResult::Projection { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .expect("SQL direct count should use the shared exact executor")
        else {
            panic!("SQL direct count should return one projection row")
        };
        assert_eq!(rows, vec![vec![OutputValue::nat64(3)]], "{sql}");
    }
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);

    let crate::db::SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query("SELECT COUNT(*) FROM IdentityRow WHERE payload = 10")
        .expect("nontrivial exact-prefix count should preserve its predicate")
    else {
        panic!("nontrivial exact-prefix count should return one projection row")
    };
    assert_eq!(rows, vec![vec![OutputValue::nat64(2)]]);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);

    let data_reads_before = DataStore::current_get_call_count();
    for (sql, expected) in [
        ("SELECT COUNT(*) FROM IdentityRow WHERE false", 0_u64),
        ("SELECT COUNT(*) FROM IdentityRow WHERE id = 1", 1),
    ] {
        let crate::db::SqlStatementResult::Projection { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .expect("non-entity count control should succeed")
        else {
            panic!("non-entity count control should return one projection row")
        };
        assert_eq!(rows, vec![vec![OutputValue::nat64(expected)]], "{sql}");
    }
    assert!(DataStore::current_get_call_count() > data_reads_before);

    session
        .execute_trusted_sql_query("SELECT COUNT(*) FROM IdentityRow LIMIT 1")
        .expect_err("unordered aggregate input pagination must remain rejected");

    let data_reads_before = DataStore::current_get_call_count();
    let crate::db::SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query("SELECT COUNT(DISTINCT payload) FROM IdentityRow")
        .expect("distinct count should retain prepared execution")
    else {
        panic!("distinct count should return one projection row")
    };
    assert_eq!(rows, vec![vec![OutputValue::nat64(2)]]);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
}

#[cfg(feature = "sql")]
#[test]
fn exact_count_composite_prefix_admits_seventeen_canonical_keys_only() {
    let session = initialize_with_composite_payload_index();
    for payload in [10, 10, 20] {
        insert_exact_key_fixture(&session, payload);
    }
    let ids_at_count_cap = (1_u64..=17)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let at_count_cap_sql = format!(
        "SELECT COUNT(*) FROM IdentityRow WHERE payload = 10 AND id IN ({ids_at_count_cap})",
    );
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    assert_eq!(
        sql_projection_rows(&session, at_count_cap_sql.as_str()),
        vec![vec![OutputValue::nat64(2)]],
    );
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);

    let authored_duplicate_sql = format!(
        "SELECT COUNT(*) FROM IdentityRow WHERE payload = 10 AND id IN (1, {ids_at_count_cap})",
    );
    assert_eq!(
        sql_projection_rows(&session, authored_duplicate_sql.as_str()),
        vec![vec![OutputValue::nat64(2)]],
    );
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);

    let ids_over_count_cap = format!("{ids_at_count_cap}, 18");
    let over_count_cap_sql = format!(
        "SELECT COUNT(*) FROM IdentityRow WHERE payload = 10 AND id IN ({ids_over_count_cap})",
    );
    assert_eq!(
        sql_projection_rows(&session, over_count_cap_sql.as_str()),
        vec![vec![OutputValue::nat64(2)]],
    );
    assert!(DataStore::current_get_call_count() > data_reads_before);
}

#[cfg(feature = "sql")]
#[test]
fn exact_count_nullable_field_uses_prepared_borrowed_primary_scan() {
    let session = initialize_with_snapshot(identity_snapshot_with_nullable_payload(STORE_PATH));
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![
                dynamic_payload_patch(10),
                DynamicStructuralPatch::new(Vec::new()),
            ],
        )
        .expect("nullable count fixture should insert");

    let data_reads_before = DataStore::current_get_call_count();
    let crate::db::SqlStatementResult::Projection { rows, .. } = session
        .execute_trusted_sql_query("SELECT COUNT(payload) FROM IdentityRow")
        .expect("nullable count should retain prepared execution")
    else {
        panic!("nullable count should return one projection row")
    };
    assert_eq!(rows, vec![vec![OutputValue::nat64(1)]]);
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
}

#[cfg(feature = "sql")]
#[test]
fn indexed_extrema_nullable_field_uses_prepared_borrowed_primary_scan() {
    let session = initialize_with_snapshot(identity_snapshot_with_nullable_payload(STORE_PATH));
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![DynamicStructuralPatch::new(Vec::new())],
        )
        .expect("all-null extrema fixture should insert");

    for sql in [
        "SELECT MIN(payload) FROM IdentityRow",
        "SELECT MAX(payload) FROM IdentityRow",
    ] {
        let data_reads_before = DataStore::current_get_call_count();
        let crate::db::SqlStatementResult::Projection { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .expect("all-null extrema should retain complete reduction")
        else {
            panic!("all-null extrema should return one projection row")
        };
        assert_eq!(rows, vec![vec![OutputValue::null()]], "{sql}");
        assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    }

    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(10)])
        .expect("mixed nullable extrema fixture should insert");

    for sql in [
        "SELECT MIN(payload) FROM IdentityRow",
        "SELECT MAX(payload) FROM IdentityRow",
    ] {
        let data_reads_before = DataStore::current_get_call_count();
        let crate::db::SqlStatementResult::Projection { rows, .. } = session
            .execute_trusted_sql_query(sql)
            .expect("mixed nullable extrema should retain complete reduction")
        else {
            panic!("mixed nullable extrema should return one projection row")
        };
        assert_eq!(rows, vec![vec![OutputValue::nat64(10)]], "{sql}");
        assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    }
}

#[test]
fn exact_count_rejects_non_metadata_shapes_without_physical_reads() {
    let session = initialize();
    insert_exact_key_fixture(&session, 10);
    let rejected = [
        DynamicQuery::new(ENTITY_NAME).limit(1),
        DynamicQuery::new(ENTITY_NAME).select(["payload"]),
        DynamicQuery::new(ENTITY_NAME).order_by(crate::db::asc("payload")),
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FieldRef::new("id").eq(1_u64)),
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FilterExpr::and(vec![
            crate::db::FieldRef::new("payload").eq(10_u64),
            crate::db::FieldRef::new("id").eq(1_u64),
        ])),
        DynamicQuery::new(ENTITY_NAME)
            .filter(crate::db::FieldRef::new("payload").in_list(0_u64..=16)),
    ];
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    for request in rejected {
        let error = session
            .execute_public_exact_count(&request)
            .expect_err("unsupported count shape must reject");
        assert_eq!(
            error.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_UNSUPPORTED,
        );
        assert!(matches!(
            error,
            crate::db::QueryError::Execute(QueryExecutionError::Unsupported(_)),
        ));
    }
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);
}

#[test]
fn exact_count_unavailable_metadata_has_a_typed_diagnostic_without_physical_reads() {
    let journaled = initialize_journaled();
    insert_exact_key_fixture(&journaled, 10);
    let binding = exact_key_binding(&journaled);
    let requests = [
        DynamicQuery::new(ENTITY_NAME),
        DynamicQuery::new(ENTITY_NAME).filter(crate::db::FieldRef::new("payload").eq(10_u64)),
    ];
    let data_reads_before = DataStore::current_get_call_count();
    let index_reads_before = IndexStore::current_entry_read_count();
    for request in requests {
        let dynamic = journaled
            .execute_public_exact_count(&request)
            .expect_err("journal overlay has no exact metadata");
        let typed = journaled
            .execute_public_exact_count_for_typed_binding(&binding, &request)
            .expect_err("typed binding must retain unavailable-metadata diagnostic");
        for error in [dynamic, typed] {
            let diagnostic = error.diagnostic();
            assert_eq!(
                diagnostic.error_code(),
                icydb_diagnostic_code::ErrorCode::QUERY_EXACT_COUNT_METADATA_UNAVAILABLE,
            );
            assert_eq!(
                diagnostic.class(),
                icydb_diagnostic_code::ErrorClass::Unsupported
            );
            assert_eq!(
                diagnostic.origin(),
                icydb_diagnostic_code::ErrorOrigin::Query
            );
            assert!(matches!(
                error,
                crate::db::QueryError::Execute(QueryExecutionError::Unsupported(_)),
            ));
        }
    }
    assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    assert_eq!(IndexStore::current_entry_read_count(), index_reads_before);
}

#[test]
fn exact_count_typed_binding_fails_closed_after_accepted_revision_changes() {
    let session = initialize();
    let binding = exact_key_binding(&session);
    let request = DynamicQuery::new(ENTITY_NAME);
    assert_eq!(
        session
            .execute_public_exact_count_for_typed_binding(&binding, &request)
            .unwrap(),
        Some(0),
    );

    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        STORE_PATH,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(ENTITY_TAG, identity_snapshot(STORE_PATH, false))]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
        ]),
    );
    let store = session
        .db
        .store_handle(STORE_PATH)
        .expect("exact-count store should resolve");
    crate::db::commit::publish_accepted_schema_candidate(
        STORE_PATH,
        store,
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .expect("successor accepted schema should publish");

    assert_eq!(
        session
            .execute_public_exact_count_for_typed_binding(&binding, &request)
            .unwrap(),
        None,
    );
}

#[cfg(feature = "sql")]
fn identity_row_stored_bytes<C: CanisterKind>(
    session: &DbSession<C>,
    store_path: &'static str,
    key: u64,
) -> u64 {
    let data_key = DecodedDataStoreKey::try_from_structural_key(ENTITY_TAG, &Value::Nat64(key))
        .expect("identity row key should encode");
    let raw_key = data_key.to_raw().expect("identity raw key should encode");
    let store = session
        .db
        .recovered_store(store_path)
        .expect("identity store should resolve");
    store.with_data(|data_store| {
        u64::try_from(
            data_store
                .get(&raw_key)
                .expect("inserted identity row should exist")
                .len(),
        )
        .expect("bounded row length should fit u64")
    })
}

#[cfg(feature = "sql")]
fn with_stored_bytes_limit<T>(
    limit: u64,
    shape_fingerprint_prefix: u64,
    operation: impl FnOnce() -> Result<T, crate::db::query::intent::QueryError>,
) -> Result<T, crate::db::query::intent::QueryError> {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::StoredBytesRead,
        limit,
    );
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
        shape_fingerprint_prefix,
    );

    with_query_execution_budget_for_tests(budget, context, operation)
}

#[cfg(feature = "sql")]
fn advance_with_exhausted_mutation_predicate_budget(
    session: &DbSession<JournaledTestCanister>,
    request: &MutationJobAdvanceRequest,
) -> Result<crate::db::MutationJobAdvanceReceipt, MutationJobError> {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(1_000_000_000, 64 * 1_024),
    )
    .with_limit_for_tests(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
        0,
    );
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::Mutation,
        0x6d75_7461_7465_7465,
    );
    with_execution_budget_for_tests(
        budget,
        context,
        || session.advance_trusted_mutation_job(request),
        |_| MutationJobError::Internal,
    )
}

#[cfg(feature = "sql")]
const fn exact_key(value: u64) -> PrimaryKeyValue {
    PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value))
}

#[cfg(feature = "sql")]
fn assert_exact_key_batch<C: CanisterKind>(session: &DbSession<C>, fold: bool) {
    let first = insert_exact_key_fixture(session, 41);
    let second = insert_exact_key_fixture(session, 42);
    if fold {
        for _ in 0..8 {
            if session.db.drive_startup_recovery_page().unwrap() {
                break;
            }
        }
        assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
    }
    let missing = u64::MAX;
    let binding = exact_key_binding(session);
    let gets_before = DataStore::current_get_call_count();
    let result = session
        .execute_public_exact_key_batch_for_typed_binding(
            &binding,
            &[
                exact_key(second),
                exact_key(missing),
                exact_key(first),
                exact_key(second),
            ],
        )
        .expect("exact-key batch should execute")
        .expect("exact-key binding should remain current");

    assert_eq!(result.positions, vec![0, 1, 2, 0]);
    assert_eq!(
        result.distinct_rows,
        vec![
            Some(expected_dynamic_row(second, 42)),
            None,
            Some(expected_dynamic_row(first, 41)),
        ],
    );
    assert_eq!(
        DataStore::current_get_call_count().saturating_sub(gets_before),
        3,
        "four input positions with one duplicate must perform three physical reads",
    );
    for limit in [1, 2] {
        let budget = HardExecutionBudget::uniform_for_tests(
            32_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1_024),
        )
        .with_limit_for_tests(
            icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
            limit,
        );
        let bounded = session.execute_exact_key_batch_with_hard_budget_for_tests(
            &binding,
            &[
                exact_key(second),
                exact_key(missing),
                exact_key(first),
                exact_key(second),
            ],
            &budget,
        );
        if limit == 2 {
            let bounded = bounded.unwrap().unwrap();
            assert_eq!(bounded.distinct_rows, result.distinct_rows);
            assert_eq!(bounded.positions, result.positions);
        } else {
            let facts = bounded.unwrap_err().diagnostic_facts();
            assert!(facts.contains(&(
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited.raw(),
            )));
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn exact_key_batches_preserve_semantics_across_heap_and_journaled_stores() {
    assert_exact_key_batch(&initialize(), false);
    assert_exact_key_batch(&initialize_journaled(), false);
    assert_exact_key_batch(&initialize_journaled(), true);
}

#[cfg(feature = "sql")]
fn assert_primary_range_materialization_fetches_once<C: CanisterKind>(
    session: &DbSession<C>,
    store_path: &'static str,
) {
    let key = insert_exact_key_fixture(session, 41);
    let stored_bytes = identity_row_stored_bytes(session, store_path, key);

    let scalar = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "payload"])
        .order_by(asc("id"))
        .limit(1);
    let gets_before = DataStore::current_get_call_count();
    let scalar_page = with_stored_bytes_limit(stored_bytes, 0x7072_696d_6172_792d, || {
        session.execute_trusted_live_page(&scalar, None)
    })
    .expect("one scalar primary-range row should fit one payload-read allowance");
    assert_eq!(scalar_page.row_count, 1);
    assert_eq!(
        DataStore::current_get_call_count().saturating_sub(gets_before),
        0,
        "scalar primary traversal should reuse its iterator payload without a point lookup",
    );

    let grouped = DynamicQuery::new(ENTITY_NAME)
        .group_by("payload")
        .aggregate(crate::db::count())
        .grouped_limits(10, 16 * 1_024)
        .limit(1);
    let gets_before = DataStore::current_get_call_count();
    let grouped_page = with_stored_bytes_limit(stored_bytes, 0x6772_6f75_7065_642d, || {
        session.execute_trusted_dynamic_grouped_query(&grouped)
    })
    .expect("one grouped primary-range row should fit one payload-read allowance");
    assert_eq!(grouped_page.row_count, 1);
    assert_eq!(
        DataStore::current_get_call_count().saturating_sub(gets_before),
        1,
        "grouped primary traversal should fetch its source row exactly once",
    );
}

#[cfg(feature = "sql")]
#[test]
fn row_materialization_fetches_each_required_payload_at_most_once() {
    assert_primary_range_materialization_fetches_once(&initialize(), STORE_PATH);
    assert_primary_range_materialization_fetches_once(
        &initialize_journaled(),
        JOURNALED_STORE_PATH,
    );
}

#[cfg(feature = "sql")]
#[test]
fn ordered_grouped_pages_close_a_group_spanning_physical_refills_before_resume() {
    let session = initialize();
    let mut patches = Vec::new();
    for _ in 0..70 {
        patches.push(dynamic_payload_patch(10));
    }
    for _ in 0..3 {
        patches.push(dynamic_payload_patch(20));
    }
    patches.push(dynamic_payload_patch(30));
    let inserted = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, patches)
        .expect("ordered grouped continuation rows should insert");
    assert_eq!(inserted.rows.len(), 74);

    let query = DynamicQuery::new(ENTITY_NAME)
        .group_by("payload")
        .aggregate(crate::db::count())
        .aggregate(crate::db::sum("id"))
        .order_by(asc("payload"))
        .grouped_limits(4, 16 * 1_024)
        .limit(1);
    let expected = [
        (10_u64, 70_u64, crate::types::Decimal::new(2_485, 0)),
        (20, 3, crate::types::Decimal::new(216, 0)),
        (30, 1, crate::types::Decimal::new(74, 0)),
    ];
    let mut continuation: Option<String> = None;
    let mut seen_cursors = std::collections::BTreeSet::new();

    for (page_index, (group_key, row_count, id_sum)) in expected.into_iter().enumerate() {
        let request = continuation.as_ref().map_or_else(
            || query.clone(),
            |cursor| query.clone().cursor(cursor.clone()),
        );
        let entries_before = IndexStore::current_entry_read_count();
        let rows_before = DataStore::current_get_call_count();
        let page = session
            .execute_trusted_dynamic_grouped_query(&request)
            .unwrap_or_else(|error| {
                panic!("ordered grouped page {page_index} should execute: {error:?}")
            });
        let entries_read = IndexStore::current_entry_read_count().saturating_sub(entries_before);
        let rows_read = DataStore::current_get_call_count().saturating_sub(rows_before);

        assert_eq!(page.row_count, 1);
        let [row] = page.rows.as_slice() else {
            panic!("ordered grouped page must contain exactly one closed group")
        };
        assert_eq!(row.group_key(), &[OutputValue::nat64(group_key)]);
        assert_eq!(
            row.aggregate_values(),
            &[OutputValue::nat64(row_count), OutputValue::decimal(id_sum),],
        );
        if page_index == 0 {
            assert!(
                entries_read.saturating_add(rows_read) >= 70,
                "the first closed group must span the maintained 64-entry physical refill",
            );
        }

        continuation = page.next_cursor;
        if page_index + 1 < expected.len() {
            let cursor = continuation
                .as_ref()
                .expect("another closed group should retain continuation");
            assert!(
                seen_cursors.insert(cursor.clone()),
                "ordered grouped continuation must advance monotonically",
            );
        } else {
            assert_eq!(continuation, None);
        }
    }
}

#[cfg(feature = "sql")]
#[test]
fn exhaustive_pages_require_and_recompare_the_complete_source_proof() {
    let session = initialize_journaled();
    let first = insert_exact_key_fixture(&session, 41);
    let second = insert_exact_key_fixture(&session, 42);
    let third = insert_exact_key_fixture(&session, 43);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "payload"])
        .order_by(asc("id"));

    let page = session
        .execute_trusted_exhaustive_page(&query, None, None)
        .expect("initial exhaustive page should capture its source proof");
    assert_eq!(
        page.rows,
        vec![
            expected_dynamic_row(first, 41),
            expected_dynamic_row(second, 42),
        ],
    );
    let continuation = page
        .continuation
        .as_deref()
        .expect("unreturned row should retain exhaustive continuation");
    assert!(matches!(
        session.execute_trusted_exhaustive_page(&query, Some(continuation), None),
        Err(ExhaustiveReadError::Revision(
            ReadSetRevisionError::ResumeProofRequired
        )),
    ));
    let resumed = session
        .execute_trusted_exhaustive_page(&query, Some(continuation), Some(&page.proof))
        .expect("unchanged proof should resume exhaustive traversal");
    assert_eq!(resumed.rows, vec![expected_dynamic_row(third, 43)]);
    assert_eq!(resumed.continuation, None);

    let stale_page = session
        .execute_trusted_exhaustive_page(&query, None, None)
        .expect("fresh exhaustive page should capture current revision");
    let stale_continuation = stale_page
        .continuation
        .as_deref()
        .expect("fresh three-row traversal should retain continuation");
    let _ = insert_exact_key_fixture(&session, 44);
    assert!(matches!(
        session.execute_trusted_exhaustive_page(
            &query,
            Some(stale_continuation),
            Some(&stale_page.proof),
        ),
        Err(ExhaustiveReadError::Revision(
            ReadSetRevisionError::StoreDataChanged { .. }
        )),
    ));
}

#[cfg(feature = "sql")]
#[test]
fn heap_exhaustive_pages_require_one_call_without_a_supplied_proof() {
    let session = initialize();
    insert_exact_key_fixture(&session, 41);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "payload"])
        .filter(crate::db::FieldRef::new("id").in_list([1_u64, 2, 3]))
        .order_by(asc("id"));
    for public in [false, true] {
        let page = if public {
            session.execute_public_exhaustive_page(&query, None, None)
        } else {
            session.execute_trusted_exhaustive_page(&query, None, None)
        }
        .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert!(page.continuation.is_none());
        let reused = if public {
            session.execute_public_exhaustive_page(&query, None, Some(&page.proof))
        } else {
            session.execute_trusted_exhaustive_page(&query, None, Some(&page.proof))
        };
        assert!(matches!(
            reused,
            Err(ExhaustiveReadError::Revision(
                ReadSetRevisionError::DurableStoreRequired { .. }
            ))
        ));
    }
    insert_exact_key_fixture(&session, 42);
    insert_exact_key_fixture(&session, 43);
    for public in [false, true] {
        let page = if public {
            session.execute_public_exhaustive_page(&query, None, None)
        } else {
            session.execute_trusted_exhaustive_page(&query, None, None)
        };
        assert!(matches!(
            page,
            Err(ExhaustiveReadError::Revision(
                ReadSetRevisionError::DurableStoreRequired { .. }
            ))
        ));
    }
    assert_eq!(
        session
            .execute_trusted_live_page(&query, None)
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[cfg(feature = "sql")]
#[test]
fn heap_sources_cannot_back_durable_resumable_jobs() {
    use crate::db::{ResumableJobRecord, integrity::with_resumable_progress_store};

    let session = initialize();
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("heap source proof should capture for one-call exhaustive reads");
    let job_id = ResumableJobId::try_from_bytes([70; 32])
        .expect("nonzero heap test job identity should admit");

    assert!(matches!(
        session.start_resumable_job(job_id, proof.clone(), Vec::new()),
        Err(ResumableJobError::SourceProof(
            ReadSetRevisionError::DurableStoreRequired { .. }
        )),
    ));

    // Model a retained record whose current source is heap-backed while its
    // revision values still match. Admission at creation cannot protect a
    // later process; advancement must recheck the current storage capability.
    let record = ResumableJobRecord::new(job_id, proof, vec![7]).unwrap();
    with_resumable_progress_store::<TestCanister, _>(|store| store.insert_resumable(&record))
        .unwrap();
    let request = ResumableJobAdvanceRequest::new(
        job_id,
        0,
        ResumableJobIdempotencyKey::new("heap-resume").unwrap(),
    );
    let called = Cell::new(false);
    for _ in 0..2 {
        assert!(matches!(
            session.compare_proof_and_advance(&request, |_| {
                called.set(true);
                Ok::<_, ()>(ResumableJobAdvance::new(None, vec![8], Vec::new()).unwrap())
            }),
            Err(CompareProofAndAdvanceError::Protocol(
                ResumableJobError::SourceProof(ReadSetRevisionError::DurableStoreRequired { .. })
            )),
        ));
        assert!(!called.get());
        assert_eq!(
            session.resumable_job_state(job_id).unwrap(),
            *record.state()
        );
    }
}

#[cfg(feature = "sql")]
#[test]
fn journaled_source_proof_and_job_survive_empty_and_retained_tail_recovery() {
    let session = initialize_journaled();
    insert_exact_key_fixture(&session, 51);
    insert_exact_key_fixture(&session, 52);
    insert_exact_key_fixture(&session, 53);
    let query = DynamicQuery::new(ENTITY_NAME)
        .select(["id", "payload"])
        .order_by(asc("id"));
    let page = session
        .execute_trusted_exhaustive_page(&query, None, None)
        .unwrap();
    let cursor = page.continuation.as_ref().unwrap();
    let proof = page.proof;
    let job_id = ResumableJobId::try_from_bytes([76; 32]).unwrap();
    session
        .start_resumable_job(job_id, proof.clone(), vec![0])
        .unwrap();
    let store = session.db.store_handle(JOURNALED_STORE_PATH).unwrap();
    let revision = store.access_state_revision().unwrap();

    // First recover retained row batches, then the now-empty tail. Neither
    // changes the logical source, even though both rebuild disposable overlays.
    for sequence in 0..2 {
        forget_recovered_domain_for_tests(&session.db).unwrap();
        drive_journaled_recovery_to_completion(&session);
        assert_eq!(store.access_state_revision().unwrap(), revision);
        session
            .verify_durable_read_set_revision_proof(&proof)
            .unwrap();
        let resumed = session
            .execute_trusted_exhaustive_page(&query, Some(cursor), Some(&proof))
            .unwrap();
        assert_eq!(resumed.rows.len(), 1);
        let request = ResumableJobAdvanceRequest::new(
            job_id,
            sequence,
            ResumableJobIdempotencyKey::new(format!("recovered-{sequence}")).unwrap(),
        );
        let receipt = session
            .compare_proof_and_advance(&request, |_| {
                Ok::<_, ()>(
                    ResumableJobAdvance::new(Some("next".into()), vec![1], Vec::new()).unwrap(),
                )
            })
            .unwrap();
        assert_eq!(receipt.status, ResumableJobAdvanceStatus::Advanced);
    }
    // Actual readiness changes remain durable and invalidate the same proof.
    store.mark_index_building().unwrap();
    store.mark_index_ready().unwrap();
    assert!(matches!(
        session.verify_durable_read_set_revision_proof(&proof),
        Err(ExhaustiveReadError::Revision(
            ReadSetRevisionError::StoreAccessChanged { .. }
        )),
    ));
}

#[cfg(feature = "sql")]
#[test]
fn proof_and_progress_controls_charge_one_shared_request_scope() {
    let (session, root) = initialize_journaled_with_root();
    let resource = icydb_diagnostic_code::DiagnosticExecutionBudgetResource::QueryExecutions;
    let before = root.observed(resource);
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("proof capture should use the retained request scope");
    let job_id = ResumableJobId::try_from_bytes([75; 32])
        .expect("nonzero accounting job identity should admit");
    session
        .start_resumable_job(job_id, proof, Vec::new())
        .expect("job start should use the same retained request scope");
    let _ = session
        .resumable_job_state(job_id)
        .expect("job load should use the same retained request scope");

    assert_eq!(root.observed(resource).saturating_sub(before), 3);
}

#[cfg(feature = "sql")]
#[test]
fn source_proofs_ignore_unrelated_stores_but_bind_access_state_changes() {
    let session = initialize();
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("source proof should cover only the entity's physical store");
    let shared_store_proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME, ENTITY_NAME])
        .expect("entities sharing one physical source should deduplicate");
    assert_eq!(shared_store_proof, proof);
    assert_eq!(shared_store_proof.stores().len(), 1);
    let unrelated = session
        .db
        .store_handle(UNRELATED_STORE_PATH)
        .expect("unrelated registered store should resolve");
    unrelated.with_data_mut(|store| {
        let _ = store.remove(&RawDataStoreKey::from_persisted_bytes(vec![1]));
    });
    session
        .verify_read_set_revision_proof(&proof)
        .expect("a nonparticipating store mutation must not invalidate the proof");

    let source = session
        .db
        .store_handle(STORE_PATH)
        .expect("participating source store should resolve");
    source
        .mark_index_building()
        .expect("source access-state transition should advance its revision");
    assert!(matches!(
        session.verify_read_set_revision_proof(&proof),
        Err(ExhaustiveReadError::Revision(
            ReadSetRevisionError::StoreAccessChanged { .. }
        )),
    ));
}

#[cfg(feature = "sql")]
#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle test proves successful replay plus pre-page and post-page source invalidation without sharing progress state across tests"
)]
#[test]
fn journaled_job_advance_is_idempotent_and_revision_checked_on_both_sides() {
    let session = initialize_journaled();
    let proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("journaled source proof should capture");
    let job_id =
        ResumableJobId::try_from_bytes([71; 32]).expect("nonzero job identity should admit");
    session
        .start_resumable_job(job_id, proof, vec![0])
        .expect("journaled job should start outside its protected source revision");
    let request = ResumableJobAdvanceRequest::new(
        job_id,
        0,
        ResumableJobIdempotencyKey::new("page-0").expect("bounded idempotency key should admit"),
    );
    let calls = Cell::new(0_u8);
    let receipt = session
        .compare_proof_and_advance(&request, |state| {
            calls.set(calls.get() + 1);
            assert_eq!(state.application_state, vec![0]);
            Ok::<_, ()>(
                ResumableJobAdvance::new(Some("cursor-1".to_string()), vec![1], vec![9])
                    .expect("bounded application advance should admit"),
            )
        })
        .expect("unchanged source should advance exactly once");
    assert_eq!(calls.get(), 1);
    assert_eq!(receipt.status, ResumableJobAdvanceStatus::Advanced);
    assert_eq!(receipt.committed_sequence, 1);

    let replay = session
        .compare_proof_and_advance::<()>(&request, |_| {
            panic!("lost-response replay must not execute application work")
        })
        .expect("same request identity should return its persisted receipt");
    assert_eq!(replay, receipt);
    let retained = session
        .resumable_job_state(job_id)
        .expect("advanced state should remain durable");
    assert_eq!(retained.sequence, 1);
    assert_eq!(retained.application_state, vec![1]);

    let _ = insert_exact_key_fixture(&session, 51);
    let pre_change_request = ResumableJobAdvanceRequest::new(
        job_id,
        1,
        ResumableJobIdempotencyKey::new("page-1").expect("bounded idempotency key should admit"),
    );
    let pre_change_calls = Cell::new(0_u8);
    let invalidated = session
        .compare_proof_and_advance::<()>(&pre_change_request, |_| {
            pre_change_calls.set(pre_change_calls.get() + 1);
            unreachable!("pre-page proof failure must reject before application work")
        })
        .expect("source drift should persist one replayable invalidation receipt");
    assert_eq!(pre_change_calls.get(), 0);
    assert_eq!(invalidated.status, ResumableJobAdvanceStatus::Invalidated);
    let invalidated_state = session
        .resumable_job_state(job_id)
        .expect("invalidated job should remain inspectable");
    assert_eq!(invalidated_state.status, ResumableJobStatus::Invalidated);
    assert_eq!(invalidated_state.continuation, None);
    assert_eq!(invalidated_state.application_state, vec![1]);
    assert_eq!(
        session
            .compare_proof_and_advance::<()>(&pre_change_request, |_| {
                panic!("invalidation replay must not execute application work")
            })
            .expect("lost invalidation reply should replay exactly"),
        invalidated,
    );

    let post_proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("post-change journaled proof should capture");
    let post_job_id = ResumableJobId::try_from_bytes([72; 32])
        .expect("nonzero post-change job identity should admit");
    session
        .start_resumable_job(post_job_id, post_proof, vec![7])
        .expect("post-change journaled job should start");
    let post_request = ResumableJobAdvanceRequest::new(
        post_job_id,
        0,
        ResumableJobIdempotencyKey::new("post-page-0")
            .expect("bounded idempotency key should admit"),
    );
    let post_receipt = session
        .compare_proof_and_advance::<()>(&post_request, |_| {
            let _ = insert_exact_key_fixture(&session, 52);
            Ok(ResumableJobAdvance::new(None, vec![8], vec![10])
                .expect("bounded post-change candidate should admit"))
        })
        .expect("post-page drift should discard the candidate and persist invalidation");
    assert_eq!(post_receipt.status, ResumableJobAdvanceStatus::Invalidated);
    let post_state = session
        .resumable_job_state(post_job_id)
        .expect("post-page invalidation should remain inspectable");
    assert_eq!(post_state.status, ResumableJobStatus::Invalidated);
    assert_eq!(post_state.application_state, vec![7]);
    assert_dynamic_payload(&session, 2, 52);
    session
        .acknowledge_resumable_job(post_job_id, post_state.sequence)
        .expect("terminal job acknowledgement should remove retained progress");
    session
        .acknowledge_resumable_job(post_job_id, post_state.sequence)
        .expect("lost acknowledgement reply should be safely replayable");
    assert_eq!(
        session.resumable_job_state(post_job_id),
        Err(ResumableJobError::NotFound),
    );

    let completed_job_id = ResumableJobId::try_from_bytes([74; 32])
        .expect("nonzero completed job identity should admit");
    let completed_proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("completed-job source proof should capture");
    session
        .start_resumable_job(completed_job_id, completed_proof, Vec::new())
        .expect("completed-job fixture should start");
    let completed_request = ResumableJobAdvanceRequest::new(
        completed_job_id,
        0,
        ResumableJobIdempotencyKey::new("complete").expect("bounded completion key should admit"),
    );
    let completed_receipt = session
        .compare_proof_and_advance::<()>(&completed_request, |_| {
            Ok(ResumableJobAdvance::new(None, vec![99], vec![100])
                .expect("bounded terminal advance should admit"))
        })
        .expect("null continuation should commit terminal completion");
    let completed_state = session
        .resumable_job_state(completed_job_id)
        .expect("completed state should remain replayable before acknowledgement");
    assert_eq!(completed_state.status, ResumableJobStatus::Completed);
    assert_eq!(
        session
            .compare_proof_and_advance::<()>(&completed_request, |_| {
                panic!("completed request replay must not execute application work")
            })
            .expect("completed request should replay until acknowledgement"),
        completed_receipt,
    );
    let after_completion = ResumableJobAdvanceRequest::new(
        completed_job_id,
        1,
        ResumableJobIdempotencyKey::new("after-complete")
            .expect("bounded post-completion key should admit"),
    );
    assert!(matches!(
        session.compare_proof_and_advance::<()>(&after_completion, |_| {
            panic!("completed jobs cannot execute another page")
        }),
        Err(CompareProofAndAdvanceError::Protocol(
            ResumableJobError::Completed
        )),
    ));
    session
        .acknowledge_resumable_job(completed_job_id, completed_state.sequence)
        .expect("completed job should acknowledge and free capacity");
    session
        .acknowledge_resumable_job(completed_job_id, completed_state.sequence)
        .expect("completion acknowledgement should be idempotent");

    let stale_job_id = ResumableJobId::try_from_bytes([73; 32])
        .expect("nonzero stale-sequence job identity should admit");
    let stale_proof = session
        .capture_read_set_revision_proof(&[ENTITY_NAME])
        .expect("stale-sequence source proof should capture");
    session
        .start_resumable_job(stale_job_id, stale_proof, Vec::new())
        .expect("stale-sequence job should start");
    let stale_request = ResumableJobAdvanceRequest::new(
        stale_job_id,
        4,
        ResumableJobIdempotencyKey::new("stale").expect("bounded idempotency key should admit"),
    );
    assert!(matches!(
        session.compare_proof_and_advance::<()>(&stale_request, |_| {
            panic!("stale sequence must reject before application work")
        }),
        Err(CompareProofAndAdvanceError::Protocol(
            ResumableJobError::StaleSequence {
                expected: 4,
                actual: 0,
            }
        )),
    ));
    assert_eq!(
        session.acknowledge_resumable_job(stale_job_id, 0),
        Err(ResumableJobError::NotTerminal),
    );
}

#[cfg(feature = "sql")]
#[test]
fn exact_key_batch_uses_typed_hard_execution_budget() {
    let session = initialize();
    let binding = exact_key_binding(&session);
    let budget =
        HardExecutionBudget::uniform_for_tests(0, HardExecutionFailureHeadroom::new(500, 256));
    let error = session
        .execute_exact_key_batch_with_hard_budget_for_tests(
            &binding,
            &[exact_key(u64::MAX)],
            &budget,
        )
        .expect_err("zero query budget should reject the exact-key route");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    let facts = error.diagnostic_facts();
    assert_eq!(
        &facts[..5],
        &[
            (
                icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
                icydb_diagnostic_code::DiagnosticExecutionBudgetResource::QueryExecutions.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 0),
            (icydb_diagnostic_code::DiagnosticFactTag::Actual, 1),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExecutionBudgetScope,
                icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution.raw(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExecutionLane,
                icydb_diagnostic_code::DiagnosticExecutionLane::PublicRead.raw(),
            ),
        ],
    );
    assert_eq!(
        facts[5].0,
        icydb_diagnostic_code::DiagnosticFactTag::QueryShapeFingerprintPrefix,
    );
    assert_ne!(facts[5].1, 0);
}

#[cfg(feature = "sql")]
fn assert_planned_query_exhausts(
    session: &DbSession<TestCanister>,
    query: &crate::db::DynamicQuery,
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
) {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(resource, 0);
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
        0x7068_7973_6963_616c,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        session.execute_trusted_live_page(query, None)
    })
    .expect_err("the injected zero resource allowance should reject planned execution");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error.diagnostic_facts()[0],
        (
            icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
            resource.raw(),
        ),
    );
}

#[cfg(feature = "sql")]
fn assert_grouped_query_exhausts(
    session: &DbSession<TestCanister>,
    query: &crate::db::DynamicQuery,
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
) {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(resource, 0);
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
        0x6772_6f75_7065_642d,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        session.execute_trusted_dynamic_grouped_query(query)
    })
    .expect_err("the injected zero resource allowance should reject grouped execution");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error.diagnostic_facts()[0],
        (
            icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
            resource.raw(),
        ),
    );
}

#[cfg(feature = "sql")]
fn assert_sql_query_exhausts(
    session: &DbSession<TestCanister>,
    sql: &str,
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
) {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(resource, 0);
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
        0x7371_6c2d_736f_7274,
    );
    let error = with_query_execution_budget_for_tests(budget, context, || {
        session.execute_trusted_sql_query(sql)
    })
    .expect_err("the injected zero resource allowance should reject SQL execution");

    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
    assert_eq!(
        error.diagnostic_facts()[0],
        (
            icydb_diagnostic_code::DiagnosticFactTag::BudgetResource,
            resource.raw(),
        ),
    );
}

#[cfg(feature = "sql")]
fn assert_sql_query_fits_resource_limit(
    session: &DbSession<TestCanister>,
    sql: &str,
    resource: icydb_diagnostic_code::DiagnosticExecutionBudgetResource,
    limit: u64,
) {
    let budget = HardExecutionBudget::uniform_for_tests(
        u64::MAX,
        HardExecutionFailureHeadroom::new(500, 256),
    )
    .with_limit_for_tests(resource, limit);
    let context = HardExecutionContext::new(
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::TrustedRead,
        0x7371_6c2d_626f_756e,
    );
    with_query_execution_budget_for_tests(budget, context, || {
        session.execute_trusted_sql_query(sql)
    })
    .expect("bounded SQL execution should fit its physical-work limit");
}

#[cfg(feature = "sql")]
#[test]
fn planned_read_routes_share_physical_resource_accounting() {
    let session = initialize();
    let first = insert_exact_key_fixture(&session, 41);
    insert_exact_key_fixture(&session, 42);

    let fallback = crate::db::DynamicQuery::new(ENTITY_NAME)
        .filter(crate::db::FieldRef::new("id").eq(first))
        .select(["id", "payload"])
        .order_by(crate::db::asc("id"))
        .limit(1);
    assert_eq!(
        session
            .execute_trusted_live_page(&fallback, None)
            .expect("bounded fallback execution should preserve its result")
            .row_count,
        1,
    );
    assert_planned_query_exhausts(
        &session,
        &fallback,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
    );

    let covering = crate::db::DynamicQuery::new(ENTITY_NAME)
        .filter(crate::db::FieldRef::new("payload").eq(41_u64))
        .select(["payload"])
        .order_by(crate::db::asc("payload"))
        .limit(1);
    assert_eq!(
        session
            .execute_trusted_live_page(&covering, None)
            .expect("bounded covering execution should preserve its result")
            .row_count,
        1,
    );
    assert_planned_query_exhausts(
        &session,
        &covering,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
    );

    let residual = crate::db::DynamicQuery::new(ENTITY_NAME)
        .filter(crate::db::FieldRef::new("payload").eq_field("id"))
        .select(["id"])
        .order_by(crate::db::asc("id"))
        .limit(1);
    assert_eq!(
        session
            .execute_trusted_live_page(&residual, None)
            .expect("bounded residual execution should preserve its result")
            .row_count,
        0,
    );
    assert_planned_query_exhausts(
        &session,
        &residual,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
    );

    assert_planned_query_exhausts(
        &session,
        &fallback,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::ResultBytes,
    );

    let grouped = crate::db::DynamicQuery::new(ENTITY_NAME)
        .group_by("payload")
        .aggregate(crate::db::count())
        .order_by(crate::db::asc("payload"))
        .grouped_limits(10, 16 * 1_024)
        .limit(1);
    let grouped_result = session
        .execute_trusted_dynamic_grouped_query(&grouped)
        .expect("bounded grouped execution should preserve its result");
    assert_eq!(grouped_result.row_count, 1);
    assert!(grouped_result.next_cursor.is_some());
    assert_grouped_query_exhausts(
        &session,
        &grouped,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::GroupDistinctEntries,
    );
    assert_grouped_query_exhausts(
        &session,
        &grouped,
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::CursorSteps,
    );

    assert_sql_query_exhausts(
        &session,
        "SELECT payload, COUNT(*) AS row_count FROM IdentityRow \
         GROUP BY payload ORDER BY row_count DESC, payload ASC LIMIT 1",
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::SortEntries,
    );
}

#[cfg(feature = "sql")]
#[test]
fn mutation_execution_budget_exhaustion_terminalizes_forward_and_verify() {
    let (session, _root) = initialize_journaled_with_root();
    assert_eq!(insert_exact_key_fixture(&session, 41), 1);

    for (identity, sql, expected_phase) in [
        (
            91_u8,
            "UPDATE IdentityRow SET payload = 42 WHERE id = 1",
            MutationJobPhase::Forward,
        ),
        (
            92_u8,
            "UPDATE IdentityRow SET payload = 42 WHERE id = 999",
            MutationJobPhase::Verify,
        ),
    ] {
        let job_id = MutationJobId::try_from_bytes([identity; 32])
            .expect("budget fixture identity should admit");
        let mut state = session
            .start_trusted_sql_mutation_job(job_id, sql)
            .expect("budget fixture job should start");
        if expected_phase == MutationJobPhase::Verify {
            let forward = MutationJobAdvanceRequest::new(
                job_id,
                state.sequence,
                MutationJobIdempotencyKey::new(format!("budget-forward-{identity}"))
                    .expect("bounded Forward replay identity should admit"),
            );
            let receipt = session
                .advance_trusted_mutation_job(&forward)
                .expect("nonmatching Forward page should enter Verify");
            assert_eq!(receipt.phase, MutationJobPhase::Verify);
            state = session
                .mutation_job_state(job_id)
                .expect("Verify predecessor should remain readable");
        }
        assert_eq!(state.phase, expected_phase);

        let request = MutationJobAdvanceRequest::new(
            job_id,
            state.sequence,
            MutationJobIdempotencyKey::new(format!("budget-exhaust-{identity}"))
                .expect("bounded exhaustion replay identity should admit"),
        );
        let terminal = advance_with_exhausted_mutation_predicate_budget(&session, &request)
            .expect("admitted execution-budget failure should commit terminal progress");
        assert_eq!(
            terminal.status,
            MutationJobStatus::RestartRequired(
                MutationJobRestartReason::ExecutionBudgetPolicyExceeded,
            ),
        );
        assert_eq!(terminal.rows_updated, 0);
        assert_eq!(
            session.advance_trusted_mutation_job(&request),
            Ok(terminal.clone()),
            "exact terminal replay must not execute the exhausted page again",
        );
        assert_dynamic_payload(&session, 1, 41);
        session
            .acknowledge_mutation_job(job_id, terminal.committed_sequence)
            .expect("terminal budget fixture should acknowledge");
    }
}

fn assert_dynamic_payload<C: CanisterKind>(
    session: &DbSession<C>,
    key: u64,
    expected_payload: u64,
) {
    let unchanged = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(key),
            patch: dynamic_payload_patch(expected_payload),
        })
        .expect("the expected row should remain readable through a no-op update");
    assert_eq!(unchanged.affected_rows, 0);
    assert_eq!(
        unchanged.rows,
        vec![expected_dynamic_row(key, expected_payload)],
    );
}

fn assert_exact_batch_backlog_pressure(
    pressure: &InternalError,
    before: JournalTailControl,
    next_sequence: u64,
) {
    assert_eq!(
        pressure.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE,
    );
    assert_eq!(
        pressure.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::BacklogResource,
                icydb_diagnostic_code::DiagnosticBacklogResource::Batches.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::CurrentCount, 64),
            (icydb_diagnostic_code::DiagnosticFactTag::ProposedCount, 1),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 64),
        ],
    );
    assert_eq!(
        crate::db::commit::next_database_commit_sequence()
            .expect("pressure must leave the database sequence readable"),
        next_sequence,
    );
    assert!(matches!(
        crate::db::commit::observe_commit_control()
            .expect("pressure must leave commit control observable"),
        crate::db::commit::CommitControlObservation::Present {
            marker_present: false,
            ..
        },
    ));
    assert_eq!(
        JOURNALED_TAIL_STORE.with(|tail| {
            tail.borrow()
                .current_tail_control()
                .expect("pressure must preserve the exact tail control")
        }),
        before,
    );
}

fn batch(values: &[u64]) -> Vec<AcceptedStructuralMutation> {
    values
        .iter()
        .map(|value| {
            AcceptedStructuralMutation::save(
                MutationMode::Insert,
                AcceptedStructuralMutationTarget::ResolveFromAfterImage,
                payload_patch(*value),
            )
        })
        .collect()
}

fn atomic_progress_fixture(
    identity_byte: u8,
) -> (
    MutationJobRecord,
    MutationJobRecord,
    MutationProgressRecordOp,
) {
    let job_id = MutationJobId::try_from_bytes([identity_byte; 32])
        .expect("nonzero atomic progress job id should admit");
    let before = MutationJobRecord::new(job_id, vec![1, identity_byte], vec![2])
        .expect("atomic progress predecessor should admit");
    let request = MutationJobAdvanceRequest::new(
        job_id,
        0,
        MutationJobIdempotencyKey::new(format!("atomic-{identity_byte}"))
            .expect("atomic progress replay key should admit"),
    );
    let (after, _) = before
        .apply_transition(
            &request,
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![3],
                1,
                1,
                0,
            ),
        )
        .expect("atomic progress successor should admit");
    let operation = MutationProgressRecordOp::replace(&before, &after)
        .expect("atomic progress replacement should admit");
    (before, after, operation)
}

fn assert_mutation_facts(
    error: &InternalError,
    session: &DbSession<TestCanister>,
    tail: Vec<(icydb_diagnostic_code::DiagnosticFactTag, u64)>,
) {
    use icydb_diagnostic_code::DiagnosticFactTag as Tag;
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let fingerprint = catalog.fingerprint();
    let mut expected = vec![
        (
            Tag::AcceptedSchemaFingerprintMethod,
            u64::from(catalog.fingerprint_method_version()),
        ),
        (
            Tag::AcceptedSchemaFingerprintHigh,
            u64::from_be_bytes(fingerprint[..8].try_into().unwrap()),
        ),
        (
            Tag::AcceptedSchemaFingerprintLow,
            u64::from_be_bytes(fingerprint[8..].try_into().unwrap()),
        ),
    ];
    expected.extend(tail);
    assert_eq!(error.diagnostic_facts(), expected);
    assert_eq!(
        icydb_diagnostic_code::validate_known_diagnostic_fact_schema(
            error.diagnostic().error_code(),
            &expected,
        ),
        Ok(()),
    );
}

fn assert_identity_boundary(error: &InternalError) {
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Identity);
}

#[test]
fn generated_candidate_collision_is_identity_corruption_before_generic_uniqueness() {
    let generated = insert_key_exists_after_generation(true);
    assert_eq!(generated.class(), ErrorClass::Corruption);
    assert_eq!(generated.origin(), ErrorOrigin::Identity);

    let ordinary = insert_key_exists_after_generation(false);
    assert_ne!(ordinary.origin(), ErrorOrigin::Identity);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn pre_key_candidate_count_rejects_values_beyond_the_persisted_u32_bound() {
    let error = checked_pre_key_candidate_count(
        usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize should hold u32 + 1"),
    )
    .expect_err("candidate counts beyond u32 must reject");
    assert_identity_boundary(&error);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one holding lifecycle proves split, merge, transfer, late-failure neutrality, result order, and Identity state"
)]
fn mixed_structural_batch_preserves_holding_conservation_and_failure_atomicity() {
    let session = initialize();
    let seeded = session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
        .expect("seed rows should commit");
    assert_eq!(seeded.affected_rows, 1);

    let split = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(60),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(40),
            },
        ])
        .expect("one holding should split atomically");
    assert_eq!(
        split.iter().map(|result| result.affected_rows).sum::<u32>(),
        2,
    );
    assert_eq!(
        batch_rows(&split),
        vec![expected_dynamic_row(1, 60), expected_dynamic_row(2, 40),],
        "split after-images must retain input order and exact quantity",
    );

    let rejected_split = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(50),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: DynamicStructuralPatch::new(Vec::new()),
            },
        ])
        .expect_err("an invalid split output must reject the staged source update");
    assert_eq!(rejected_split.class(), ErrorClass::Unsupported);
    assert_eq!(rejected_split.origin(), ErrorOrigin::Executor);
    assert_mutation_facts(
        &rejected_split,
        &session,
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                ENTITY_TAG.value(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 2),
            (
                icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 1),
        ],
    );
    assert_dynamic_payload(&session, 1, 60);
    assert_dynamic_payload(&session, 2, 40);

    let transfer = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(70),
            },
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
                patch: dynamic_payload_patch(30),
            },
        ])
        .expect("distinct transfer patches should share one atomic batch");
    assert_eq!(
        batch_rows(&transfer),
        vec![expected_dynamic_row(1, 70), expected_dynamic_row(2, 30),],
        "the transfer must preserve the exact total quantity",
    );

    let merge = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
            },
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(100),
            },
        ])
        .expect("two holdings should merge atomically");
    assert_eq!(
        batch_rows(&merge),
        vec![expected_dynamic_row(2, 30), expected_dynamic_row(1, 100),],
        "delete before-images and update after-images must retain input order",
    );

    let resplit = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(60),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(40),
            },
        ])
        .expect("the merged holding should split again");
    assert_eq!(
        batch_rows(&resplit),
        vec![expected_dynamic_row(1, 60), expected_dynamic_row(3, 40),],
    );

    let rejected_merge = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(3),
            },
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(99),
                patch: dynamic_payload_patch(100),
            },
        ])
        .expect_err("a late missing merge target must preserve the earlier staged delete");
    assert_eq!(rejected_merge.class(), ErrorClass::NotFound);
    assert_dynamic_payload(&session, 1, 60);
    assert_dynamic_payload(&session, 3, 40);

    SCHEMA_STORE.with(|store| {
        let cursor = store
            .borrow()
            .identity_statement_cursor(
                database_incarnation_id().expect("database incarnation should remain readable"),
                ENTITY_TAG,
                FieldId::new(1),
                &AcceptedFieldKind::Nat64,
            )
            .expect("mixed Identity state should remain readable");
        assert_eq!(cursor.expected_high_water(), 3);
        assert!(!cursor.has_allocations());
    });
}

#[test]
fn mixed_structural_batch_rejects_duplicate_holding_targets_without_mutation() {
    let session = initialize();
    session
        .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(100)])
        .expect("the holding fixture should initialize");

    let duplicate = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(60),
            },
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
            },
        ])
        .expect_err("duplicate targets across operation kinds must reject");
    assert!(matches!(
        duplicate.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchDuplicateKey,
        }),
    ));
    assert_eq!(
        duplicate.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                ENTITY_TAG.value(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::FirstBatchPosition,
                0,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::DuplicateBatchPosition,
                1,
            ),
        ],
    );
    assert_dynamic_payload(&session, 1, 100);
}

#[test]
fn dynamic_insert_batch_checks_count_before_entity_resolution() {
    use icydb_diagnostic_code::{DiagnosticDetail, RuntimeBoundaryCode};

    let session = initialize();
    for (count, boundary) in [
        (0, RuntimeBoundaryCode::MutationBatchEmpty),
        (
            MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS + 1,
            RuntimeBoundaryCode::MutationBatchTooManyItems,
        ),
    ] {
        let error = session
            .execute_trusted_dynamic_insert_batch(
                "",
                (0..count).map(|_| dynamic_payload_patch(10)).collect(),
            )
            .expect_err("batch count must reject before the empty entity name");
        assert_eq!(
            error.diagnostic().detail(),
            Some(&DiagnosticDetail::RuntimeBoundary { boundary }),
        );
    }
    let error = session
        .execute_trusted_dynamic_insert_batch("", vec![dynamic_payload_patch(10)])
        .expect_err("an admitted count must still validate the entity name");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 0);
}

#[test]
fn dynamic_insert_batch_preserves_positions_atomicity_and_identity_order() {
    use icydb_diagnostic_code::DiagnosticFactTag;

    let session = initialize();
    let authored_identity = DynamicStructuralPatch::new(vec![(
        "id".to_string(),
        DynamicWriteCell::Value(InputValue::nat64(99)),
    )]);
    let error = session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(10), authored_identity],
        )
        .expect_err("a late generated-field write must reject during lowering");
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::BatchPosition, 1))
    );

    let wrong_type = DynamicStructuralPatch::new(vec![(
        "payload".to_string(),
        DynamicWriteCell::Value(InputValue::text("invalid".to_string())),
    )]);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(10), wrong_type],
        )
        .expect_err("late value validation must reject the complete staged batch");
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 0);

    let inserted = session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(10), dynamic_payload_patch(20)],
        )
        .expect("rejected batches must not consume generated identities");
    assert_eq!(inserted.affected_rows, 2);
    assert_eq!(
        inserted.rows,
        vec![
            vec![OutputValue::nat64(1), OutputValue::nat64(10)],
            vec![OutputValue::nat64(2), OutputValue::nat64(20)],
        ],
    );
}

#[test]
fn mixed_structural_batch_rejects_empty_and_over_bound_before_resolution() {
    let session = initialize();
    let empty = session
        .execute_trusted_dynamic_mutation_batch(Vec::new())
        .expect_err("an empty public batch must reject");
    assert!(matches!(
        empty.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchEmpty,
        }),
    ));
    assert_eq!(
        empty.diagnostic_facts(),
        vec![(icydb_diagnostic_code::DiagnosticFactTag::ActualCount, 0,)],
    );

    let requests = (0..=MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS)
        .map(|_| DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
        })
        .collect();
    let over_bound = session
        .execute_trusted_dynamic_mutation_batch(requests)
        .expect_err("operation cap plus one must reject before row resolution");
    assert!(matches!(
        over_bound.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchTooManyItems,
        }),
    ));
    assert_eq!(
        over_bound.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualCount,
                (MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS + 1) as u64,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                MAX_STRUCTURAL_MUTATION_BATCH_OPERATIONS as u64,
            ),
        ],
    );
}

#[test]
fn mixed_structural_batch_staged_byte_bound_uses_checked_exact_boundary() {
    assert_eq!(
        structural_mutation_staged_charge([11, 13, 17])
            .expect("the writer-owned formula should sum all three row-image components"),
        41,
    );
    let mut exact = 0;
    add_structural_mutation_staged_bytes(&mut exact, [MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES])
        .expect("the exact staged-byte boundary should admit");
    assert_eq!(exact, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES);

    let error = add_structural_mutation_staged_bytes(&mut exact, [1])
        .expect_err("one byte above the staged-byte boundary must reject");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchStagedBytesExceeded,
        }),
    ));
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                (MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES + 1) as u64,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES as u64,
            ),
        ],
    );

    let mut prefix = 0;
    assert_eq!(
        admit_structural_mutation_staged_charge(
            &mut prefix,
            [MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES],
            AcceptedStructuralMutationPacking::BoundedPrefix,
        )
        .expect("the exact prefix boundary should calculate"),
        AcceptedStructuralMutationStagedAdmission::Admitted,
    );
    assert_eq!(prefix, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES);
    assert_eq!(
        admit_structural_mutation_staged_charge(
            &mut prefix,
            [1],
            AcceptedStructuralMutationPacking::BoundedPrefix,
        )
        .expect("the next prefix candidate should calculate"),
        AcceptedStructuralMutationStagedAdmission::PageFull,
    );
    assert_eq!(prefix, MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES);

    let mut empty_prefix = 0;
    assert_eq!(
        admit_structural_mutation_staged_charge(
            &mut empty_prefix,
            [MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES + 1],
            AcceptedStructuralMutationPacking::BoundedPrefix,
        )
        .expect("one oversized candidate should classify without mutating the prefix"),
        AcceptedStructuralMutationStagedAdmission::CandidateExceedsPolicy,
    );
    assert_eq!(empty_prefix, 0);

    validate_structural_mutation_result_bytes(MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES)
        .expect("the exact result-byte boundary should admit");
    let error =
        validate_structural_mutation_result_bytes(MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES + 1)
            .expect_err("one byte above the result-byte boundary must reject");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::MutationBatchResultBytesExceeded,
        }),
    ));
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                (MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES + 1) as u64,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                MAX_STRUCTURAL_MUTATION_BATCH_RESULT_BYTES as u64,
            ),
        ],
    );
}

fn database_owned_default_snapshot() -> crate::db::schema::AcceptedSchemaSnapshot {
    use crate::db::schema::{AcceptedSchemaSnapshot, FieldWriteManagement};

    let policies = [
        (
            "id",
            AcceptedFieldKind::Nat64,
            Some(FieldInsertGeneration::Identity),
            None,
        ),
        (
            "generated",
            AcceptedFieldKind::Ulid,
            Some(FieldInsertGeneration::Ulid),
            None,
        ),
        (
            "created_at",
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::CreatedAt),
        ),
        (
            "updated_at",
            AcceptedFieldKind::Timestamp,
            None,
            Some(FieldWriteManagement::UpdatedAt),
        ),
        ("payload", AcceptedFieldKind::Nat64, None, None),
    ];
    let fields: Vec<_> = policies
        .into_iter()
        .enumerate()
        .map(|(index, (name, kind, generation, management))| {
            let codec = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
            PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(u32::try_from(index).unwrap() + 1),
                name.to_string(),
                SchemaFieldSlot::new(u16::try_from(index).unwrap()),
                kind,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(generation, management),
                FieldStorageDecode::ByKind,
                codec,
            )
        })
        .collect();
    AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY_SOURCE.to_string(),
        ENTITY_NAME.to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    ))
    .expect("owned-field schema should admit")
}

#[test]
fn structural_cells_preserve_default_intent_and_database_ownership() {
    use super::{AcceptedMutationIntentPatch, FieldSlot, lower_resolved_write_cell};
    use crate::{db::schema::AcceptedRowLayoutRuntimeContract, error::MutationDiagnosticContext};

    let snapshot = database_owned_default_snapshot();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&snapshot).unwrap();
    for index in 0..5 {
        let field = descriptor.field_for_slot_index(index).unwrap();
        let slot = FieldSlot::from_validated_index(index);
        let authored = match index {
            1 => InputValue::ulid(crate::types::Ulid::from_u128(7)),
            2 | 3 => InputValue::timestamp(Timestamp::from_millis(7)),
            _ => InputValue::nat64(7),
        };
        for mode in [
            MutationMode::Insert,
            MutationMode::Replace,
            MutationMode::Update,
        ] {
            let context = MutationDiagnosticContext::new(
                crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                [7; 16],
                ENTITY_TAG.value(),
                mode.diagnostic_operation(),
                3,
            );
            for cell in [
                DynamicWriteCell::Omitted,
                DynamicWriteCell::Default,
                DynamicWriteCell::Null,
                DynamicWriteCell::Value(authored.clone()),
            ] {
                let expected = match &cell {
                    DynamicWriteCell::Omitted => Some(AcceptedMutationIntentPatch::new()),
                    DynamicWriteCell::Default
                        if matches!(mode, MutationMode::Insert)
                            || (index == 4 && matches!(mode, MutationMode::Replace)) =>
                    {
                        Some(AcceptedMutationIntentPatch::new().set_explicit_insert_default(slot))
                    }
                    DynamicWriteCell::Default if index == 4 => {
                        Some(AcceptedMutationIntentPatch::new().set_explicit_update_default(slot))
                    }
                    DynamicWriteCell::Null if index == 4 => Some(
                        AcceptedMutationIntentPatch::new().set_authored(slot, InputValue::null()),
                    ),
                    DynamicWriteCell::Value(value) if index == 4 => {
                        Some(AcceptedMutationIntentPatch::new().set_authored(slot, value.clone()))
                    }
                    _ => None,
                };
                let result = lower_resolved_write_cell(
                    AcceptedMutationIntentPatch::new(),
                    field,
                    cell,
                    mode,
                    context,
                );
                if let Some(expected) = expected {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let error = result.unwrap_err();
                    let expected = InternalError::mutation_database_owned_field_explicit(
                        context,
                        field.field_id().get(),
                    );
                    assert_eq!(error.diagnostic(), expected.diagnostic());
                    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
                }
            }
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one lifecycle proves shared materialization and every maintained frontend against the same zero-state owner"
)]
#[test]
fn identity_insert_frontends_share_one_committed_range_without_rejected_consumption() {
    let session = initialize();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("identity catalog should resolve");
    let initial_description = session
        .try_describe_entity_by_name(ENTITY_NAME)
        .expect("accepted Identity description should resolve");
    assert_eq!(
        initial_description.entity_tag(),
        catalog.identity().entity_tag().value()
    );
    assert_eq!(
        initial_description.accepted_schema_fingerprint_method(),
        catalog.fingerprint_method_version()
    );
    assert_eq!(
        initial_description.accepted_schema_fingerprint(),
        catalog.fingerprint()
    );
    let initial_identity = initial_description
        .identity()
        .expect("accepted Identity policy should be described");
    assert_eq!(initial_identity.field(), "id");
    assert_eq!(initial_identity.generator(), "Identity::next");
    assert_eq!(initial_identity.accepted_kind(), "nat64");
    assert_eq!(initial_identity.minimum(), 1);
    assert_eq!(initial_identity.maximum(), u128::from(u64::MAX));
    assert_eq!(initial_identity.high_water(), 0);
    assert_eq!(initial_identity.remaining(), u128::from(u64::MAX));
    assert!(!initial_identity.exhausted());

    let rejected = session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[1_000, 2_000]),
            Timestamp::from_millis(6),
            |_| Err::<(), _>(InternalError::executor_unsupported()),
        )
        .expect_err("a rejected precommit result must not publish its tentative range");
    assert_eq!(rejected.class(), ErrorClass::Unsupported);
    assert_eq!(DATA_STORE.with(|store| store.borrow().len()), 0);

    let rows = session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[10, 20, 30]),
            Timestamp::from_millis(7),
            Ok,
        )
        .expect("one accepted batch should commit rows and one identity range");
    assert_eq!(
        rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
        vec![
            vec![Value::Nat64(1), Value::Nat64(10)],
            vec![Value::Nat64(2), Value::Nat64(20)],
            vec![Value::Nat64(3), Value::Nat64(30)],
        ],
    );

    let dynamic = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: DynamicStructuralPatch::new(vec![(
                "payload".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(40)),
            )]),
        })
        .expect("dynamic omission should commit through shared Identity generation");
    assert_eq!(dynamic.affected_rows, 1);

    let dynamic_default = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".to_string(), DynamicWriteCell::Default),
                (
                    "payload".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(45)),
                ),
            ]),
        })
        .expect("dynamic DEFAULT should use accepted Identity generation");
    assert_eq!(dynamic_default.affected_rows, 1);
    assert_eq!(dynamic_default.rows, vec![expected_dynamic_row(5, 45)]);

    for (request, operation) in [
        (
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "id".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(41)),
                    ),
                    (
                        "payload".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(42)),
                    ),
                ]),
            },
            icydb_diagnostic_code::DiagnosticMutationOperation::Insert,
        ),
        (
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: DynamicStructuralPatch::new(vec![(
                    "id".to_string(),
                    DynamicWriteCell::Default,
                )]),
            },
            icydb_diagnostic_code::DiagnosticMutationOperation::Update,
        ),
    ] {
        let error = session
            .execute_trusted_dynamic_mutation(&request)
            .expect_err("structural Identity authorship and regeneration must reject");
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Executor);
        assert_mutation_facts(
            &error,
            &session,
            vec![
                (
                    icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                    ENTITY_TAG.value(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 1),
                (
                    icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                    operation.raw(),
                ),
                (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0),
            ],
        );
    }

    let binding = session
        .issue_typed_entity_binding(&TYPED_DESCRIPTOR)
        .expect("typed output should bind the Identity field");
    let typed_patch = binding
        .bind_write_ordinals(vec![(1, DynamicWriteCell::Value(InputValue::nat64(50)))])
        .expect("typed payload should lower");
    let typed = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Insert { patch: typed_patch },
        )
        .expect("typed omission should commit through shared Identity generation");
    assert_eq!(
        typed
            .expect("typed insert should return one mutation result")
            .affected_rows,
        1,
    );
    let typed_default_patch = binding
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Default),
            (1, DynamicWriteCell::Value(InputValue::nat64(55))),
        ])
        .expect("typed DEFAULT intent should lower");
    let typed_default = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Insert {
                patch: typed_default_patch,
            },
        )
        .expect("typed DEFAULT should use accepted Identity generation")
        .expect("typed insert should return one mutation result");
    assert_eq!(typed_default.affected_rows, 1);
    assert_eq!(typed_default.rows, vec![expected_dynamic_row(7, 55)]);
    let explicit_typed_patch = binding
        .bind_write_ordinals(vec![
            (0, DynamicWriteCell::Value(InputValue::nat64(51))),
            (1, DynamicWriteCell::Value(InputValue::nat64(52))),
        ])
        .expect("the low-level binding should retain exact authored intent");
    let explicit_typed_error = session
        .execute_trusted_typed_mutation(
            &binding,
            DynamicTypedMutation::Insert {
                patch: explicit_typed_patch,
            },
        )
        .expect_err("typed Identity authorship must reject before allocation");
    assert_eq!(explicit_typed_error.class(), ErrorClass::Unsupported);
    assert_eq!(explicit_typed_error.origin(), ErrorOrigin::Executor);
    assert_mutation_facts(
        &explicit_typed_error,
        &session,
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::EntityTag,
                ENTITY_TAG.value(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::FieldId, 1),
            (
                icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                icydb_diagnostic_code::DiagnosticMutationOperation::Insert.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 0),
        ],
    );

    let replace_error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Replace {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(99),
            patch: DynamicStructuralPatch::new(vec![(
                "payload".to_string(),
                DynamicWriteCell::Value(InputValue::nat64(60)),
            )]),
        })
        .expect_err("save-as-insert with a chosen Identity must reject");
    assert_eq!(replace_error.class(), ErrorClass::Unsupported);
    assert_eq!(replace_error.origin(), ErrorOrigin::Executor);

    #[cfg(feature = "sql")]
    {
        for sql in [
            "INSERT INTO IdentityRow (payload) VALUES (70) RETURNING id, payload",
            "INSERT INTO IdentityRow (id, payload) VALUES (DEFAULT, 80) RETURNING id",
        ] {
            let _result = session
                .execute_trusted_sql_mutation(sql)
                .expect("SQL omission and DEFAULT should commit Identity generation");
        }

        let error = session
            .execute_trusted_sql_mutation("INSERT INTO IdentityRow (id, payload) VALUES (42, 90)")
            .expect_err("an explicit SQL Identity value must reject before allocation");
        let diagnostic = error.diagnostic();
        assert_eq!(
            diagnostic.code(),
            icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
        );
        assert!(matches!(
            diagnostic.detail(),
            Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
                boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::ExplicitGeneratedField,
            }),
        ));
    }

    let expected_committed = if cfg!(feature = "sql") { 9 } else { 7 };
    assert_eq!(
        DATA_STORE.with(|store| store.borrow().len()),
        expected_committed
    );
    SCHEMA_STORE.with(|store| {
        let cursor = store
            .borrow()
            .identity_statement_cursor(
                database_incarnation_id().expect("database incarnation should remain readable"),
                ENTITY_TAG,
                FieldId::new(1),
                &AcceptedFieldKind::Nat64,
            )
            .expect("committed writes must leave active state readable");
        assert_eq!(cursor.expected_high_water(), u128::from(expected_committed),);
        assert!(!cursor.has_allocations());
    });
    let committed_description = session
        .try_describe_entity_by_name(ENTITY_NAME)
        .expect("committed Identity description should resolve");
    let committed_identity = committed_description
        .identity()
        .expect("accepted Identity policy should remain described");
    assert_eq!(
        committed_identity.high_water(),
        u128::from(expected_committed),
    );
    assert_eq!(
        committed_identity.remaining(),
        u128::from(u64::MAX - expected_committed),
    );
    assert!(!committed_identity.exhausted());
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one ordered scenario proves target/progress atomicity, every interruption wake-up, state-only admission, and successful no-op wake-up behavior"
)]
fn mutation_progress_and_target_rows_recover_as_one_marker_transition() {
    let session = initialize_journaled();
    let initial_entity_revision = JOURNALED_TAIL_STORE
        .with(|tail| tail.borrow().entity_mutation_revision(ENTITY_TAG))
        .expect("direct initial schema publication must install entity revision authority");
    assert_eq!(initial_entity_revision, 1);
    install_startup_recovery_wakeup(record_startup_wakeup);
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled atomic-progress catalog should resolve");

    for (ordinal, interruption) in [
        MutationCommitInterruption::MarkerPersisted,
        MutationCommitInterruption::JournalPublished,
        MutationCommitInterruption::RowsPublished,
        MutationCommitInterruption::ProgressReplaced,
    ]
    .into_iter()
    .enumerate()
    {
        let identity_byte = 31 + u8::try_from(ordinal).expect("small ordinal should fit");
        let (before, after, operation) = atomic_progress_fixture(identity_byte);
        with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            match store.insert_mutation(&before)? {
                InsertMutationJobResult::Inserted => Ok(()),
                InsertMutationJobResult::Occupied(_) => {
                    Err(crate::db::MutationJobError::IdentityConflict)
                }
            }
        })
        .expect("atomic predecessor should insert once");

        let wakeups_before = STARTUP_WAKEUPS.with(Cell::get);
        interrupt_next_mutation_commit_for_tests(interruption);
        let interrupted = session.execute_accepted_structural_update_with_mutation_progress(
            &catalog,
            batch(&[700 + u64::try_from(ordinal).expect("small ordinal should fit")]),
            Timestamp::from_millis(17),
            operation,
        );
        assert!(
            interrupted.is_err(),
            "selected atomic boundary should interrupt"
        );
        assert_eq!(
            STARTUP_WAKEUPS.with(Cell::get),
            wakeups_before.saturating_add(1),
            "a normally returned retained-marker error must register its wake-up",
        );

        forget_recovered_domain_for_tests(&session.db)
            .expect("interruption should reset volatile recovery ownership");
        let retained_before = with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            store.load_mutation(before.state().job_id)
        })
        .expect("pre-driver progress should load");
        let row_count_before = JOURNALED_DATA_STORE.with(|store| store.borrow().len());
        let pending = session
            .db
            .ensure_recovered_state()
            .expect_err("ordinary admission must not drive retained-marker recovery");
        assert_eq!(
            pending.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
        );
        assert_eq!(
            with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
                store.load_mutation(before.state().job_id)
            })
            .expect("post-admission progress should load"),
            retained_before,
        );
        assert_eq!(
            JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
            row_count_before,
            "state-only admission must not mutate target rows",
        );
        drive_journaled_recovery_to_completion(&session);
        let retained = with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            store.load_mutation(before.state().job_id)
        })
        .expect("recovered successor should load");
        assert_eq!(retained, after);
        assert_eq!(
            JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
            u64::try_from(ordinal + 1).expect("small row count should fit"),
        );
        assert_eq!(
            JOURNALED_TAIL_STORE
                .with(|tail| tail.borrow().entity_mutation_revision(ENTITY_TAG))
                .expect("recovery must publish the target entity revision"),
            initial_entity_revision
                + u64::try_from(ordinal + 1).expect("small revision delta should fit"),
            "target rows, entity revision, and progress must recover as one transition",
        );
    }

    let (before, after, operation) = atomic_progress_fixture(39);
    with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
        match store.insert_mutation(&before)? {
            InsertMutationJobResult::Inserted => Ok(()),
            InsertMutationJobResult::Occupied(_) => {
                Err(crate::db::MutationJobError::IdentityConflict)
            }
        }
    })
    .expect("final predecessor should insert once");
    let wakeups_before_success = STARTUP_WAKEUPS.with(Cell::get);
    session
        .execute_accepted_structural_update_with_mutation_progress(
            &catalog,
            batch(&[799]),
            Timestamp::from_millis(18),
            operation,
        )
        .expect("uninterrupted atomic transition should clear its marker");
    assert_eq!(
        STARTUP_WAKEUPS.with(Cell::get),
        wakeups_before_success.saturating_add(1),
        "a successful retained commit must request online convergence",
    );
    let retained = with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
        store.load_mutation(before.state().job_id)
    })
    .expect("final successor should load");
    assert_eq!(retained, after);
    forget_recovered_domain_for_tests(&session.db)
        .expect("post-clear recovery ownership should reset");
    let pending = session
        .db
        .ensure_recovered_state()
        .expect_err("an upgrade epoch must remain gated until its driver runs");
    assert_eq!(
        pending.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    drive_journaled_recovery_to_completion(&session);
    assert_eq!(
        JOURNALED_TAIL_STORE
            .with(|tail| tail.borrow().entity_mutation_revision(ENTITY_TAG))
            .expect("uninterrupted transition must retain its entity revision"),
        initial_entity_revision + 5,
    );
}

fn assert_mixed_entity_recovered_state(session: &DbSession<JournaledTestCanister>) {
    for (entity_name, payload) in [
        (ENTITY_NAME, 100_u64),
        (SECOND_ENTITY_NAME, 1_100),
        (THIRD_ENTITY_NAME, 2_100),
    ] {
        let result = session
            .execute_trusted_live_page(
                &DynamicQuery::new(entity_name)
                    .filter(crate::db::FieldRef::new("payload").eq(payload))
                    .select(["id", "payload"])
                    .order_by(crate::db::asc("id"))
                    .limit(64),
                None,
            )
            .expect("every recovered mixed entity should remain queryable");
        assert_eq!(result.rows.len(), 1);
    }
    let retained_relation = session
        .execute_trusted_dynamic_mutation_batch(vec![DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(1),
        }])
        .expect_err("the recovered reverse relation must protect its target");
    assert!(retained_relation.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
    JOURNALED_SCHEMA_STORE.with(|store| {
        let store = store.borrow();
        for entity_tag in [ENTITY_TAG, SECOND_ENTITY_TAG, THIRD_ENTITY_TAG] {
            let cursor = store
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    entity_tag,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("every mixed Identity owner should remain readable");
            assert_eq!(cursor.expected_high_water(), 1);
            assert!(!cursor.has_allocations());
        }
    });
    JOURNALED_TAIL_STORE.with(|tail| {
        let tail = tail.borrow();
        assert_eq!(
            tail.entity_mutation_revision(ENTITY_TAG)
                .expect("first entity revision should remain readable"),
            2,
        );
        assert_eq!(
            tail.entity_mutation_revision(SECOND_ENTITY_TAG)
                .expect("second entity revision should remain readable"),
            2,
        );
        assert_eq!(
            tail.entity_mutation_revision(THIRD_ENTITY_TAG)
                .expect("third entity revision should remain readable"),
            2,
        );
    });
}

fn assert_mixed_entity_recovery(interruption: MutationCommitInterruption) {
    let session = initialize_journaled_multi_entity();
    interrupt_next_mutation_commit_for_tests(interruption);
    let interrupted = session.execute_trusted_dynamic_mutation_batch(vec![
        DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(100),
        },
        DynamicMutation::Insert {
            entity: SECOND_ENTITY_NAME.to_string(),
            patch: related_dynamic_payload_patch(1_100, 1),
        },
        DynamicMutation::Insert {
            entity: THIRD_ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(2_100),
        },
    ]);
    let interruption_error =
        interrupted.expect_err("the selected marker boundary should interrupt");
    assert_eq!(interruption_error.class(), ErrorClass::InvariantViolation);
    if interruption == MutationCommitInterruption::MarkerPersisted {
        let (marker_bytes, journal_batch_bytes) =
            crate::db::commit::retained_commit_marker_measurement_for_tests()
                .expect("the retained marker measurement should remain readable")
                .expect("marker persistence should retain one marker");
        assert_eq!(marker_bytes, 770);
        assert_eq!(journal_batch_bytes, vec![740]);
    }
    if interruption != MutationCommitInterruption::MarkerPersisted {
        let retained_batch = JOURNALED_TAIL_STORE.with(|tail| {
            let tail = tail.borrow();
            let watermark = tail
                .fold_watermark()
                .expect("the interrupted fold watermark should decode")
                .highest_folded_journal_sequence();
            tail.next_batch_after(watermark)
                .expect("the interrupted journal tail should decode")
                .expect("the interrupted marker should publish one journal batch")
        });
        let row_paths = retained_batch
            .records()
            .iter()
            .filter_map(|record| match record {
                JournalRecord::RowPut { entity_path, .. }
                | JournalRecord::RowDelete { entity_path, .. } => Some(entity_path.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            row_paths,
            vec![ENTITY_SOURCE, SECOND_ENTITY_SOURCE, THIRD_ENTITY_SOURCE],
        );
    }

    forget_recovered_domain_for_tests(&session.db)
        .expect("the retained mixed marker should reset volatile recovery ownership");
    drive_journaled_recovery_to_completion(&session);
    assert_mixed_entity_recovered_state(&session);
}

#[test]
fn mixed_entity_recovery_after_marker_persistence() {
    assert_mixed_entity_recovery(MutationCommitInterruption::MarkerPersisted);
}

#[test]
fn mixed_entity_recovery_after_journal_publication() {
    assert_mixed_entity_recovery(MutationCommitInterruption::JournalPublished);
}

#[test]
fn mixed_entity_recovery_after_row_prefix_publication() {
    assert_mixed_entity_recovery(MutationCommitInterruption::RowPrefixPublished);
}

#[test]
fn mixed_entity_recovery_after_all_rows_publish() {
    assert_mixed_entity_recovery(MutationCommitInterruption::RowsPublished);
}

#[test]
fn mixed_entity_recovery_after_state_materialization() {
    assert_mixed_entity_recovery(MutationCommitInterruption::StateMaterialized);
}

#[test]
fn mutation_progress_neither_side_mismatch_blocks_recovery() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled corruption catalog should resolve");
    let (before, _after, operation) = atomic_progress_fixture(41);
    with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
        match store.insert_mutation(&before)? {
            InsertMutationJobResult::Inserted => Ok(()),
            InsertMutationJobResult::Occupied(_) => {
                Err(crate::db::MutationJobError::IdentityConflict)
            }
        }
    })
    .expect("corruption predecessor should insert once");

    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::MarkerPersisted);
    assert!(
        session
            .execute_accepted_structural_update_with_mutation_progress(
                &catalog,
                batch(&[811]),
                Timestamp::from_millis(19),
                operation,
            )
            .is_err(),
        "marker interruption should retain recovery authority",
    );
    let (unexpected, _) = before
        .apply_transition(
            &MutationJobAdvanceRequest::new(
                before.state().job_id,
                0,
                MutationJobIdempotencyKey::new("unexpected-third-state")
                    .expect("unexpected replay key should admit"),
            ),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![99],
                2,
                0,
                0,
            ),
        )
        .expect("unexpected but valid progress state should admit");
    with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
        store.replace_mutation(&unexpected)
    })
    .expect("test should install the neither-side state");

    forget_recovered_domain_for_tests(&session.db)
        .expect("corrupt recovery ownership should reset");
    let error = session
        .db
        .drive_startup_recovery_page()
        .expect_err("neither-side progress must block recovery");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Recovery);
    assert_eq!(
        with_mutation_progress_store::<JournaledTestCanister, _>(|store| {
            store.load_mutation(before.state().job_id)
        })
        .expect("unexpected state should remain inspectable to the test"),
        unexpected,
    );
    assert!(
        session.db.drive_startup_recovery_page().is_err(),
        "a retained corrupt marker must continue blocking database access",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one ordered scenario exercises every durable interruption boundary, guarded recovery, derived rebuild, and both integrity tiers"
)]
fn journaled_identity_recovery_quiesces_every_publication_interruption_before_reallocation() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled identity catalog should resolve");

    for (ordinal, interruption) in [
        MutationCommitInterruption::MarkerPersisted,
        MutationCommitInterruption::JournalPublished,
        MutationCommitInterruption::RowsPublished,
        MutationCommitInterruption::StateMaterialized,
    ]
    .into_iter()
    .enumerate()
    {
        interrupt_next_mutation_commit_for_tests(interruption);
        let interrupted = session.execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[u64::try_from(ordinal).expect("ordinal should fit")]),
            Timestamp::from_millis(8),
            Ok,
        );
        assert!(
            interrupted.is_err(),
            "the selected durable boundary should interrupt",
        );

        let Err(pending) = session.execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[100 + u64::try_from(ordinal).expect("ordinal should fit")]),
            Timestamp::from_millis(9),
            Ok,
        ) else {
            panic!("ordinary mutation must not drive retained-marker recovery");
        };
        assert_eq!(
            pending.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
        );
        drive_journaled_recovery_to_completion(&session);

        let committed = session
            .execute_accepted_structural_save_batch(
                &catalog,
                true,
                batch(&[100 + u64::try_from(ordinal).expect("ordinal should fit")]),
                Timestamp::from_millis(9),
                Ok,
            )
            .expect("the next mutation must recover before allocating");
        let expected_high_water =
            u64::try_from((ordinal + 1) * 2).expect("small test high-water should fit");
        assert_eq!(
            committed
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![vec![
                Value::Nat64(expected_high_water),
                Value::Nat64(100 + u64::try_from(ordinal).expect("ordinal should fit")),
            ]],
        );
        assert_eq!(
            JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
            expected_high_water,
        );
        JOURNALED_SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("guarded recovery must leave quiescent active state");
            assert_eq!(
                cursor.expected_high_water(),
                u128::from(expected_high_water),
            );
            assert!(!cursor.has_allocations());
        });
    }

    for (ordinal, (interruption, deleted_key)) in [
        (MutationCommitInterruption::MarkerPersisted, 2),
        (MutationCommitInterruption::JournalPublished, 4),
        (MutationCommitInterruption::RowPrefixPublished, 6),
        (MutationCommitInterruption::RowsPublished, 8),
        (MutationCommitInterruption::StateMaterialized, 7),
    ]
    .into_iter()
    .enumerate()
    {
        let expected_payload =
            501 + u64::try_from(ordinal).expect("small interruption ordinal should fit");
        interrupt_next_mutation_commit_for_tests(interruption);
        let interrupted = session.execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(expected_payload),
            },
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(deleted_key),
            },
        ]);
        assert!(
            interrupted.is_err(),
            "the selected caller-key mixed publication boundary should interrupt",
        );
        let pending = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(expected_payload),
            })
            .expect_err("ordinary update must not drive retained-marker recovery");
        assert_eq!(
            pending.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
        );
        drive_journaled_recovery_to_completion(&session);
        let recovered_update = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(expected_payload),
            })
            .expect("guarded reentry should complete the marker-authorized mixed batch");
        assert_eq!(
            recovered_update.affected_rows, 0,
            "the recovered update must already expose its admitted final image",
        );
        let recovered_delete = session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(deleted_key),
            })
            .expect_err("the recovered delete must already be materialized");
        assert_eq!(recovered_delete.class(), ErrorClass::NotFound);
        JOURNALED_SCHEMA_STORE.with(|store| {
            let cursor = store
                .borrow()
                .identity_statement_cursor(
                    database_incarnation_id().expect("database incarnation should remain readable"),
                    ENTITY_TAG,
                    FieldId::new(1),
                    &AcceptedFieldKind::Nat64,
                )
                .expect("caller-key recovery must preserve active Identity state");
            assert_eq!(cursor.expected_high_water(), 8);
            assert!(!cursor.has_allocations());
        });
    }

    forget_recovered_domain_for_tests(&session.db)
        .expect("the final journal tail should remain recoverable");
    session
        .db
        .drive_startup_recovery_page()
        .expect("derived rebuild must not allocate another identity");

    let data_generation = JOURNALED_DATA_STORE.with(|store| store.borrow().generation());
    let index_generation = JOURNALED_INDEX_STORE.with(|store| store.borrow().generation());
    let data_len = JOURNALED_DATA_STORE.with(|store| store.borrow().len());
    let index_len = JOURNALED_INDEX_STORE.with(|store| store.borrow().len());
    forget_recovered_domain_for_tests(&session.db)
        .expect("an empty-tail upgrade should reset recovery ownership");
    session
        .db
        .drive_startup_recovery_page()
        .expect("an empty-tail upgrade should admit without rebuilding stored rows or indexes");
    assert_eq!(
        JOURNALED_DATA_STORE.with(|store| store.borrow().generation()),
        data_generation
            .checked_add(1)
            .expect("test generation should advance once"),
        "empty-tail recovery must reset the disposable row projection exactly once",
    );
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| store.borrow().generation()),
        index_generation
            .checked_add(1)
            .expect("test generation should advance once"),
        "empty-tail recovery must reset the disposable index projection exactly once",
    );
    assert_eq!(
        JOURNALED_DATA_STORE.with(|store| store.borrow().len()),
        data_len,
        "empty-tail recovery must not rebuild or remove authoritative rows",
    );
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| store.borrow().len()),
        index_len,
        "empty-tail recovery must not clear or rebuild canonical secondary indexes",
    );

    let quick = execute_quick_integrity(
        &session.db,
        catalog.inspection_plan(),
        catalog.runtime_root_identity().database_incarnation(),
    )
    .expect("quiescent Identity control inventory should be inspectable");
    assert_eq!(quick.status(), &QuickIntegrityStatus::CompleteClean);
    let row_page = execute_row_integrity_page(
        &session.db,
        catalog.inspection_plan(),
        PhysicalUnitCheckpoint::BeforeFirst,
        RowInspectionLimits::standard(),
    )
    .expect("Identity rows should remain within committed high-water");
    assert!(row_page.exhausted());
    assert!(row_page.findings().is_empty());

    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 3);
    assert!(
        JOURNALED_INDEX_STORE.with(|store| !store.borrow().is_empty()),
        "derived index rebuild should restore witnesses without allocating identities",
    );
    assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
    JOURNALED_SCHEMA_STORE.with(|store| {
        let cursor = store
            .borrow()
            .identity_statement_cursor(
                database_incarnation_id().expect("database incarnation should remain readable"),
                ENTITY_TAG,
                FieldId::new(1),
                &AcceptedFieldKind::Nat64,
            )
            .expect("folded identity state should reopen without allocating");
        assert_eq!(cursor.expected_high_water(), 8);
        assert!(!cursor.has_allocations());
    });
}

#[test]
fn journaled_online_convergence_drains_the_full_backlog_in_complete_batch_callbacks_without_reallocating_ids()
 {
    const SUBMISSION: &str = "generated/8899aabbccddeeff";
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled identity catalog should resolve");

    for payload in 0_u64..64 {
        session
            .execute_accepted_structural_save_batch(
                &catalog,
                true,
                batch(&[payload]),
                Timestamp::from_millis(8),
                Ok,
            )
            .unwrap_or_else(|error| {
                panic!("journaled identity fixture row {payload} should commit: {error:?}")
            });
    }

    let before = JOURNALED_TAIL_STORE.with(|tail| {
        tail.borrow()
            .current_tail_control()
            .expect("online backlog control should remain valid")
    });
    assert_eq!(before.batch_count(), 64);
    let next_sequence = crate::db::commit::next_database_commit_sequence()
        .expect("database sequence preview should remain readable");
    let Err(pressure) = session.execute_accepted_structural_save_batch(
        &catalog,
        true,
        batch(&[64]),
        Timestamp::from_millis(8),
        Ok,
    ) else {
        panic!("the exact cumulative batch ceiling should reject one more batch")
    };
    assert_exact_batch_backlog_pressure(&pressure, before, next_sequence);

    for folded_batches in 1..=64 {
        let complete = session
            .db
            .drive_startup_recovery_page()
            .expect("online complete-batch callback should commit");
        assert_eq!(complete, folded_batches == 64);
    }

    assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
    session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[64]),
            Timestamp::from_millis(8),
            Ok,
        )
        .expect("drain should make the rejected mutation retryable");
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the retry tail should converge"),
    );

    assert_eq!(
        drive_generated_startup_recovery_page(&session, &JOURNALED_STORE_REGISTRY, SUBMISSION,)
            .expect("online convergence should commit"),
        GeneratedStartupDriverStep::ApplyGeneratedSchema,
        "journal convergence does not complete an unsubmitted generated schema",
    );
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the drained journal should remain quiescent"),
    );

    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 65);
    assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
    assert_dynamic_payload(&session, 1, 0);
    assert_dynamic_payload(&session, 65, 64);
    JOURNALED_SCHEMA_STORE.with(|store| {
        let cursor = store
            .borrow()
            .identity_statement_cursor(
                database_incarnation_id().expect("database incarnation should remain readable"),
                ENTITY_TAG,
                FieldId::new(1),
                &AcceptedFieldKind::Nat64,
            )
            .expect("online convergence must preserve active Identity state");
        assert_eq!(cursor.expected_high_water(), 65);
        assert!(!cursor.has_allocations());
    });
}

#[test]
fn journaled_online_convergence_reconstructs_same_key_batches_from_canonical_predecessors() {
    let session = initialize_journaled();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(10),
        })
        .expect("the initial positioned row should commit");
    for payload in [20, 30] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(payload),
            })
            .unwrap_or_else(|error| {
                panic!("the positioned same-key update should commit: {error:?}")
            });
    }

    assert_dynamic_payload(&session, 1, 30);
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| store.borrow().len()),
        1,
        "the newest live index effect should hide every predecessor",
    );
    for folded_batches in 1..=3 {
        let complete = session
            .db
            .drive_startup_recovery_page()
            .expect("the positioned same-key batch should converge");
        assert_eq!(complete, folded_batches == 3);
    }

    assert_dynamic_payload(&session, 1, 30);
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| store.borrow().len()),
        1,
        "canonical derived state must contain only the newest membership",
    );
    assert!(!JOURNALED_TAIL_STORE.with(|tail| tail.borrow().has_stored_batch()));
}

#[test]
fn ready_cardinality_combines_durable_base_with_exact_live_delta_and_fold_maintenance() {
    let session = initialize_journaled();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(10),
        })
        .expect("initial cardinality row should commit");
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("initial cardinality row should fold"),
    );
    drive_journaled_cardinality_to_ready(&session);
    let handle = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled cardinality store should resolve");
    let (index_id, prefix_components) = journaled_user_index_prefix();
    reset_journaled_cardinality_projections();
    assert_eq!(
        JOURNALED_DATA_STORE.with(|store| store.borrow().exact_entity_count(ENTITY_TAG)),
        None,
        "the reopened-style volatile full count must remain unavailable",
    );
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 1);

    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(10),
        })
        .expect("post-Ready row should commit into the live overlay");
    for payload in [20, 10] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
                patch: dynamic_payload_patch(payload),
            })
            .expect("same-key post-Ready overlay should commit");
    }
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 2);
    for folded in 1..=3 {
        let complete = session
            .db
            .drive_startup_recovery_page()
            .expect("post-Ready row should fold with exact maintenance");
        assert_eq!(complete, folded == 3);
        assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 2);
    }
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 2);
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: ENTITY_NAME.to_string(),
            key: InputValue::nat64(2),
        })
        .expect("post-Ready delete should commit into the live overlay");
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 1);
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("post-Ready delete should fold with exact maintenance"),
    );
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 1);
    mark_journaled_cardinality_building();
    assert_eq!(
        handle.exact_entity_count(ENTITY_TAG),
        None,
        "non-Ready evidence must select the conservative path",
    );
    #[cfg(feature = "sql")]
    {
        let data_reads_before = DataStore::current_get_call_count();
        let crate::db::SqlStatementResult::Projection { rows, .. } = session
            .execute_trusted_sql_query("SELECT COUNT(*) FROM IdentityRow")
            .expect("non-Ready entity cardinality should retain SQL fallback")
        else {
            panic!("fallback count should return one projection row")
        };
        assert_eq!(rows, vec![vec![OutputValue::nat64(1)]]);
        assert_eq!(DataStore::current_get_call_count(), data_reads_before);
    }
}

#[test]
fn journaled_cardinality_rejects_volatile_counts_and_unfolded_accepted_root_drift() {
    let session = initialize_journaled();
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(10),
        })
        .expect("cardinality fixture row should commit");
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("cardinality fixture row should fold"),
    );
    drive_journaled_cardinality_to_ready(&session);
    let handle = session
        .db
        .store_handle(JOURNALED_STORE_PATH)
        .expect("journaled cardinality store should resolve");
    let (index_id, prefix_components) = journaled_user_index_prefix();
    let data_generation = JOURNALED_DATA_STORE.with(|store| store.borrow().generation());

    assert_eq!(
        JOURNALED_DATA_STORE.with(|store| store.borrow().exact_entity_count(ENTITY_TAG)),
        Some(1),
        "the live full-count cache should be populated before accepted-root drift",
    );
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| {
            store.borrow().exact_prefix_cardinality(
                data_generation,
                IndexKeyKind::User,
                index_id,
                prefix_components.as_slice(),
            )
        }),
        Some(1),
        "the live prefix-count cache should be populated before accepted-root drift",
    );
    assert_eq!(
        JOURNALED_INDEX_STORE.with(|store| {
            store
                .borrow()
                .exact_child_prefixes_for_parent_set(
                    data_generation,
                    IndexKeyKind::User,
                    index_id,
                    [prefix_components.as_slice()],
                    8,
                )
                .unwrap()
        }),
        Some(Vec::new()),
        "the volatile child-prefix cache should demonstrate the bypass fixture",
    );
    assert_eq!(
        handle
            .exact_user_index_child_prefixes_for_parent_set(
                data_generation,
                index_id,
                [prefix_components.as_slice()],
                8,
            )
            .unwrap(),
        None,
        "journaled child enumeration must use its conservative route instead of volatile authority",
    );
    assert_journaled_cardinality(handle, index_id, prefix_components.as_slice(), 1);

    let candidate = accepted_schema_candidate_with_field_bindings_for_tests(
        JOURNALED_STORE_PATH,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(ENTITY_TAG, identity_snapshot(JOURNALED_STORE_PATH, false))]),
        BTreeMap::from([
            ((ENTITY_TAG, source_key(ID_SOURCE)), FieldId::new(1)),
            ((ENTITY_TAG, source_key(PAYLOAD_SOURCE)), FieldId::new(2)),
        ]),
    );
    crate::db::commit::publish_accepted_schema_candidate(
        JOURNALED_STORE_PATH,
        handle,
        AcceptedSchemaRevision::INITIAL,
        &candidate,
    )
    .expect("a successor accepted root should publish into the live overlay");

    assert_eq!(
        handle.exact_entity_count(ENTITY_TAG),
        None,
        "an unfolded accepted root must invalidate durable evidence immediately",
    );
    assert_eq!(
        handle.exact_user_index_prefix_count(
            data_generation,
            IndexKeyKind::User,
            index_id,
            prefix_components.as_slice(),
        ),
        None,
        "journaled consumers must not fall back to a populated volatile prefix cache",
    );
}

#[test]
fn journaled_convergence_uses_final_batch_rows_for_unique_release() {
    let session = initialize_journaled_with_unique_payload();
    let inserted = session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![dynamic_payload_patch(10), dynamic_payload_patch(20)],
        )
        .expect("the unique journal fixture should commit");
    assert_eq!(
        inserted.rows,
        vec![expected_dynamic_row(1, 10), expected_dynamic_row(2, 20)],
    );
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the unique fixture should become canonical"),
    );

    let swapped = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
                patch: dynamic_payload_patch(20),
            },
            DynamicMutation::Update {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(2),
                patch: dynamic_payload_patch(10),
            },
        ])
        .expect("one journal batch should admit a final-row unique swap");
    assert_eq!(
        batch_rows(&swapped),
        vec![expected_dynamic_row(1, 20), expected_dynamic_row(2, 10)],
    );
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the unique swap should converge in one complete batch"),
    );

    let released = session
        .execute_trusted_dynamic_mutation_batch(vec![
            DynamicMutation::Delete {
                entity: ENTITY_NAME.to_string(),
                key: InputValue::nat64(1),
            },
            DynamicMutation::Insert {
                entity: ENTITY_NAME.to_string(),
                patch: dynamic_payload_patch(20),
            },
        ])
        .expect("a journaled delete should release its unique value to the final insert");
    assert_eq!(
        batch_rows(&released),
        vec![expected_dynamic_row(1, 20), expected_dynamic_row(3, 20)],
    );
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("the delete and unique reuse should converge together"),
    );

    assert_dynamic_payload(&session, 2, 10);
    assert_dynamic_payload(&session, 3, 20);
    assert_eq!(JOURNALED_INDEX_STORE.with(|store| store.borrow().len()), 2);
    assert!(
        session
            .execute_trusted_dynamic_insert_batch(ENTITY_NAME, vec![dynamic_payload_patch(20)],)
            .is_err(),
        "the converged unique index must remain authoritative",
    );
}

#[test]
fn journaled_startup_recovery_completes_one_large_batch_atomically() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled identity catalog should resolve");
    let payloads = (0_u64..129).collect::<Vec<_>>();
    session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&payloads),
            Timestamp::from_millis(9),
            Ok,
        )
        .expect("one large journal batch should commit");

    forget_recovered_domain_for_tests(&session.db)
        .expect("upgrade should reset recovery ownership");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should precede folding")
    );
    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 0);
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("the complete batch recovery page should commit"),
    );

    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 129);
    JOURNALED_TAIL_STORE.with(|tail| {
        let tail = tail.borrow();
        assert!(!tail.has_stored_batch());
    });
    assert!(session.db.ensure_recovered_state().is_err());
    assert!(
        session
            .db
            .drive_startup_recovery_page()
            .expect("verification should finish startup")
    );
    assert_dynamic_payload(&session, 1, 0);
    assert_dynamic_payload(&session, 129, 128);
}

#[test]
fn complete_batch_validation_rejects_a_late_record_before_canonical_writes() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled identity catalog should resolve");
    session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[7]),
            Timestamp::from_millis(9),
            Ok,
        )
        .expect("journal batch predecessor should commit");

    JOURNALED_TAIL_STORE.with(|tail| {
        let mut tail = tail.borrow_mut();
        let original = tail
            .next_batch_after(JournalSequence::new(0))
            .expect("journal batch should decode")
            .expect("journal batch should exist");
        let mut records = original.records().to_vec();
        records.push(
            JournalRecord::constraint_validation_job_delete(
                JOURNALED_STORE_PATH,
                EntityTag::new(ENTITY_TAG.value() + 1),
                ConstraintId::new(1).unwrap(),
            )
            .expect("bounded semantic corruption should build"),
        );
        let corrupted = JournalBatch::new_with_database_commit_sequence(
            original.batch_id(),
            original.commit_marker_id(),
            original.journal_sequence(),
            original.database_commit_sequence(),
            records,
        )
        .expect("current corrupt batch shape should build");
        let encoded =
            encode_journal_batch(&corrupted).expect("current corrupt batch envelope should encode");
        tail.clear_batches_through(original.journal_sequence());
        tail.insert_raw_batch_for_tests(original.journal_sequence(), encoded)
            .expect("corrupt persisted batch should replace the predecessor");
    });

    forget_recovered_domain_for_tests(&session.db)
        .expect("upgrade should reset recovery ownership");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should precede fold validation")
    );
    let error = session
        .db
        .drive_startup_recovery_page()
        .expect_err("late semantic corruption must fail before fold apply");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 0);
    JOURNALED_TAIL_STORE.with(|tail| {
        let tail = tail.borrow();
        assert_eq!(
            tail.fold_watermark()
                .expect("watermark should remain readable")
                .highest_folded_journal_sequence(),
            JournalSequence::new(0),
        );
        assert!(tail.has_stored_batch());
    });
}

#[test]
fn prepared_batch_row_evidence_rejects_a_late_malformed_row_before_canonical_writes() {
    let session = initialize_journaled();
    let catalog = session
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .expect("journaled identity catalog should resolve");
    session
        .execute_accepted_structural_save_batch(
            &catalog,
            true,
            batch(&[7, 8]),
            Timestamp::from_millis(9),
            Ok,
        )
        .expect("two-row journal batch should commit");

    JOURNALED_TAIL_STORE.with(|tail| {
        let mut tail = tail.borrow_mut();
        let original = tail
            .next_batch_after(JournalSequence::new(0))
            .expect("journal batch should decode")
            .expect("journal batch should exist");
        let mut records = original.records().to_vec();
        let mut row_ordinal = 0_u8;
        for record in &mut records {
            if let JournalRecord::RowPut { row_bytes, .. } = record {
                row_ordinal = row_ordinal.saturating_add(1);
                if row_ordinal == 2 {
                    *row_bytes = vec![0xff; 8];
                    break;
                }
            }
        }
        assert_eq!(row_ordinal, 2, "the late row record should be present");
        let corrupted = JournalBatch::new_with_database_commit_sequence(
            original.batch_id(),
            original.commit_marker_id(),
            original.journal_sequence(),
            original.database_commit_sequence(),
            records,
        )
        .expect("current corrupt batch shape should build");
        let encoded =
            encode_journal_batch(&corrupted).expect("current corrupt batch envelope should encode");
        tail.clear_batches_through(original.journal_sequence());
        tail.insert_raw_batch_for_tests(original.journal_sequence(), encoded)
            .expect("corrupt persisted batch should replace the predecessor");
    });

    forget_recovered_domain_for_tests(&session.db)
        .expect("upgrade should reset recovery ownership");
    assert!(
        !session
            .db
            .drive_startup_recovery_page()
            .expect("replay should precede row preparation")
    );
    let error = session
        .db
        .drive_startup_recovery_page()
        .expect_err("late malformed row must fail during complete batch preparation");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(JOURNALED_DATA_STORE.with(|store| store.borrow().len()), 0);
    JOURNALED_TAIL_STORE.with(|tail| {
        let tail = tail.borrow();
        assert_eq!(
            tail.fold_watermark()
                .expect("watermark should remain readable")
                .highest_folded_journal_sequence(),
            JournalSequence::new(0),
        );
        assert!(tail.has_stored_batch());
    });
}

#[test]
fn typed_mutation_batch_recovers_as_one_marker_atomic_transition() {
    let session = initialize_journaled();
    let binding = exact_key_binding(&session);
    session
        .execute_trusted_same_entity_typed_mutation_batch(
            &binding,
            vec![
                typed_payload_insert(&binding, 10),
                typed_payload_insert(&binding, 20),
            ],
        )
        .expect("typed recovery fixture should commit")
        .expect("typed recovery fixture binding should remain current");
    interrupt_next_mutation_commit_for_tests(MutationCommitInterruption::RowsPublished);

    let interrupted = session.execute_trusted_same_entity_typed_mutation_batch(
        &binding,
        vec![typed_payload_delete(1), typed_payload_insert(&binding, 30)],
    );
    assert!(
        interrupted.is_err(),
        "typed batch should expose the selected durable interruption",
    );
    let pending = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: ENTITY_NAME.to_string(),
            patch: dynamic_payload_patch(30),
        })
        .expect_err("ordinary writes must not bypass retained-marker recovery");
    assert_eq!(
        pending.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    drive_journaled_recovery_to_completion(&session);
    let recovered = session
        .execute_trusted_live_page(&crate::db::DynamicQuery::new(ENTITY_NAME), None)
        .expect("the recovered typed batch should be readable");
    assert_eq!(
        recovered.rows,
        vec![expected_dynamic_row(2, 20), expected_dynamic_row(3, 30)],
    );
}
