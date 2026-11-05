use super::{
    AcceptedSchemaPublication, publish_accepted_schema_candidate,
    publish_accepted_schema_candidates_with_application_record,
    publish_database_control_atomically,
};
use crate::{
    db::{
        Db,
        commit::recovery::forget_recovered_domain_for_tests,
        commit::{
            CommitMarker, DatabaseControlOp, RecoveryProgress, begin_commit, continue_recovery,
            generate_commit_id, generate_marker_batch_id, next_database_commit_sequence,
        },
        data::DataStore,
        index::IndexStore,
        journal::{DatabaseCommitSequence, JournalBatch, JournalRecord, JournalSequence},
        registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, CandidateSchemaRevision, FieldId,
            FieldInsertGeneration, FieldStorageDecode, LeafCodec, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, ScalarCodec, SchemaApplicationRecord,
            SchemaApplicationRecordOp, SchemaChangeOutcome, SchemaChangeReceipt, SchemaFieldSlot,
            SchemaFieldWritePolicy, SchemaInsertDefault, SchemaRowLayout, SchemaStore,
            SchemaVersion, accepted_schema_candidate_for_tests,
            empty_accepted_schema_candidate_for_tests, entity_source_lineage_matches_for_tests,
            load_live_schema_checkpoint, prepared_schema_migration_record_op_for_tests,
            schema_migration_record_lifecycle_ops_for_tests,
            schema_migration_record_matches_for_tests,
            unadopted_entity_source_lineage_op_for_tests, with_schema_application_store,
        },
    },
    traits::{CanisterKind, Path},
    types::EntityTag,
};
use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposalDigest, SchemaSubmissionKey,
    TargetDatabaseIdentity, TargetStoreIdentity,
};
use std::cell::RefCell;

fn drive_startup_recovery_to_completion<C: CanisterKind>(db: &Db<C>) {
    for _ in 0..1_024 {
        match continue_recovery(db).expect("test startup recovery page should succeed") {
            RecoveryProgress::Complete => return,
            RecoveryProgress::Pending => {}
        }
    }
    panic!("test startup recovery should complete within 1,024 bounded pages");
}

const COMPLETION_STORE_PATH: &str = "schema_publication_tests::CompletionHeap";
const RECOVERY_STORE_PATH: &str = "schema_publication_tests::RecoveryHeap";
const RECOVERY_ENTITY_PATH: &str = "schema_publication_tests::RecoveredEntity";

thread_local! {
    static COMPLETION_DATA: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static COMPLETION_INDEX: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static COMPLETION_SCHEMA: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static COMPLETION_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            COMPLETION_STORE_PATH,
            &COMPLETION_DATA,
            &COMPLETION_INDEX,
            &COMPLETION_SCHEMA,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("completion heap store should register");
        registry
    };
    static RECOVERY_DATA: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static RECOVERY_INDEX: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static RECOVERY_SCHEMA: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static RECOVERY_REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_store(
            RECOVERY_STORE_PATH,
            &RECOVERY_DATA,
            &RECOVERY_INDEX,
            &RECOVERY_SCHEMA,
            StoreAllocationIdentities::absent(),
            StoreRuntimeStorageCapabilities::heap(),
        ).expect("recovery heap store should register");
        registry
    };
}

fn candidate_with_entity(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    entity_tag: EntityTag,
    entity_path: &str,
) -> CandidateSchemaRevision {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        entity_path.to_string(),
        "RecoveredEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );
    accepted_schema_candidate_for_tests(
        store_path,
        revision,
        std::collections::BTreeMap::from([(entity_tag, snapshot)]),
    )
}

fn candidate_with_identity_entity(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    entity_tag: EntityTag,
) -> CandidateSchemaRevision {
    let field_id = FieldId::new(1);
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "schema_publication_tests::IdentityEntity".to_string(),
        "IdentityEntity".to_string(),
        field_id,
        SchemaRowLayout::initial(vec![(field_id, SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial_with_write_policy(
            field_id,
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
        )],
    );
    accepted_schema_candidate_for_tests(
        store_path,
        revision,
        std::collections::BTreeMap::from([(entity_tag, snapshot)]),
    )
}

struct CompletionCanister;

impl Path for CompletionCanister {
    const PATH: &'static str = "schema_publication_tests::CompletionCanister";
}

impl CanisterKind for CompletionCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(240)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.schema_publication.completion.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(244)
    }
    const STARTUP_STABLE_KEY: &'static str =
        "icydb.test.schema_publication.completion.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(243)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.schema_publication.completion.integrity.v1";
}

struct RecoveryCanister;

impl Path for RecoveryCanister {
    const PATH: &'static str = "schema_publication_tests::RecoveryCanister";
}

impl CanisterKind for RecoveryCanister {
    fn commit_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(241)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.schema_publication.recovery.commit.v1";
    fn startup_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(244)
    }
    const STARTUP_STABLE_KEY: &'static str =
        "icydb.test.schema_publication.recovery.startup.control.v1";
    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(242)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
        "icydb.test.schema_publication.recovery.integrity.v1";
}

fn applied_record(
    candidate: &CandidateSchemaRevision,
    discriminator: u8,
    submission: &str,
) -> SchemaApplicationRecord {
    let accepted_head = ExpectedAcceptedHead::Exact {
        revision: candidate.revision().get(),
        fingerprint: ExpectedSchemaFingerprint::from_bytes(
            candidate.root().fingerprint().as_bytes(),
        ),
    };
    let receipt = SchemaChangeReceipt::new(
        TargetDatabaseIdentity::from_bytes([discriminator; 32]),
        SchemaSubmissionKey::try_new(submission).expect("submission key should admit"),
        SchemaProposalDigest::from_bytes([discriminator.wrapping_add(1); 32]),
        ExpectedAcceptedHead::Empty,
        SchemaChangeOutcome::Applied { accepted_head },
    )
    .expect("applied receipt should admit");
    SchemaApplicationRecord::new(receipt, Vec::new())
        .expect("terminal application record should admit")
}

fn assert_candidate_and_record_published(
    store: crate::db::registry::StoreHandle,
    candidate: &CandidateSchemaRevision,
    record: &SchemaApplicationRecord,
) {
    let current = store
        .with_schema(SchemaStore::current_accepted_schema_bundle)
        .expect("heap schema should remain readable");
    assert_eq!(
        current.as_ref(),
        Some(candidate.bundle()),
        "accepted heap candidate must publish before success",
    );
    let checkpoint = load_live_schema_checkpoint(candidate.store_path())
        .expect("live accepted checkpoint should remain readable")
        .expect("live accepted checkpoint should exist");
    assert_eq!(
        checkpoint.candidate().encoded_bundle(),
        candidate.encoded_bundle()
    );
    assert_eq!(
        checkpoint.candidate().encoded_root(),
        candidate.encoded_root()
    );
    let loaded = with_schema_application_store(|store| {
        store.load(
            record.receipt().database_identity(),
            record.receipt().submission_key(),
        )
    })
    .expect("application record store should remain readable");
    assert_eq!(
        loaded.as_ref(),
        Some(record),
        "application receipt must describe the published candidate",
    );
}

#[test]
fn marker_owned_application_publishes_one_live_only_store_and_receipt() {
    let db = Db::<CompletionCanister>::new(
        &COMPLETION_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let store = db
        .store_handle(COMPLETION_STORE_PATH)
        .expect("completion heap store should resolve");
    let candidate = candidate_with_identity_entity(
        COMPLETION_STORE_PATH,
        AcceptedSchemaRevision::new(1),
        EntityTag::new(72),
    );
    let record = applied_record(&candidate, 0x51, "single-live-completion");
    let operation =
        SchemaApplicationRecordOp::insert(&record).expect("record insertion should prepare");

    publish_accepted_schema_candidates_with_application_record(
        vec![AcceptedSchemaPublication::new(
            COMPLETION_STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &candidate,
        )],
        operation,
    )
    .expect("marker-owned live-only application should publish");

    assert_candidate_and_record_published(store, &candidate, &record);

    COMPLETION_DATA.with(|store| *store.borrow_mut() = DataStore::init_heap());
    COMPLETION_INDEX.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    COMPLETION_SCHEMA.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
    drive_startup_recovery_to_completion(&db);
    assert_candidate_and_record_published(store, &candidate, &record);

    let second = empty_accepted_schema_candidate_for_tests(
        COMPLETION_STORE_PATH,
        AcceptedSchemaRevision::new(2),
    );
    publish_accepted_schema_candidate(
        COMPLETION_STORE_PATH,
        store,
        AcceptedSchemaRevision::new(1),
        &second,
    )
    .expect("plain live-only publication should checkpoint");
    assert_candidate_and_record_published(store, &second, &record);

    COMPLETION_DATA.with(|store| *store.borrow_mut() = DataStore::init_heap());
    COMPLETION_INDEX.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    COMPLETION_SCHEMA.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
    drive_startup_recovery_to_completion(&db);
    assert_candidate_and_record_published(store, &second, &record);

    let reused = candidate_with_identity_entity(
        COMPLETION_STORE_PATH,
        AcceptedSchemaRevision::new(3),
        EntityTag::new(72),
    );
    publish_accepted_schema_candidate(
        COMPLETION_STORE_PATH,
        store,
        AcceptedSchemaRevision::new(2),
        &reused,
    )
    .expect_err("retired identity owner must remain unavailable after checkpoint recovery");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the interrupted compound publication and recovery assertions form one scenario"
)]
fn interrupted_live_only_application_recovers_candidate_and_receipt_from_marker() {
    let db = Db::<RecoveryCanister>::new(
        &RECOVERY_REGISTRY,
        crate::db::RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let store = db
        .store_handle(RECOVERY_STORE_PATH)
        .expect("recovery heap store should resolve");
    let initial = empty_accepted_schema_candidate_for_tests(
        RECOVERY_STORE_PATH,
        AcceptedSchemaRevision::new(1),
    );
    let initial_record = applied_record(&initial, 0x60, "single-live-initial");
    let initial_operation = SchemaApplicationRecordOp::insert(&initial_record)
        .expect("initial record insertion should prepare");
    publish_accepted_schema_candidates_with_application_record(
        vec![AcceptedSchemaPublication::new(
            RECOVERY_STORE_PATH,
            store,
            AcceptedSchemaRevision::NONE,
            &initial,
        )],
        initial_operation,
    )
    .expect("initial live-only application should publish");
    assert_candidate_and_record_published(store, &initial, &initial_record);

    let entity_tag = EntityTag::new(71);
    let candidate = candidate_with_entity(
        RECOVERY_STORE_PATH,
        AcceptedSchemaRevision::new(2),
        entity_tag,
        RECOVERY_ENTITY_PATH,
    );
    let record = applied_record(&candidate, 0x61, "single-live-recovery");
    let lineage = unadopted_entity_source_lineage_op_for_tests(
        TargetStoreIdentity::from_bytes([0x62; 32]),
        entity_tag,
        ExpectedAcceptedHead::Exact {
            revision: candidate.revision().get(),
            fingerprint: ExpectedSchemaFingerprint::from_bytes(
                candidate.root().fingerprint().as_bytes(),
            ),
        },
    )
    .expect("lineage effect should prepare");
    let application =
        SchemaApplicationRecordOp::insert(&record).expect("record insertion should prepare");
    let migration = prepared_schema_migration_record_op_for_tests()
        .expect("migration operation should prepare");
    let marker_id = generate_commit_id(0).expect("marker id should generate");
    let batch = JournalBatch::new_with_database_commit_sequence(
        generate_marker_batch_id(marker_id, 0).expect("batch id should derive"),
        marker_id,
        JournalSequence::new(0),
        DatabaseCommitSequence::new(
            next_database_commit_sequence().expect("database sequence should preview"),
        ),
        vec![
            JournalRecord::accepted_schema_publish(
                RECOVERY_STORE_PATH,
                AcceptedSchemaRevision::new(1),
                candidate.encoded_bundle().to_vec(),
                candidate.encoded_root().to_vec(),
            )
            .expect("schema journal record should admit"),
        ],
    )
    .expect("schema journal batch should admit");
    let marker = CommitMarker::from_parts_with_database_control(
        marker_id,
        vec![batch],
        vec![
            DatabaseControlOp::SchemaApplication(application.clone()),
            DatabaseControlOp::EntitySourceLineage(lineage.clone()),
            DatabaseControlOp::SchemaMigration(migration.clone()),
        ],
    )
    .expect("application marker should admit");
    let _interrupted = begin_commit(&marker).expect("marker should persist before interruption");
    with_schema_application_store(|store| store.apply(&application))
        .expect("interruption should leave only the receipt applied");
    assert!(
        !entity_source_lineage_matches_for_tests(&lineage)
            .expect("lineage state should remain readable"),
        "receipt-first interruption must not imply lineage publication",
    );
    assert!(
        !schema_migration_record_matches_for_tests(&migration)
            .expect("migration state should remain readable"),
        "receipt-first interruption must not imply migration progress publication",
    );

    RECOVERY_DATA.with(|store| *store.borrow_mut() = DataStore::init_heap());
    RECOVERY_INDEX.with(|store| *store.borrow_mut() = IndexStore::init_heap());
    RECOVERY_SCHEMA.with(|store| *store.borrow_mut() = SchemaStore::init_heap());
    assert!(
        store
            .with_schema(SchemaStore::current_accepted_schema_root)
            .expect("heap schema root should remain readable")
            .is_none(),
        "simulated upgrade must clear the live schema projection",
    );
    drive_startup_recovery_to_completion(&db);

    assert_candidate_and_record_published(store, &candidate, &record);
    assert!(
        entity_source_lineage_matches_for_tests(&lineage)
            .expect("recovered lineage should remain readable"),
        "marker recovery must complete the exact lineage effect",
    );
    assert!(
        schema_migration_record_matches_for_tests(&migration)
            .expect("recovered migration state should remain readable"),
        "marker recovery must complete the exact migration progress effect",
    );
    let (_, validating, aborted) = schema_migration_record_lifecycle_ops_for_tests()
        .expect("test migration lifecycle should prepare");
    publish_database_control_atomically(vec![DatabaseControlOp::SchemaMigration(validating)])
        .expect("validation gate should publish");
    let blocked = db
        .recovered_store(RECOVERY_STORE_PATH)
        .expect_err("ordinary store routing must fail while validation owns the gate");
    assert_eq!(
        blocked.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SchemaMigration {
            reason: icydb_diagnostic_code::SchemaMigrationCode::MigrationInProgress,
        }),
    );
    db.ensure_recovered_control_state()
        .expect("control-plane recovery should remain available");
    db.store_handle(RECOVERY_STORE_PATH)
        .expect("control-plane inspection should remain available while gated");
    publish_database_control_atomically(vec![DatabaseControlOp::SchemaMigration(aborted)])
        .expect("pre-rewrite abort should publish");
    db.recovered_store(RECOVERY_STORE_PATH)
        .expect("terminal abort should clear the ordinary-operation gate");
    let recovered_entity = db
        .accepted_runtime_entity_for_path(RECOVERY_ENTITY_PATH)
        .expect("recovered accepted entity should supply its runtime route");
    assert_eq!(recovered_entity.entity_tag(), entity_tag);
    assert_eq!(recovered_entity.store_path(), RECOVERY_STORE_PATH);
    drive_startup_recovery_to_completion(&db);
    assert_candidate_and_record_published(store, &candidate, &record);
    assert!(
        entity_source_lineage_matches_for_tests(&lineage)
            .expect("idempotent lineage should remain readable"),
    );
}
