use super::{
    IdentityStateInventory, LiveSchemaCheckpointKey, LiveSchemaCheckpointPreflight,
    LiveSchemaCheckpointStore, MAX_LIVE_SCHEMA_CHECKPOINT_BYTES, decode_checkpoint,
    encode_checkpoint, schema_migration_record_blocks_ordinary_operations,
};
use crate::db::schema::control_store::CONTROL_START_PAGE;
use crate::{
    db::{
        integrity::DatabaseIncarnationId,
        schema::{
            AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests,
            live_schema_checkpoint::LiveSchemaCheckpoint,
            migration_lineage::{
                AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
                EntitySourceLineageCatalogOp,
            },
            migration_record::{
                PersistedSchemaMigrationEntity, PersistedSchemaMigrationPhase,
                PersistedSchemaMigrationProgress, PersistedSchemaMigrationTransition,
                SchemaMigrationRecord, SchemaMigrationRecordOp,
            },
        },
    },
    testing::test_memory,
};
use ic_storage::Memory;
use icydb_schema::{
    EntitySourceDigest, EntitySourceKey, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
    SchemaMigrationPlanDigest, SchemaProposalDigest, TargetDatabaseIdentity, TargetStoreIdentity,
};

#[test]
fn small_checkpoint_tree_uses_one_wasm_page_and_preserves_record_limits() {
    let memory = std::rc::Rc::new(test_memory(242));
    let mut store = LiveSchemaCheckpointStore::open(memory.clone()).unwrap();
    let candidate =
        empty_accepted_schema_candidate_for_tests("test::Compact", AcceptedSchemaRevision::new(1));
    store
        .apply(
            DatabaseIncarnationId::for_tests(0x63),
            "test::Compact",
            AcceptedSchemaRevision::NONE,
            &candidate,
        )
        .unwrap();
    assert_eq!(memory.size(), CONTROL_START_PAGE + 1);
    let reopened = LiveSchemaCheckpointStore::open_existing(memory.clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened
            .load("test::Compact")
            .unwrap()
            .unwrap()
            .candidate()
            .encoded_bundle(),
        candidate.encoded_bundle()
    );
    assert_eq!(memory.size(), CONTROL_START_PAGE + 1);
    let oversized = vec![0; MAX_LIVE_SCHEMA_CHECKPOINT_BYTES + 1];
    let key = LiveSchemaCheckpointKey::for_store("test::Compact").unwrap();
    assert!(decode_checkpoint(&oversized, key).is_err());
}

fn prepared_migration_record() -> SchemaMigrationRecord {
    SchemaMigrationRecord::prepared(
        TargetDatabaseIdentity::from_bytes([1; 32]),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([2; 32]),
        },
        ExpectedAcceptedHead::Exact {
            revision: 2,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
        },
        SchemaProposalDigest::from_bytes([4; 32]),
        SchemaMigrationPlanDigest::from_bytes([5; 32]),
        vec![
            PersistedSchemaMigrationTransition::try_new(
                EntitySourceKey::try_new("User").expect("source key should admit"),
                1,
                2,
            )
            .expect("transition should admit"),
        ],
        vec![
            PersistedSchemaMigrationEntity::try_new(
                TargetStoreIdentity::from_bytes([6; 32]),
                crate::types::EntityTag::new(7),
                EntitySourceDigest::from_bytes([8; 32]),
            )
            .expect("entity should admit"),
        ],
        Vec::new(),
    )
    .expect("prepared migration should admit")
}

#[test]
fn checkpoint_codec_is_canonical_and_checksum_bound() {
    let candidate =
        empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(1));
    let checkpoint = LiveSchemaCheckpoint {
        candidate: candidate.clone(),
        identity_states: IdentityStateInventory::default(),
    };
    let encoded =
        encode_checkpoint("test::Live", &checkpoint).expect("accepted candidate should checkpoint");
    let key =
        LiveSchemaCheckpointKey::for_store("test::Live").expect("checkpoint key should derive");
    let decoded = decode_checkpoint(&encoded, key).expect("checkpoint should decode");
    assert_eq!(
        decoded.candidate().encoded_bundle(),
        candidate.encoded_bundle()
    );
    assert_eq!(decoded.candidate().encoded_root(), candidate.encoded_root());

    let mut corrupted = encoded;
    let last = corrupted
        .last_mut()
        .expect("checkpoint should include a checksum");
    *last ^= 0x80;
    assert!(decode_checkpoint(&corrupted, key).is_err());
}

#[test]
fn checkpoint_compare_and_replace_is_idempotent_and_reopen_safe() {
    let first =
        empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(1));
    let second =
        empty_accepted_schema_candidate_for_tests("test::Live", AcceptedSchemaRevision::new(2));
    let memory = std::rc::Rc::new(test_memory(244));
    let mut store =
        LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
    let incarnation = DatabaseIncarnationId::for_tests(0x63);

    assert_eq!(
        store
            .preflight(
                incarnation,
                "test::Live",
                AcceptedSchemaRevision::NONE,
                &first,
            )
            .expect("initial checkpoint should preflight"),
        LiveSchemaCheckpointPreflight::Ready,
    );
    store
        .apply(
            incarnation,
            "test::Live",
            AcceptedSchemaRevision::NONE,
            &first,
        )
        .expect("initial checkpoint should apply");
    store
        .apply(
            incarnation,
            "test::Live",
            AcceptedSchemaRevision::NONE,
            &first,
        )
        .expect("initial checkpoint replay should be idempotent");
    store
        .apply(
            incarnation,
            "test::Live",
            AcceptedSchemaRevision::new(1),
            &second,
        )
        .expect("next checkpoint should apply");

    let reopened = LiveSchemaCheckpointStore::open(memory).expect("checkpoint store should reopen");
    let loaded = reopened
        .load("test::Live")
        .expect("checkpoint should remain readable")
        .expect("checkpoint should exist");
    assert_eq!(loaded.candidate().encoded_bundle(), second.encoded_bundle());
    assert_eq!(loaded.candidate().encoded_root(), second.encoded_root());
}

#[test]
fn lineage_uses_one_reserved_record_without_changing_checkpoint_count() {
    let memory = std::rc::Rc::new(test_memory(245));
    let mut store =
        LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([4; 32]),
            crate::types::EntityTag::new(9),
            AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                revision: 3,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
            })
            .expect("lineage should admit"),
        )
        .expect("lineage entry should insert");

    let operation = EntitySourceLineageCatalogOp::replace(None, &lineage)
        .expect("lineage operation should prepare");
    store
        .apply_lineage(&operation)
        .expect("lineage should persist");
    assert_eq!(store.checkpoint_count().expect("count should close"), 0);
    assert_eq!(
        store.load_lineage().expect("lineage should load"),
        Some(lineage.clone()),
    );
    assert_eq!(
        store
            .preflight_lineage(&operation)
            .expect("replay should preflight"),
        LiveSchemaCheckpointPreflight::AlreadyApplied,
    );

    let reopened = LiveSchemaCheckpointStore::open(memory).expect("store should reopen");
    assert_eq!(
        reopened.load_lineage().expect("lineage should load"),
        Some(lineage),
    );
    assert_ne!(
        LiveSchemaCheckpointKey::lineage(),
        LiveSchemaCheckpointKey::migration()
    );
}

#[test]
fn migration_uses_one_reserved_record_and_exact_compare_replace() {
    let memory = std::rc::Rc::new(test_memory(246));
    let mut store =
        LiveSchemaCheckpointStore::open(memory.clone()).expect("store should initialize");
    let prepared = prepared_migration_record();
    let validating = prepared
        .transition(
            PersistedSchemaMigrationPhase::Validating,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validation should start");
    let insert = SchemaMigrationRecordOp::insert(&prepared).expect("insert should prepare");
    let replace =
        SchemaMigrationRecordOp::replace(&prepared, &validating).expect("replace should prepare");

    store.apply_migration(&insert).expect("insert should apply");
    assert_eq!(store.checkpoint_count().expect("count should close"), 0);
    assert_eq!(
        store
            .preflight_migration(&insert)
            .expect("replay should preflight"),
        LiveSchemaCheckpointPreflight::AlreadyApplied,
    );
    store
        .apply_migration(&replace)
        .expect("replacement should apply");
    assert_eq!(
        store.load_migration().expect("migration should load"),
        Some(validating.clone()),
    );

    let reopened = LiveSchemaCheckpointStore::open(memory).expect("store should reopen");
    assert_eq!(
        reopened.load_migration().expect("migration should load"),
        Some(validating),
    );
}

#[test]
fn migration_gate_is_phase_and_capability_exact() {
    let prepared = prepared_migration_record();
    assert!(!schema_migration_record_blocks_ordinary_operations(
        &prepared, true,
    ));
    assert!(schema_migration_record_blocks_ordinary_operations(
        &prepared, false,
    ));
    let validating = prepared
        .transition(
            PersistedSchemaMigrationPhase::Validating,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("validation should start");
    assert!(schema_migration_record_blocks_ordinary_operations(
        &validating,
        true,
    ));
    let aborted = validating
        .transition(
            PersistedSchemaMigrationPhase::Aborted,
            PersistedSchemaMigrationProgress::default(),
        )
        .expect("pre-rewrite abort should admit");
    assert!(!schema_migration_record_blocks_ordinary_operations(
        &aborted, false,
    ));
}
