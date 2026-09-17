//! Schema records finish semantic preparation before the first batch effect.

use super::super::{
    JournalRecordApplyMode, PreparedJournalOp, apply_prepared_journal_batch,
    prepare_replayed_journal_batch, validate_journal_batch_records,
    verify_recovered_accepted_schema,
};
use crate::{
    db::{
        Db, RequestExecutionRoot,
        commit::memory::{
            commit_memory_handle, current_commit_memory_allocation, select_commit_memory_allocation,
        },
        data::DataStore,
        database_format::initialize_current_database_control_for_tests,
        index::IndexStore,
        integrity::DatabaseIncarnationId,
        journal::{JournalBatch, JournalRecord, JournalSequence, JournalTailStore},
        registry::{
            StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
            StoreRuntimeStorageCapabilities,
        },
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldStorageDecode, LeafCodec,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot,
            SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
            accepted_schema_candidate_for_tests, encode_persisted_schema_snapshot,
            persisted_schema_snapshot_decode_count_for_tests,
            reset_persisted_schema_snapshot_decode_count_for_tests,
        },
    },
    testing::test_memory,
    traits::{CanisterKind, Path},
    types::EntityTag,
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

const STORE: &str = "recovery::schema_snapshot::Store";
const ENTITY: &str = "recovery::schema_snapshot::Entity";
const TAG: EntityTag = EntityTag::new(91);

struct TestCanister;
impl Path for TestCanister {
    const PATH: &'static str = "recovery::schema_snapshot::Canister";
}
impl CanisterKind for TestCanister {
    fn commit_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(155)
    }
    const COMMIT_STABLE_KEY: &'static str = "icydb.schema_replay.commit.v1";
    fn startup_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(3)
    }
    const STARTUP_STABLE_KEY: &'static str = "icydb.schema_replay.startup.v1";
    fn integrity_progress_memory_id() -> Result<u8, ic_memory::RuntimeOpenError> {
        Ok(2)
    }
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.schema_replay.integrity.v1";
}

thread_local! {
    static DATA: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(151)));
    static INDEX: RefCell<IndexStore> = RefCell::new(IndexStore::init_journaled(test_memory(152)));
    static SCHEMA: RefCell<SchemaStore> = RefCell::new(SchemaStore::init_journaled(test_memory(153)));
    static JOURNAL: RefCell<JournalTailStore> = RefCell::new(JournalTailStore::init(test_memory(154)));
    static REGISTRY: StoreRegistry = {
        let mut registry = StoreRegistry::new();
        registry.register_journaled_store(
            STORE, &DATA, &INDEX, &SCHEMA, &JOURNAL,
            StoreAllocationIdentities::new_journaled(
                StoreAllocationIdentity::new(151, "replay.data"),
                StoreAllocationIdentity::new(152, "replay.index"),
                StoreAllocationIdentity::new(153, "replay.schema"),
                StoreAllocationIdentity::new(154, "replay.journal"),
            ), StoreRuntimeStorageCapabilities::journaled(),
        ).unwrap();
        registry
    };
}

fn fixture() -> (Db<TestCanister>, PersistedSchemaSnapshot) {
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        ENTITY.into(),
        "Entity".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
        vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".into(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Ulid,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )],
    );
    let candidate = accepted_schema_candidate_for_tests(
        STORE,
        AcceptedSchemaRevision::INITIAL,
        BTreeMap::from([(TAG, snapshot.clone())]),
    );
    SCHEMA.with_borrow_mut(|store| {
        *store = SchemaStore::init_journaled(test_memory(153));
        store
            .publish_accepted_schema_candidate(
                DatabaseIncarnationId::for_tests(0x62),
                AcceptedSchemaRevision::NONE,
                &candidate,
            )
            .unwrap();
    });
    (
        Db::new(
            &REGISTRY,
            RequestExecutionRoot::__new_runtime_root().scope(),
        ),
        snapshot,
    )
}

fn batch(records: Vec<JournalRecord>) -> JournalBatch {
    JournalBatch::new([1; 16], [2; 16], JournalSequence::new(1), records).unwrap()
}

#[test]
fn schema_replay_applies_prepared_identity_without_decoding_again() {
    let (db, original) = fixture();
    let after = original.with_schema_version(SchemaVersion::new(2));
    let batch = batch(vec![
        JournalRecord::schema_put(STORE, encode_persisted_schema_snapshot(&after).unwrap())
            .unwrap(),
    ]);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    for mode in [
        JournalRecordApplyMode::Replay,
        JournalRecordApplyMode::Fold,
        JournalRecordApplyMode::Replay,
        JournalRecordApplyMode::Fold,
    ] {
        let mut prepared = match mode {
            JournalRecordApplyMode::Replay => {
                prepare_replayed_journal_batch(&db, STORE, handle, &batch).unwrap()
            }
            JournalRecordApplyMode::Fold => {
                let mut prepared = vec![None];
                validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared)
                    .unwrap();
                prepared
            }
        };
        reset_persisted_schema_snapshot_decode_count_for_tests();
        apply_prepared_journal_batch(STORE, handle, &batch, &mut prepared, mode);
        assert_eq!(persisted_schema_snapshot_decode_count_for_tests(), 0);
        assert_eq!(
            SCHEMA
                .with_borrow(|store| store.get_persisted_snapshot(TAG, after.version()))
                .unwrap(),
            Some(after.clone()),
        );
    }
}

#[test]
fn invalid_later_schema_record_rejects_before_any_snapshot_is_applied() {
    let (db, original) = fixture();
    let after = original.clone().with_schema_version(SchemaVersion::new(2));
    let valid = JournalRecord::schema_put(STORE, encode_persisted_schema_snapshot(&after).unwrap())
        .unwrap();
    let malformed = JournalRecord::SchemaPut {
        store_path: STORE.into(),
        schema_snapshot_bytes: vec![1, 2, 3],
    };
    let batch = batch(vec![valid, malformed]);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    assert!(prepare_replayed_journal_batch(&db, STORE, handle, &batch).is_err());
    assert!(
        SCHEMA
            .with_borrow(|store| store.get_persisted_snapshot(TAG, after.version()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        SCHEMA
            .with_borrow(|store| store.get_persisted_snapshot(TAG, original.version()))
            .unwrap(),
        Some(original),
    );
}

#[test]
fn catalog_replay_and_fold_retain_preparation_and_verify_stored_authority() {
    let (db, snapshot) = fixture();
    select_commit_memory_allocation(
        TestCanister::commit_memory_id().expect("test allocation"),
        TestCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let candidate = accepted_schema_candidate_for_tests(
        STORE,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(TAG, snapshot.with_schema_version(SchemaVersion::new(2)))]),
    );
    let batch = batch(vec![
        JournalRecord::accepted_schema_publish(
            STORE,
            AcceptedSchemaRevision::INITIAL,
            candidate.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        )
        .unwrap(),
    ]);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    for mode in [
        JournalRecordApplyMode::Replay,
        JournalRecordApplyMode::Fold,
        JournalRecordApplyMode::Replay,
        JournalRecordApplyMode::Fold,
    ] {
        let mut prepared = match mode {
            JournalRecordApplyMode::Replay => {
                prepare_replayed_journal_batch(&db, STORE, handle, &batch).unwrap()
            }
            JournalRecordApplyMode::Fold => {
                let mut ops = vec![None];
                validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut ops).unwrap();
                ops
            }
        };
        assert!(matches!(
            (&prepared[0], mode),
            (
                Some(PreparedJournalOp::AcceptedSchemaReplay { .. }),
                JournalRecordApplyMode::Replay
            ) | (
                Some(PreparedJournalOp::AcceptedSchemaFold(_)),
                JournalRecordApplyMode::Fold
            )
        ));
        apply_prepared_journal_batch(STORE, handle, &batch, &mut prepared, mode);
        assert!(prepared.iter().all(Option::is_none));
        if mode == JournalRecordApplyMode::Fold {
            SCHEMA
                .with_borrow_mut(SchemaStore::reset_journaled_live_projection)
                .unwrap();
        }
        verify_recovered_accepted_schema(
            &db,
            STORE,
            candidate.encoded_bundle(),
            candidate.encoded_root(),
            &mut BTreeSet::new(),
        )
        .unwrap();
    }
    // Verification must read storage independently, even after successful
    // preparation/application. Losing the stored root cannot be hidden by it.
    SCHEMA.with_borrow_mut(|store| *store = SchemaStore::init_journaled(test_memory(153)));
    assert!(
        verify_recovered_accepted_schema(
            &db,
            STORE,
            candidate.encoded_bundle(),
            candidate.encoded_root(),
            &mut BTreeSet::new()
        )
        .is_err()
    );
}

#[test]
fn stale_catalog_fold_rejects_before_publishing_any_prepared_effect() {
    let (db, original) = fixture();
    select_commit_memory_allocation(
        TestCanister::commit_memory_id().expect("test allocation"),
        TestCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let candidate = accepted_schema_candidate_for_tests(
        STORE,
        AcceptedSchemaRevision::new(3),
        BTreeMap::from([(
            TAG,
            original.clone().with_schema_version(SchemaVersion::new(3)),
        )]),
    );
    let batch = batch(vec![
        JournalRecord::accepted_schema_publish(
            STORE,
            AcceptedSchemaRevision::new(2),
            candidate.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        )
        .unwrap(),
    ]);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    let mut prepared = vec![None];
    assert!(
        validate_journal_batch_records(
            &db,
            STORE,
            handle,
            &batch,
            JournalRecordApplyMode::Fold,
            &mut prepared
        )
        .is_err()
    );
    assert!(prepared.iter().all(Option::is_none));
    SCHEMA.with_borrow(|store| {
        assert_eq!(
            store
                .current_canonical_accepted_schema_bundle()
                .unwrap()
                .unwrap()
                .revision(),
            AcceptedSchemaRevision::INITIAL
        );
        assert_eq!(
            store
                .get_persisted_snapshot(TAG, original.version())
                .unwrap(),
            Some(original)
        );
        assert!(
            store
                .get_persisted_snapshot(TAG, SchemaVersion::new(3))
                .unwrap()
                .is_none()
        );
    });
}
