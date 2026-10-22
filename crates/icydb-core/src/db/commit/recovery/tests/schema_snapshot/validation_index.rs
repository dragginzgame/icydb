//! Candidate-index pages share the batch's verified schema and job authority.

use super::*;
use crate::{
    db::{
        index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedSchemaFingerprint, ConstraintValidationJob, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, SchemaIndexId,
        },
    },
    types::Ulid,
    value::Value,
};

fn validation_fixture() -> (Db<TestCanister>, ConstraintValidationJob) {
    let (db, snapshot) = fixture();
    select_commit_memory_allocation(
        TestCanister::commit_memory_id().unwrap(),
        TestCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    INDEX.with_borrow_mut(|store| *store = IndexStore::init_journaled(test_memory(152)));
    let index = PersistedIndexSnapshot::new_sql_ddl(
        SchemaIndexId::new(1).unwrap(),
        1,
        "pending_unique".into(),
        STORE.into(),
        true,
        PersistedIndexKeySnapshot::from_field_paths(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(1),
            SchemaFieldSlot::new(0),
            vec!["id".into()],
            AcceptedFieldKind::Ulid,
            false,
        )]),
        None,
    )
    .clone_with_schema_identity(SchemaIndexId::new(1).unwrap(), 1, 7);
    let snapshot = snapshot
        .with_added_unique_activation(index, AcceptedSchemaFingerprint::new([1; 32]), 7)
        .unwrap();
    let constraint = snapshot.constraint_activations()[0].id();
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_validation_started(constraint)
        .unwrap();
    let snapshot = snapshot.with_constraint_catalog(catalog);
    let job = ConstraintValidationJob::start(
        TAG,
        ENTITY.into(),
        &snapshot.constraint_activations()[0],
        Some(7),
    )
    .unwrap();
    let candidate = accepted_schema_candidate_for_tests(
        STORE,
        AcceptedSchemaRevision::new(2),
        BTreeMap::from([(TAG, snapshot)]),
    );
    SCHEMA.with_borrow_mut(|store| {
        store
            .publish_accepted_schema_candidate(
                DatabaseIncarnationId::for_tests(0x62),
                AcceptedSchemaRevision::INITIAL,
                &candidate,
            )
            .unwrap();
        store.fold_constraint_validation_job(&job).unwrap();
        store.apply_constraint_validation_job(&job).unwrap();
    });

    (db, job)
}

fn candidate_key(ordinal: u16, generation: u64, suffix: u64) -> RawIndexStoreKey {
    let value = Ulid::from_u128(u128::from(suffix));
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(TAG, ordinal, generation),
        IndexKeyKind::User,
        &[EncodedValue::try_new(&Value::Ulid(value)).unwrap()],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Ulid(value)),
    )
    .unwrap()
    .to_raw()
    .unwrap()
}

fn validation_batch(job: &ConstraintValidationJob, keys: Vec<RawIndexStoreKey>) -> JournalBatch {
    let mut records = vec![JournalRecord::constraint_validation_job_put(STORE, job).unwrap()];
    records.extend(keys.into_iter().map(|key| {
        JournalRecord::constraint_validation_index_put(STORE, TAG, job.constraint_id(), key)
            .unwrap()
    }));

    batch(records)
}

#[test]
fn candidate_page_schema_loads_do_not_grow_with_key_count() {
    let (db, job) = validation_fixture();
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    let mut loads = Vec::new();
    for count in [1, 256] {
        let mut keys = (0..count)
            .map(|suffix| candidate_key(1, 7, suffix))
            .collect::<Vec<_>>();
        keys.sort();
        let batch = validation_batch(&job, keys);
        let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
        SchemaStore::take_canonical_bundle_load_count_for_tests();
        validate_journal_batch_records(
            &db,
            STORE,
            handle,
            &batch,
            JournalRecordApplyMode::Fold,
            &mut prepared,
        )
        .unwrap();
        loads.push(SchemaStore::take_canonical_bundle_load_count_for_tests());
    }
    assert!(
        loads[0] > 0,
        "the canonical predecessor must still be verified"
    );
    assert_eq!(loads[0], loads[1], "keys must share the batch authority");
}

#[test]
fn candidate_pages_preserve_replay_fold_and_corruption_refusal() {
    let (db, job) = validation_fixture();
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    for count in [0, 1, 256] {
        let mut keys = (0..count)
            .map(|suffix| candidate_key(1, 7, suffix))
            .collect::<Vec<_>>();
        keys.sort();
        let batch = validation_batch(&job, keys.clone());
        for mode in [JournalRecordApplyMode::Replay, JournalRecordApplyMode::Fold] {
            let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
            validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared)
                .unwrap();
            apply_prepared_journal_batch(STORE, handle, &batch, &mut prepared, mode);
            assert!(
                keys.iter()
                    .all(|key| handle.with_index(|store| store.get(key)).is_some())
            );
            SCHEMA.with_borrow(|store| {
                assert_eq!(
                    store
                        .constraint_validation_job(TAG, job.constraint_id())
                        .unwrap(),
                    Some(job.clone())
                );
            });
        }
    }
    let before = handle.with_index(IndexStore::len);
    for bad_key in [candidate_key(2, 7, 300), candidate_key(1, 8, 300)] {
        let batch = validation_batch(&job, vec![candidate_key(1, 7, 299), bad_key]);
        for mode in [JournalRecordApplyMode::Replay, JournalRecordApplyMode::Fold] {
            let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
            let error =
                validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared)
                    .unwrap_err();
            assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
            assert!(prepared.iter().all(Option::is_none));
            assert_eq!(handle.with_index(IndexStore::len), before);
        }
    }
}
