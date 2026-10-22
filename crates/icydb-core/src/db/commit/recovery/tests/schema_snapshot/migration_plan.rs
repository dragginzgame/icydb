//! Physical migration batches resolve durable plan authority once per boundary.

use super::*;
use crate::{
    db::{
        commit::{CommitMarker, recovery::verify_recovered_effects},
        data::{
            DecodedDataStoreKey, StructuralRowContract,
            canonical_row_from_runtime_value_source_with_accepted_contract,
        },
        index::{EncodedValue, IndexEntryValue, IndexId, IndexKey, IndexKeyKind},
        journal::{FoldWatermark, MAX_JOURNAL_BATCH_RECORDS},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedCompositeCatalog, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, apply_schema_migration_record_op,
            empty_accepted_enum_catalog_for_tests, physical_migration_record_ops_for_tests,
            take_migration_record_load_count_for_tests,
        },
    },
    types::Ulid,
    value::Value,
};
use icydb_schema::SchemaMigrationPlanDigest;
use std::borrow::Cow;

const PLAN: SchemaMigrationPlanDigest = SchemaMigrationPlanDigest::from_bytes([0x95; 32]);

fn migration_fixture(
    entities: u64,
    phase_steps: usize,
) -> (Db<TestCanister>, StructuralRowContract) {
    let (db, snapshot) = fixture();
    select_commit_memory_allocation(
        TestCanister::commit_memory_id().unwrap(),
        TestCanister::COMMIT_STABLE_KEY,
    );
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    DATA.with_borrow_mut(|store| *store = DataStore::init_journaled(test_memory(151)));
    INDEX.with_borrow_mut(|store| *store = IndexStore::init_journaled(test_memory(152)));
    JOURNAL.with_borrow_mut(|store| *store = JournalTailStore::init(test_memory(154)));
    for operation in physical_migration_record_ops_for_tests(TAG, entities)
        .unwrap()
        .into_iter()
        .take(phase_steps)
    {
        apply_schema_migration_record_op(&operation).unwrap();
    }

    let accepted = AcceptedSchemaSnapshot::try_new(snapshot).unwrap();
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted).unwrap();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let contract = StructuralRowContract::from_accepted_decode_contract(
        ENTITY,
        descriptor.row_decode_contract(catalog),
    );

    (db, contract)
}

fn migration_batch(
    contract: &StructuralRowContract,
    rows: u64,
    indexes: u16,
    plan: SchemaMigrationPlanDigest,
) -> JournalBatch {
    let mut records = Vec::new();
    for suffix in 0..rows {
        let value = Ulid::from_u128(u128::from(suffix));
        let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Ulid(value));
        let key = DecodedDataStoreKey::new_primary_key_value(TAG, &primary_key)
            .to_raw()
            .unwrap();
        let value = Value::Ulid(value);
        let row = canonical_row_from_runtime_value_source_with_accepted_contract(contract, |_| {
            Ok(Cow::Borrowed(&value))
        })
        .unwrap()
        .into_raw_row();
        records.push(
            JournalRecord::schema_migration_row_put(
                STORE,
                key,
                row.as_bytes().to_vec(),
                [1; 16],
                plan,
            )
            .unwrap(),
        );
        for ordinal in 1..=indexes {
            let key = IndexKey::new_from_components_with_primary_key_value(
                &IndexId::new_with_generation(TAG, ordinal, 7),
                IndexKeyKind::User,
                &[EncodedValue::try_new(&value).unwrap()],
                &primary_key,
            )
            .unwrap()
            .to_raw()
            .unwrap();
            records.push(JournalRecord::schema_migration_index_put(STORE, key, plan).unwrap());
        }
    }

    batch(records)
}

#[test]
fn physical_batch_plan_loads_do_not_grow_with_effect_count() {
    let (db, contract) = migration_fixture(1, 4);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    let mut loads = Vec::new();
    for rows in [1, 256] {
        let batch = migration_batch(&contract, rows, 3, PLAN);
        let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
        take_migration_record_load_count_for_tests();
        validate_journal_batch_records(
            &db,
            STORE,
            handle,
            &batch,
            JournalRecordApplyMode::Fold,
            &mut prepared,
        )
        .unwrap();
        loads.push(take_migration_record_load_count_for_tests());
    }
    assert!(loads[0] > 0, "the durable plan must still be verified");
    assert_eq!(
        loads[0], loads[1],
        "physical effects share one plan authority"
    );
}

#[test]
fn maximum_physical_batch_replays_folds_and_verifies_with_bounded_plan_loads() {
    let (db, contract) = migration_fixture(4096, 4);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    let batch = migration_batch(&contract, 256, 63, PLAN);
    assert_eq!(batch.records().len(), MAX_JOURNAL_BATCH_RECORDS);
    for mode in [JournalRecordApplyMode::Replay, JournalRecordApplyMode::Fold] {
        let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
        take_migration_record_load_count_for_tests();
        validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared).unwrap();
        assert_eq!(take_migration_record_load_count_for_tests(), 1);
        apply_prepared_journal_batch(STORE, handle, &batch, &mut prepared, mode);
        assert_eq!(
            take_migration_record_load_count_for_tests(),
            0,
            "apply consumes the validated batch"
        );
        assert!(prepared.iter().all(Option::is_none));
    }
    assert_eq!(handle.with_data(DataStore::len), 256);
    assert_eq!(handle.with_index(IndexStore::len), 256 * 63);
    for record in batch.records() {
        match record {
            JournalRecord::SchemaMigrationRowPut {
                primary_key,
                row_bytes,
                ..
            } => {
                assert_eq!(
                    handle.with_data(|store| store.get(primary_key).unwrap().as_bytes().to_vec()),
                    *row_bytes
                );
            }
            JournalRecord::SchemaMigrationIndexPut { key, .. } => {
                assert_eq!(
                    handle.with_index(|store| store.get(key)),
                    Some(IndexEntryValue::presence())
                );
            }
            _ => panic!("fixture must contain only physical migration effects"),
        }
    }
    JOURNAL.with_borrow_mut(|store| {
        store
            .persist_fold_watermark(FoldWatermark::new(batch.journal_sequence(), 1))
            .unwrap();
    });
    let marker = CommitMarker::from_parts(batch.commit_marker_id(), vec![batch]).unwrap();
    take_migration_record_load_count_for_tests();
    verify_recovered_effects(&db, Some(&marker)).unwrap();
    assert_eq!(
        take_migration_record_load_count_for_tests(),
        1,
        "verification reads durable authority independently"
    );
    // A later callback must not reuse earlier admission after its authority is
    // missing. Select an independently initialized control allocation only.
    select_commit_memory_allocation(156, "icydb.schema_replay.missing-plan.v1");
    let memory = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
    initialize_current_database_control_for_tests(&memory);
    let error = verify_recovered_effects(&db, Some(&marker)).unwrap_err();
    assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
    select_commit_memory_allocation(
        TestCanister::commit_memory_id().unwrap(),
        TestCanister::COMMIT_STABLE_KEY,
    );
}

#[test]
fn physical_batches_refuse_missing_early_or_different_plan_before_effects() {
    let (db, contract) = migration_fixture(1, 0);
    let handle = db.with_store_registry(|registry| registry.iter().next().unwrap().1);
    let batch = migration_batch(&contract, 1, 3, PLAN);
    for operation in physical_migration_record_ops_for_tests(TAG, 1).unwrap() {
        for mode in [JournalRecordApplyMode::Replay, JournalRecordApplyMode::Fold] {
            let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
            let error =
                validate_journal_batch_records(&db, STORE, handle, &batch, mode, &mut prepared)
                    .unwrap_err();
            assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
            assert!(prepared.iter().all(Option::is_none));
            assert_eq!(handle.with_data(DataStore::len), 0);
            assert_eq!(handle.with_index(IndexStore::len), 0);
        }
        apply_schema_migration_record_op(&operation).unwrap();
    }
    let wrong = migration_batch(
        &contract,
        1,
        3,
        SchemaMigrationPlanDigest::from_bytes([9; 32]),
    );
    for mode in [JournalRecordApplyMode::Replay, JournalRecordApplyMode::Fold] {
        let mut prepared = (0..wrong.records().len()).map(|_| None).collect::<Vec<_>>();
        let error = validate_journal_batch_records(&db, STORE, handle, &wrong, mode, &mut prepared)
            .unwrap_err();
        assert_eq!(error.class(), crate::error::ErrorClass::Corruption);
        assert!(prepared.iter().all(Option::is_none));
    }
    let mut prepared = (0..batch.records().len()).map(|_| None).collect::<Vec<_>>();
    validate_journal_batch_records(
        &db,
        STORE,
        handle,
        &batch,
        JournalRecordApplyMode::Replay,
        &mut prepared,
    )
    .unwrap();
    apply_prepared_journal_batch(
        STORE,
        handle,
        &batch,
        &mut prepared,
        JournalRecordApplyMode::Replay,
    );
    assert_eq!(handle.with_data(DataStore::len), 1);
    assert_eq!(handle.with_index(IndexStore::len), 3);
}
