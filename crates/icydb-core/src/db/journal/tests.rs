use super::{
    FoldWatermark, JournalBatch, JournalRecord, JournalSequence, JournalTailStore,
    codec::{
        JOURNAL_BATCH_FORMAT_VERSION_CURRENT, MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD,
        MAX_JOURNAL_BATCH_BYTES, MAX_JOURNAL_BATCH_RECORDS, RawJournalBatch, decode_journal_batch,
        encode_journal_batch, inspect_raw_journal_batch_fixed_header,
        inspect_raw_journal_batch_header, journal_record_payload_len,
    },
    store::{
        JOURNAL_TAIL_CHUNK_BYTES, JournalInspectionCheckpoint, JournalInspectionLimits,
        JournalIntegrityIssue, RawJournalChunk,
    },
};
use crate::{
    db::{
        commit::CommitMarker,
        data::{DecodedDataStoreKey, RawDataStoreKey},
        index::{IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        integrity::DatabaseIncarnationId,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        positioned_overlay::{OnlineOverlayDecision, classify_journal_overlay},
        schema::{
            AcceptedCheckExprV1, AcceptedSchemaFingerprint, AcceptedSchemaRevision,
            ConstraintActivationKind, ConstraintActivationSnapshot, ConstraintActivationState,
            ConstraintId, ConstraintOrigin, ConstraintValidationJob, FieldId, IdentityRangeAdvance,
            IdentityStateOwner, empty_accepted_schema_candidate_for_tests,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    types::EntityTag,
};
use ic_stable_structures::{
    Memory, Storable, VectorMemory,
    memory_manager::{MemoryId, MemoryManager},
};
use icydb_schema::SchemaMigrationPlanDigest;
use sha2::{Digest, Sha256};
use std::{borrow::Cow, mem::size_of};

const SINGLE_MEMORY_MANAGER_BUCKET_PAGES: u64 = 1 + 128;

fn raw_data_store_key(fill: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(fill)),
    )
    .to_raw()
    .expect("test key should materialize")
}

fn row_put_record(fill: u64) -> JournalRecord {
    let fill_byte = u8::try_from(fill).expect("test fill should fit u8");
    JournalRecord::row_put(
        "test::Entity",
        raw_data_store_key(fill),
        vec![fill_byte; 3],
        [0x11; 16],
    )
    .expect("row put record should build")
}

fn row_delete_record(fill: u64) -> JournalRecord {
    JournalRecord::row_delete("test::Entity", raw_data_store_key(fill), [0x22; 16])
        .expect("row delete record should build")
}

fn schema_put_record(fill: u8) -> JournalRecord {
    JournalRecord::schema_put("test::Store", vec![fill; 8]).expect("schema put record should build")
}

fn accepted_schema_publish_record() -> JournalRecord {
    let candidate =
        empty_accepted_schema_candidate_for_tests("test::Store", AcceptedSchemaRevision::new(2));
    JournalRecord::accepted_schema_publish(
        "test::Store",
        AcceptedSchemaRevision::INITIAL,
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )
    .expect("accepted schema publication record should build")
}

fn accepted_schema_index_key(component: u8, primary_key: u64) -> RawIndexStoreKey {
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(EntityTag::new(1), 2, 7),
        IndexKeyKind::User,
        &[&[component]],
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(primary_key)),
    )
    .expect("accepted schema index key should build")
    .to_raw()
    .expect("accepted schema index key should encode")
}

fn identity_range_record(count: u32) -> JournalRecord {
    identity_range_record_from(0, count)
}

fn identity_range_record_from(expected_high_water: u128, count: u32) -> JournalRecord {
    let owner = IdentityStateOwner::try_new(
        DatabaseIncarnationId::for_tests(0x51),
        EntityTag::new(1),
        FieldId::new(3),
    )
    .expect("identity owner should build");
    let range = IdentityRangeAdvance::try_new(
        owner,
        expected_high_water,
        expected_high_water
            .checked_add(u128::from(count))
            .expect("small test range should fit"),
        count,
    )
    .expect("contiguous identity range should build");
    JournalRecord::identity_range_advance(range).expect("identity range record should build")
}

fn validation_job() -> ConstraintValidationJob {
    let activation = ConstraintActivationSnapshot::new(
        ConstraintId::new(7).expect("test constraint ID should be non-zero"),
        "pending_check".to_string(),
        ConstraintOrigin::Generated,
        ConstraintActivationKind::Check {
            expression: Box::new(AcceptedCheckExprV1::True),
        },
        ConstraintActivationState::Validating,
        AcceptedSchemaFingerprint::new([0xA5; 32]),
        11,
    );
    ConstraintValidationJob::start(
        EntityTag::new(1),
        "test::Entity".to_string(),
        &activation,
        None,
    )
    .expect("test validation job should build")
}

fn batch(sequence: u64) -> JournalBatch {
    let sequence_byte = u8::try_from(sequence).expect("test sequence should fit u8");
    JournalBatch::new(
        [sequence_byte; 16],
        [0xAA; 16],
        JournalSequence::new(sequence),
        vec![
            row_put_record(sequence),
            row_delete_record(sequence + 100),
            schema_put_record(sequence_byte),
        ],
    )
    .expect("journal batch should build")
}

fn multi_chunk_batch(sequence: u64) -> JournalBatch {
    let record = JournalRecord::row_put(
        "test::Entity",
        raw_data_store_key(sequence),
        vec![0xAB; JOURNAL_TAIL_CHUNK_BYTES as usize + 32],
        [0x44; 16],
    )
    .expect("multi-chunk row put record should build");

    JournalBatch::new(
        [0x44; 16],
        [0xAA; 16],
        JournalSequence::new(sequence),
        vec![record],
    )
    .expect("multi-chunk journal batch should build")
}

#[test]
fn journal_batch_codec_round_trips_logical_row_and_schema_records() {
    let batch = batch(1);
    let encoded = encode_journal_batch(&batch).expect("journal batch should encode");
    let decoded = decode_journal_batch(&encoded).expect("journal batch should decode");

    assert_eq!(decoded, batch);
    assert_eq!(decoded.records().len(), 3);
}

#[test]
fn every_journal_record_family_has_an_explicit_online_overlay_decision() {
    let job = validation_job();
    let index_key = accepted_schema_index_key(7, 9);
    let plan = SchemaMigrationPlanDigest::from_bytes([0x72; 32]);
    let records = vec![
        (row_put_record(1), OnlineOverlayDecision::DataPositive),
        (row_delete_record(2), OnlineOverlayDecision::DataTombstone),
        (schema_put_record(3), OnlineOverlayDecision::SchemaPositive),
        (
            accepted_schema_publish_record(),
            OnlineOverlayDecision::SchemaPositive,
        ),
        (
            JournalRecord::accepted_schema_index_delete(
                "test::Store",
                EntityTag::new(1),
                [0x51; 16],
                vec![index_key.clone()],
            )
            .expect("accepted-schema index deletion should build"),
            OnlineOverlayDecision::IndexTombstone,
        ),
        (
            JournalRecord::accepted_schema_index_put(
                "test::Store",
                EntityTag::new(1),
                [0x51; 16],
                vec![index_key.clone()],
            )
            .expect("accepted-schema index insertion should build"),
            OnlineOverlayDecision::IndexPositive,
        ),
        (
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation-job insertion should build"),
            OnlineOverlayDecision::SchemaPositive,
        ),
        (
            JournalRecord::constraint_validation_job_delete(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
            )
            .expect("validation-job deletion should build"),
            OnlineOverlayDecision::SchemaTombstone,
        ),
        (
            JournalRecord::constraint_validation_index_put(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
                index_key.clone(),
            )
            .expect("validation candidate index insertion should build"),
            OnlineOverlayDecision::IndexPositive,
        ),
        (
            identity_range_record(1),
            OnlineOverlayDecision::SchemaPositive,
        ),
        (
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(9),
                vec![0x59; 8],
                [0x69; 16],
                plan,
            )
            .expect("migration row insertion should build"),
            OnlineOverlayDecision::DataPositive,
        ),
        (
            JournalRecord::schema_migration_index_put("test::Store", index_key, plan)
                .expect("migration index insertion should build"),
            OnlineOverlayDecision::IndexPositive,
        ),
    ];

    assert_eq!(records.len(), 12);
    for (record, expected) in records {
        assert_eq!(classify_journal_overlay(&record), expected);
    }
}

#[test]
fn journal_batch_header_inspection_freezes_current_envelope_offsets() {
    let batch = batch(1);
    let encoded = encode_journal_batch(&batch).expect("journal batch should encode");

    let header = inspect_raw_journal_batch_header(&encoded).expect("current header should inspect");
    let fixed_header = inspect_raw_journal_batch_fixed_header(&encoded)
        .expect("current fixed header should inspect");

    assert_eq!(header.total_len(), encoded.len());
    assert_eq!(header.payload_len(), encoded.len() - 9);
    assert_eq!(fixed_header.total_len(), encoded.len());
    assert_eq!(
        u32::from_le_bytes(encoded[5..9].try_into().unwrap()),
        u32::try_from(header.payload_len()).expect("test header length should fit"),
    );
    assert_eq!(fixed_header.batch_id(), batch.batch_id());
    assert_eq!(&encoded[9..25], batch.batch_id().as_slice());
    assert_eq!(fixed_header.commit_marker_id(), batch.commit_marker_id());
    assert_eq!(&encoded[25..41], batch.commit_marker_id().as_slice());
    assert_eq!(fixed_header.journal_sequence(), batch.journal_sequence());
    assert_eq!(
        u64::from_le_bytes(encoded[41..49].try_into().unwrap()),
        batch.journal_sequence().get(),
    );
    assert_eq!(
        fixed_header.database_commit_sequence(),
        batch.database_commit_sequence(),
    );
    assert_eq!(
        u64::from_le_bytes(encoded[49..57].try_into().unwrap()),
        batch.database_commit_sequence().get(),
    );
    assert_eq!(
        fixed_header.record_count(),
        u32::try_from(batch.records().len()).expect("test record count should fit"),
    );
    assert_eq!(
        u32::from_le_bytes(encoded[57..61].try_into().unwrap()),
        u32::try_from(batch.records().len()).expect("test record count should fit"),
    );
    assert_eq!(
        fixed_header.batch_fingerprint().as_slice(),
        &encoded[61..93]
    );
}

#[test]
fn journal_batch_fingerprint_binds_the_exact_domain_and_non_fingerprint_bytes() {
    let encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    let mut expected = Sha256::new();
    expected.update(b"ICYDB-JOURNAL-BATCH-FINGERPRINT\0");
    expected.update(&encoded[..61]);
    expected.update(&encoded[93..]);

    assert_eq!(expected.finalize().as_slice(), &encoded[61..93]);
}

#[test]
fn journal_batch_header_inspection_accepts_the_exact_encoded_maximum() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded.truncate(9);
    let maximum_payload = MAX_JOURNAL_BATCH_BYTES
        .checked_sub(9)
        .expect("journal maximum should include the outer header");
    encoded[5..9].copy_from_slice(&maximum_payload.to_le_bytes());

    let header = inspect_raw_journal_batch_header(&encoded)
        .expect("the exact declared journal maximum should inspect");

    assert_eq!(header.total_len(), MAX_JOURNAL_BATCH_BYTES as usize);
}

#[test]
fn journal_batch_header_inspection_rejects_oversized_declared_payload_without_body() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded.truncate(9);
    encoded[5..9].copy_from_slice(&MAX_JOURNAL_BATCH_BYTES.to_le_bytes());

    let err = inspect_raw_journal_batch_header(&encoded)
        .expect_err("oversized declared payload should fail at header inspection");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_fixed_header_inspection_rejects_oversized_record_count_before_records() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    let impossible_count =
        u32::try_from(MAX_JOURNAL_BATCH_RECORDS + 1).expect("test record count should fit");
    encoded[57..61].copy_from_slice(&impossible_count.to_le_bytes());

    let err = inspect_raw_journal_batch_fixed_header(&encoded)
        .expect_err("oversized record count should fail in fixed-header inspection");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_codec_round_trips_accepted_schema_publication() {
    let batch = JournalBatch::new(
        [0x31; 16],
        [0x41; 16],
        JournalSequence::new(1),
        vec![accepted_schema_publish_record()],
    )
    .expect("accepted schema journal batch should build");

    let encoded = encode_journal_batch(&batch).expect("journal batch should encode");
    let decoded = decode_journal_batch(&encoded).expect("journal batch should decode");

    assert_eq!(decoded, batch);
}

#[test]
fn accepted_schema_publication_rejects_late_duplicate_and_non_schema_suffixes() {
    for records in [
        vec![row_put_record(1), accepted_schema_publish_record()],
        vec![
            accepted_schema_publish_record(),
            accepted_schema_publish_record(),
        ],
        vec![accepted_schema_publish_record(), row_put_record(1)],
        vec![accepted_schema_publish_record(), identity_range_record(1)],
    ] {
        let error = JournalBatch::new([0x3D; 16], [0x4D; 16], JournalSequence::new(12), records)
            .expect_err("accepted schema publication batches must stay schema-owned");

        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn journal_batch_codec_round_trips_bounded_accepted_schema_index_chunks() {
    let deletions = (0..=MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD)
        .map(|value| {
            accepted_schema_index_key(u8::try_from(value).expect("test component should fit"), 1)
        })
        .collect::<Vec<_>>();
    let insertions = (0..=MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD)
        .map(|value| {
            accepted_schema_index_key(u8::try_from(value).expect("test component should fit"), 2)
        })
        .collect::<Vec<_>>();
    let mut records = vec![accepted_schema_publish_record()];
    for keys in deletions.chunks(MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD) {
        records.push(
            JournalRecord::accepted_schema_index_delete(
                "test::Store",
                EntityTag::new(1),
                [0x51; 16],
                keys.to_vec(),
            )
            .expect("accepted schema index deletion should build"),
        );
    }
    for keys in insertions.chunks(MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD) {
        records.push(
            JournalRecord::accepted_schema_index_put(
                "test::Store",
                EntityTag::new(1),
                [0x51; 16],
                keys.to_vec(),
            )
            .expect("accepted schema index insertion should build"),
        );
    }
    let batch = JournalBatch::new([0x35; 16], [0x45; 16], JournalSequence::new(1), records)
        .expect("accepted schema index batch should build");

    let encoded = encode_journal_batch(&batch).expect("accepted schema index batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("accepted schema index batch should decode"),
        batch,
    );
    assert_eq!(batch.records().len(), 5);
}

#[test]
fn accepted_schema_index_replacement_and_validation_job_are_distinct_legal_shapes() {
    let job = validation_job();
    JournalBatch::new(
        [0x36; 16],
        [0x46; 16],
        JournalSequence::new(1),
        vec![
            accepted_schema_publish_record(),
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation job record should build"),
        ],
    )
    .expect("accepted publication plus its validation job should build");

    let error = JournalBatch::new(
        [0x37; 16],
        [0x47; 16],
        JournalSequence::new(2),
        vec![
            accepted_schema_publish_record(),
            JournalRecord::accepted_schema_index_put(
                "test::Store",
                EntityTag::new(1),
                [0x51; 16],
                vec![accepted_schema_index_key(1, 1)],
            )
            .expect("accepted schema index insertion should build"),
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation job record should build"),
        ],
    )
    .expect_err("index replacement and validation-job publication cannot coexist");
    assert_eq!(error.class, ErrorClass::Corruption);
    assert_eq!(error.origin, ErrorOrigin::Store);
}

#[test]
fn accepted_schema_index_chunks_reject_unbound_oversized_and_noncanonical_sets() {
    let key = accepted_schema_index_key(1, 1);
    let unbound = JournalRecord::accepted_schema_index_put(
        "test::Store",
        EntityTag::new(1),
        [0x51; 16],
        vec![key.clone()],
    )
    .expect("structural index chunk should build");
    assert!(
        JournalBatch::new(
            [0x36; 16],
            [0x46; 16],
            JournalSequence::new(1),
            vec![unbound],
        )
        .is_err(),
        "an accepted schema index chunk requires one leading schema publication",
    );

    assert!(
        JournalRecord::accepted_schema_index_put(
            "test::Store",
            EntityTag::new(1),
            [0x51; 16],
            vec![key; MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD + 1],
        )
        .is_err(),
        "one persisted chunk must reject max-plus-one keys before encoding",
    );

    let high = accepted_schema_index_key(2, 2);
    let low = accepted_schema_index_key(1, 1);
    assert!(
        JournalRecord::accepted_schema_index_delete(
            "test::Store",
            EntityTag::new(1),
            [0x51; 16],
            vec![high, low],
        )
        .is_err(),
        "persisted chunk keys must be strictly ordered",
    );
}

#[test]
fn accepted_schema_index_chunks_reject_store_path_drift_from_publication() {
    let error = JournalBatch::new(
        [0x38; 16],
        [0x48; 16],
        JournalSequence::new(3),
        vec![
            accepted_schema_publish_record(),
            JournalRecord::accepted_schema_index_put(
                "other::Store",
                EntityTag::new(1),
                [0x51; 16],
                vec![accepted_schema_index_key(1, 1)],
            )
            .expect("accepted schema index insertion should build"),
        ],
    )
    .expect_err("accepted schema index chunks must bind to the publication store path");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn accepted_schema_index_chunk_decode_rejects_empty_and_max_plus_one_before_keys() {
    let schema = accepted_schema_publish_record();
    let chunk = JournalRecord::accepted_schema_index_put(
        "test::Store",
        EntityTag::new(1),
        [0x51; 16],
        vec![accepted_schema_index_key(1, 1)],
    )
    .expect("bounded accepted schema index insertion should build");
    let schema_bytes = journal_record_payload_len(&schema);
    let batch = JournalBatch::new(
        [0x37; 16],
        [0x47; 16],
        JournalSequence::new(1),
        vec![schema, chunk],
    )
    .expect("accepted schema index batch should build");
    let encoded = encode_journal_batch(&batch).expect("accepted schema index batch should encode");

    // Complete fixed current envelope, then the schema record.
    let chunk_offset = 9 + 16 + 16 + 8 + 8 + 4 + 32 + schema_bytes;
    // Tag + store path + entity + accepted-after fingerprint precede key count.
    let key_count_offset = chunk_offset + 1 + 4 + "test::Store".len() + 8 + 16;
    let key_count_end = key_count_offset + size_of::<u32>();
    assert!(key_count_end <= encoded.len());

    for rejected_count in [0, MAX_ACCEPTED_SCHEMA_INDEX_KEYS_PER_RECORD + 1] {
        let mut corrupt = encoded.clone();
        corrupt[key_count_offset..key_count_end].copy_from_slice(
            &u32::try_from(rejected_count)
                .expect("focused rejected count should fit")
                .to_le_bytes(),
        );
        let error = decode_journal_batch(&corrupt)
            .expect_err("noncanonical persisted index key counts must fail closed");
        assert_eq!(error.class, ErrorClass::Corruption);
        assert_eq!(error.origin, ErrorOrigin::Store);
    }
}

#[test]
fn journal_batch_codec_round_trips_validation_job_replacement_and_removal() {
    let job = validation_job();
    let batch = JournalBatch::new(
        [0x32; 16],
        [0x42; 16],
        JournalSequence::new(1),
        vec![
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation job record should build"),
        ],
    )
    .expect("validation job batch should build");
    let encoded = encode_journal_batch(&batch).expect("validation job batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("validation job batch should decode"),
        batch,
    );

    let removal = JournalBatch::new(
        [0x33; 16],
        [0x43; 16],
        JournalSequence::new(2),
        vec![
            JournalRecord::constraint_validation_job_delete(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
            )
            .expect("validation job removal should build"),
        ],
    )
    .expect("validation job removal batch should build");
    let encoded = encode_journal_batch(&removal).expect("validation job removal should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("validation job removal should decode"),
        removal,
    );
}

#[test]
fn journal_batch_codec_rejects_multiple_validation_job_transitions_without_overlay() {
    let job = validation_job();
    let error = JournalBatch::new(
        [0x3C; 16],
        [0x4C; 16],
        JournalSequence::new(11),
        vec![
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation job record should build"),
            JournalRecord::constraint_validation_job_delete(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
            )
            .expect("validation job removal should build"),
        ],
    )
    .expect_err("one batch must not require multiple validation-job transitions");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn journal_batch_codec_binds_candidate_index_entries_to_validation_job() {
    let job = validation_job();
    let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(9));
    let key = IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(job.entity_tag(), 2, 11),
        IndexKeyKind::User,
        &[b"candidate"],
        &primary_key,
    )
    .expect("candidate index key should build")
    .to_raw()
    .expect("candidate index key should encode");
    let batch = JournalBatch::new(
        [0x34; 16],
        [0x44; 16],
        JournalSequence::new(3),
        vec![
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("validation job record should build"),
            JournalRecord::constraint_validation_index_put(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
                key,
            )
            .expect("candidate index record should build"),
        ],
    )
    .expect("candidate index batch should build");

    let encoded = encode_journal_batch(&batch).expect("candidate index batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("candidate index batch should decode"),
        batch,
    );
}

#[test]
fn journal_batch_codec_round_trips_exact_migration_row_and_index_effects() {
    let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(9));
    let index_key = IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new_with_generation(EntityTag::new(1), 2, 7),
        IndexKeyKind::User,
        &[b"candidate"],
        &primary_key,
    )
    .expect("migration index key should build")
    .to_raw()
    .expect("migration index key should encode");
    let plan = SchemaMigrationPlanDigest::from_bytes([0x71; 32]);
    let batch = JournalBatch::new(
        [0x39; 16],
        [0x49; 16],
        JournalSequence::new(8),
        vec![
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(9),
                vec![0x59; 8],
                [0x69; 16],
                plan,
            )
            .expect("migration row effect should build"),
            JournalRecord::schema_migration_index_put("test::Store", index_key, plan)
                .expect("migration index effect should build"),
        ],
    )
    .expect("migration journal batch should build");

    let encoded = encode_journal_batch(&batch).expect("migration journal batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("migration journal batch should decode"),
        batch,
    );
}

#[test]
fn journal_batch_codec_rejects_duplicate_row_targets_without_overlay() {
    let error = JournalBatch::new(
        [0x3A; 16],
        [0x4A; 16],
        JournalSequence::new(9),
        vec![row_put_record(1), row_delete_record(1)],
    )
    .expect_err("one batch must not require same-key row overlay semantics");

    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn journal_batch_codec_rejects_duplicate_migration_targets_without_overlay() {
    let plan = SchemaMigrationPlanDigest::from_bytes([0x72; 32]);
    let other_plan = SchemaMigrationPlanDigest::from_bytes([0x73; 32]);
    let index_key = accepted_schema_index_key(7, 9);
    for records in [
        vec![
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(9),
                vec![0x59; 8],
                [0x69; 16],
                plan,
            )
            .expect("first migration row effect should build"),
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(9),
                vec![0x5A; 8],
                [0x69; 16],
                plan,
            )
            .expect("second migration row effect should build"),
        ],
        vec![
            JournalRecord::schema_migration_index_put("test::Store", index_key.clone(), plan)
                .expect("first migration index effect should build"),
            JournalRecord::schema_migration_index_put("test::Store", index_key, plan)
                .expect("second migration index effect should build"),
        ],
        vec![
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(10),
                vec![0x59; 8],
                [0x69; 16],
                plan,
            )
            .expect("first-plan migration row effect should build"),
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(11),
                vec![0x5A; 8],
                [0x69; 16],
                other_plan,
            )
            .expect("second-plan migration row effect should build"),
        ],
    ] {
        let error = JournalBatch::new([0x3B; 16], [0x4B; 16], JournalSequence::new(10), records)
            .expect_err("migration batches must not require duplicate or mixed-plan overlay");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn journal_batch_codec_binds_one_identity_range_to_its_exact_ordered_row_set() {
    let batch = JournalBatch::new(
        [0x34; 16],
        [0x44; 16],
        JournalSequence::new(3),
        vec![
            row_put_record(1),
            row_put_record(2),
            identity_range_record(2),
        ],
    )
    .expect("ordered identity range batch should build");
    let encoded = encode_journal_batch(&batch).expect("identity range batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("identity range batch should decode"),
        batch,
    );

    for records in [
        vec![
            row_put_record(2),
            row_put_record(1),
            identity_range_record(2),
        ],
        vec![row_put_record(1), identity_range_record(2)],
        vec![
            row_put_record(1),
            row_delete_record(2),
            identity_range_record(1),
        ],
    ] {
        let error = JournalBatch::new([0x35; 16], [0x45; 16], JournalSequence::new(4), records)
            .expect_err("range/row order, count, and operation mismatches must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn journal_batch_identity_range_filters_mixed_existing_row_transitions() {
    let batch = JournalBatch::new(
        [0x37; 16],
        [0x47; 16],
        JournalSequence::new(6),
        vec![
            row_delete_record(1),
            row_put_record(5),
            row_put_record(3),
            row_put_record(6),
            identity_range_record_from(4, 2),
        ],
    )
    .expect("mixed existing rows and contiguous allocations should build");
    let encoded = encode_journal_batch(&batch).expect("mixed Identity batch should encode");
    assert_eq!(
        decode_journal_batch(&encoded).expect("mixed Identity batch should decode"),
        batch,
    );

    for records in [
        vec![
            row_put_record(5),
            row_delete_record(6),
            identity_range_record_from(4, 1),
        ],
        vec![row_put_record(7), identity_range_record_from(4, 1)],
    ] {
        let error = JournalBatch::new([0x38; 16], [0x48; 16], JournalSequence::new(7), records)
            .expect_err("deleting an allocation or putting above the range must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn identity_range_adds_one_fixed_65_byte_record_per_owner_batch() {
    let row_only = JournalBatch::new(
        [0x36; 16],
        [0x46; 16],
        JournalSequence::new(5),
        vec![row_put_record(1)],
    )
    .expect("row-only comparison batch should build");
    let with_range = JournalBatch::new(
        [0x36; 16],
        [0x46; 16],
        JournalSequence::new(5),
        vec![row_put_record(1), identity_range_record(1)],
    )
    .expect("identity range comparison batch should build");
    let row_only_bytes = encode_journal_batch(&row_only)
        .expect("row-only comparison batch should encode")
        .len();
    let with_range_bytes = encode_journal_batch(&with_range)
        .expect("identity range comparison batch should encode")
        .len();

    assert_eq!(with_range_bytes.saturating_sub(row_only_bytes), 65);
}

#[test]
fn commit_marker_rejects_duplicate_identity_owner_ranges_across_batches() {
    let marker_id = [0x46; 16];
    let first = JournalBatch::new(
        [0x56; 16],
        marker_id,
        JournalSequence::new(1),
        vec![row_put_record(1), identity_range_record_from(0, 1)],
    )
    .expect("first range batch should build");
    let second = JournalBatch::new(
        [0x57; 16],
        marker_id,
        JournalSequence::new(2),
        vec![row_put_record(2), identity_range_record_from(1, 1)],
    )
    .expect("second range batch should build");

    let error = CommitMarker::from_parts(marker_id, vec![first, second])
        .expect_err("one marker must not carry the same identity owner twice");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}

#[test]
fn validation_job_appends_do_not_change_the_durable_data_revision() {
    let mut store = JournalTailStore::init(test_memory(209));
    assert_eq!(
        store
            .data_mutation_revision()
            .expect("initial data revision should load"),
        1,
    );

    let job = validation_job();
    let job_batch = JournalBatch::new(
        [0x51; 16],
        [0x61; 16],
        JournalSequence::new(1),
        vec![
            JournalRecord::constraint_validation_job_put("test::Store", &job)
                .expect("job record should build"),
        ],
    )
    .expect("job batch should build");
    store
        .append_batch(&job_batch)
        .expect("job batch should append");
    assert_eq!(
        store
            .data_mutation_revision()
            .expect("job-only data revision should load"),
        1,
    );

    let row_batch = JournalBatch::new(
        [0x52; 16],
        [0x62; 16],
        JournalSequence::new(2),
        vec![row_put_record(2)],
    )
    .expect("row batch should build");
    store
        .append_batch(&row_batch)
        .expect("row batch should append");
    assert_eq!(
        store
            .data_mutation_revision()
            .expect("row data revision should load"),
        3,
    );

    let removal_batch = JournalBatch::new(
        [0x53; 16],
        [0x63; 16],
        JournalSequence::new(3),
        vec![
            JournalRecord::constraint_validation_job_delete(
                "test::Store",
                job.entity_tag(),
                job.constraint_id(),
            )
            .expect("job removal should build"),
        ],
    )
    .expect("job removal batch should build");
    store
        .append_batch(&removal_batch)
        .expect("job removal batch should append");
    assert_eq!(
        store
            .data_mutation_revision()
            .expect("job removal must not change data revision"),
        3,
    );

    let migration_batch = JournalBatch::new(
        [0x54; 16],
        [0x64; 16],
        JournalSequence::new(4),
        vec![
            JournalRecord::schema_migration_row_put(
                "test::Store",
                raw_data_store_key(4),
                vec![0x74; 8],
                [0x84; 16],
                SchemaMigrationPlanDigest::from_bytes([0x94; 32]),
            )
            .expect("migration row effect should build"),
        ],
    )
    .expect("migration row batch should build");
    store
        .append_batch(&migration_batch)
        .expect("migration row batch should append");
    assert_eq!(
        store
            .data_mutation_revision()
            .expect("migration row data revision should load"),
        5,
    );
}

#[test]
fn access_state_revision_is_durable_and_not_a_journal_batch() {
    let memory = test_memory(253);
    let mut store = JournalTailStore::init(memory.clone());
    assert_eq!(
        store
            .access_state_revision()
            .expect("initial access-state revision should load"),
        1,
    );
    assert_eq!(
        store
            .advance_access_state_revision()
            .expect("access-state transition should advance"),
        2,
    );
    assert_eq!(store.len(), 0);

    drop(store);
    let reopened = JournalTailStore::init(memory);
    assert_eq!(
        reopened
            .access_state_revision()
            .expect("access-state revision should survive reopen"),
        2,
    );
    assert_eq!(reopened.len(), 0);
}

#[test]
fn accepted_schema_publication_record_rejects_revision_gap() {
    let candidate =
        empty_accepted_schema_candidate_for_tests("test::Store", AcceptedSchemaRevision::new(3));

    assert!(
        JournalRecord::accepted_schema_publish(
            "test::Store",
            AcceptedSchemaRevision::INITIAL,
            candidate.encoded_bundle().to_vec(),
            candidate.encoded_root().to_vec(),
        )
        .is_err()
    );
}

#[test]
fn journal_batch_decode_rejects_future_version() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[4] = JOURNAL_BATCH_FORMAT_VERSION_CURRENT.saturating_add(1);

    let err =
        decode_journal_batch(&encoded).expect_err("future journal batch versions must fail closed");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
}

#[test]
fn journal_batch_decode_rejects_corrupt_magic() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[0] = b'X';

    let err = decode_journal_batch(&encoded).expect_err("corrupt magic should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_decode_rejects_empty_bytes() {
    let err = decode_journal_batch(&[]).expect_err("empty journal batch bytes should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_decode_rejects_truncated_payload() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded.truncate(encoded.len().saturating_sub(1));

    let err = decode_journal_batch(&encoded).expect_err("truncated payload should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_decode_rejects_trailing_bytes() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded.push(0xFF);

    let err = decode_journal_batch(&encoded).expect_err("trailing journal batch bytes should fail");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_decode_rejects_unknown_record_tag() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    let first_record_tag_offset = 9 + 16 + 16 + 8 + 8 + 4 + 32;
    encoded[first_record_tag_offset] = 0xFF;

    let err = decode_journal_batch(&encoded).expect_err("unknown record tag should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_decode_rejects_first_middle_and_last_record_corruption() {
    let batch = batch(1);
    let encoded = encode_journal_batch(&batch).expect("journal batch should encode");
    let mut record_offset = 93;

    for record in batch.records() {
        let mut corrupt = encoded.clone();
        corrupt[record_offset] = 0xff;
        let error = decode_journal_batch(&corrupt)
            .expect_err("record corruption at every tested ordinal should fail closed");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        record_offset = record_offset
            .checked_add(journal_record_payload_len(record))
            .expect("bounded record offset should fit");
    }
    assert_eq!(record_offset, encoded.len());
}

#[test]
fn journal_batch_decode_rejects_fingerprint_substitution() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[61] ^= 0x01;

    let err = decode_journal_batch(&encoded)
        .expect_err("substituted journal batch fingerprint must fail closed");

    assert_eq!(err.class(), ErrorClass::Corruption);
    assert_eq!(err.origin(), ErrorOrigin::Store);
}

#[test]
fn raw_journal_batch_decode_rejects_oversized_value_before_payload_parsing() {
    let raw = RawJournalBatch::from_control_bytes(vec![0u8; MAX_JOURNAL_BATCH_BYTES as usize + 1]);

    let err = raw
        .decode()
        .expect_err("oversized raw journal value should fail before payload parsing");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_batch_codec_rejects_oversized_row_payload() {
    let err = JournalRecord::row_put(
        "test::Entity",
        raw_data_store_key(7),
        vec![0u8; crate::db::codec::MAX_ROW_BYTES as usize + 1],
        [0x33; 16],
    )
    .expect_err("oversized row payload should fail");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_sequence_uses_big_endian_storable_order() {
    let low = JournalSequence::new(9);
    let high = JournalSequence::new(10);
    let low_bytes = low.to_bytes().into_owned();

    assert!(low.to_bytes() < high.to_bytes());
    assert_eq!(JournalSequence::from_bytes(Cow::Owned(low_bytes)), low);
}

#[test]
fn journal_tail_store_visits_batches_in_sequence_order_after_watermark() {
    let mut store = JournalTailStore::init(test_memory(210));
    store
        .append_batch(&batch(1))
        .expect("first batch should append");
    store
        .append_batch(&batch(2))
        .expect("second batch should append");

    let mut visited = Vec::new();
    store
        .visit_batches_after(JournalSequence::new(0), |batch| {
            visited.push(batch.journal_sequence().get());
            Ok(())
        })
        .expect("journal tail should visit in order");

    assert_eq!(visited, vec![1, 2]);
    assert_eq!(store.len(), 2);
}

#[test]
fn journal_tail_store_skips_batches_at_or_below_watermark() {
    let mut store = JournalTailStore::init(test_memory(211));
    store.append_batch(&batch(1)).expect("batch should append");
    store.append_batch(&batch(2)).expect("batch should append");

    let mut visited = Vec::new();
    store
        .visit_batches_after(JournalSequence::new(1), |batch| {
            visited.push(batch.journal_sequence().get());
            Ok(())
        })
        .expect("journal tail should skip folded batch");

    assert_eq!(visited, vec![2]);
}

#[test]
fn journal_tail_store_persists_fold_watermark_without_counting_it_as_tail_batch() {
    let mut store = JournalTailStore::init(test_memory(216));
    store.append_batch(&batch(1)).expect("batch should append");
    store.append_batch(&batch(2)).expect("batch should append");

    store
        .persist_fold_watermark(FoldWatermark::new(JournalSequence::new(2), 1))
        .expect("fold watermark should persist");

    let watermark = store
        .fold_watermark()
        .expect("fold watermark should be readable");
    assert_eq!(watermark.highest_folded_journal_sequence().get(), 2);
    assert_eq!(watermark.fold_epoch(), 1);
    assert_eq!(store.len(), 2);
}

#[test]
fn journal_tail_store_cleanup_keeps_watermark_as_replay_boundary() {
    let mut store = JournalTailStore::init(test_memory(217));
    let first = batch(1);
    let second = batch(2);
    store.append_batch(&first).expect("batch should append");
    store.append_batch(&second).expect("batch should append");
    let first_retirement = store
        .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
        .expect("first retirement should preflight");
    store.apply_prepared_batch_retirement(first_retirement);
    assert!(
        store.has_stored_batch(),
        "retiring the first batch must retain the second batch",
    );
    let second_retirement = store
        .prepare_batch_retirement(&second, FoldWatermark::new(JournalSequence::new(2), 2))
        .expect("second retirement should preflight");
    store.apply_prepared_batch_retirement(second_retirement);
    assert!(
        !store.has_stored_batch(),
        "complete retirement must leave the physical tail empty",
    );

    let mut visited = Vec::new();
    store
        .visit_batches_after(
            store
                .fold_watermark()
                .expect("fold watermark should be readable")
                .highest_folded_journal_sequence(),
            |batch| {
                visited.push(batch.journal_sequence().get());
                Ok(())
            },
        )
        .expect("folded tail should read as empty replay tail");

    assert_eq!(visited, Vec::<u64>::new());
    assert_eq!(store.len(), 0);
    assert_eq!(
        store
            .next_append_sequence()
            .expect("next append sequence should account for watermark")
            .get(),
        3,
    );
}

#[test]
fn journal_tail_store_reserves_a_representable_post_commit_revision() {
    let mut store = JournalTailStore::init(test_memory(224));
    store
        .initialize_current_tail_control()
        .expect("current tail control should initialize");
    store
        .persist_fold_watermark(FoldWatermark::new(JournalSequence::new(u64::MAX - 1), 1))
        .expect("near-exhausted fold watermark should persist");

    assert_eq!(
        store
            .next_append_sequence()
            .expect("the final sequence remains representable")
            .get(),
        u64::MAX,
    );
    let error = store
        .next_mutation_append_sequence()
        .expect_err("a mutation must retain a representable post-commit revision");
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::JournalMutationRevisionExhausted,
        }),
    );
}

#[test]
fn journal_batch_rejects_the_unverifiable_final_sequence() {
    let error = JournalBatch::new(
        [0xEF; 16],
        [0xFE; 16],
        JournalSequence::new(u64::MAX),
        vec![row_put_record(1)],
    )
    .expect_err("a mutation batch at the final sequence cannot expose a later revision");

    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::JournalMutationRevisionExhausted,
        }),
    );
}

#[test]
fn journal_tail_store_treats_identical_duplicate_append_as_idempotent() {
    let mut store = JournalTailStore::init(test_memory(212));
    let batch = batch(1);
    store.append_batch(&batch).expect("batch should append");
    store
        .append_batch(&batch)
        .expect("same batch append should be idempotent");

    assert_eq!(store.len(), 1);
}

#[test]
fn journal_tail_store_republishes_missing_chunks_after_prefix_append() {
    let mut store = JournalTailStore::init(test_memory(221));
    let batch = multi_chunk_batch(1);
    let encoded = encode_journal_batch(&batch).expect("multi-chunk batch should encode");
    assert!(
        encoded.len() > JOURNAL_TAIL_CHUNK_BYTES as usize,
        "fixture must span multiple journal-tail chunks",
    );

    store
        .insert_raw_batch_for_tests(
            JournalSequence::new(1),
            encoded[..JOURNAL_TAIL_CHUNK_BYTES as usize].to_vec(),
        )
        .expect("prefix raw journal bytes should insert as an interrupted fixture");
    store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("prefix-only journal batch should fail before republish");

    store
        .append_batch(&batch)
        .expect("republishing the full batch should fill missing chunks");

    let mut visited = Vec::new();
    store
        .visit_batches_after(JournalSequence::new(0), |batch| {
            visited.push(batch.journal_sequence().get());
            Ok(())
        })
        .expect("repaired journal batch should visit cleanly");
    assert_eq!(visited, vec![1]);
    assert_eq!(store.len(), 1);
}

#[test]
fn marker_owned_append_rejects_fixed_header_identity_and_count_mismatches() {
    let encoded_batch = batch(1);
    let encoded = encode_journal_batch(&encoded_batch).expect("journal batch should encode");
    let mismatches = [
        JournalBatch::new(
            [0xF1; 16],
            encoded_batch.commit_marker_id(),
            encoded_batch.journal_sequence(),
            encoded_batch.records().to_vec(),
        )
        .expect("batch-id mismatch fixture should build"),
        JournalBatch::new(
            encoded_batch.batch_id(),
            [0xF2; 16],
            encoded_batch.journal_sequence(),
            encoded_batch.records().to_vec(),
        )
        .expect("marker-id mismatch fixture should build"),
        JournalBatch::new(
            encoded_batch.batch_id(),
            encoded_batch.commit_marker_id(),
            JournalSequence::new(2),
            encoded_batch.records().to_vec(),
        )
        .expect("sequence mismatch fixture should build"),
        JournalBatch::new(
            encoded_batch.batch_id(),
            encoded_batch.commit_marker_id(),
            encoded_batch.journal_sequence(),
            vec![row_put_record(1)],
        )
        .expect("record-count mismatch fixture should build"),
    ];

    for (ordinal, mismatch) in mismatches.iter().enumerate() {
        let memory_id = u8::try_from(232 + ordinal).expect("test memory ID should fit");
        let mut store = JournalTailStore::init(test_memory(memory_id));
        let error = store
            .append_marker_encoded_batch(mismatch, &encoded)
            .expect_err("marker-owned bytes must match every fixed batch identity field");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        assert!(store.is_empty());
    }
}

#[test]
fn journal_tail_store_rejects_batch_at_fold_watermark_control_sequence() {
    let mut store = JournalTailStore::init(test_memory(218));
    let control_sequence_batch = JournalBatch::new(
        [0x01; 16],
        [0xAA; 16],
        JournalSequence::new(0),
        vec![row_put_record(1)],
    )
    .expect("control-sequence batch shape should build before tail append rejects it");

    let err = store
        .append_batch(&control_sequence_batch)
        .expect_err("sequence zero is reserved for fold-watermark control");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_sequence_gap_above_watermark() {
    let mut store = JournalTailStore::init(test_memory(213));
    let gap_batch = batch(2);
    store
        .insert_raw_batch_for_tests(
            gap_batch.journal_sequence(),
            encode_journal_batch(&gap_batch).expect("gap batch should encode"),
        )
        .expect("gap fixture should insert");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("sequence gap should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_corrupt_raw_batch_bytes_during_visit() {
    let mut store = JournalTailStore::init(test_memory(219));
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[0] = b'X';
    store
        .insert_raw_batch_for_tests(JournalSequence::new(1), encoded)
        .expect("corrupt raw journal bytes should insert as a raw persisted fixture");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("corrupt raw journal tail bytes should fail during ordered visit");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_header_declared_trailing_bytes_during_visit() {
    let mut store = JournalTailStore::init(test_memory(230));
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    let declared_payload_len = u32::try_from(encoded.len() - 10)
        .expect("shortened payload length should fit the current header");
    encoded[5..9].copy_from_slice(&declared_payload_len.to_le_bytes());
    store
        .insert_raw_batch_for_tests(JournalSequence::new(1), encoded)
        .expect("trailing raw journal bytes should insert as a raw persisted fixture");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("declared trailing bytes should fail during ordered visit");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_header_sequence_mismatch_during_visit() {
    let mut store = JournalTailStore::init(test_memory(231));
    let encoded = encode_journal_batch(&batch(2)).expect("journal batch should encode");
    store
        .insert_raw_batch_for_tests(JournalSequence::new(1), encoded)
        .expect("sequence-mismatched raw journal bytes should insert as a raw persisted fixture");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("header sequence mismatch should fail during ordered visit");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_truncated_raw_batch_bytes_during_visit() {
    let mut store = JournalTailStore::init(test_memory(220));
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded.truncate(encoded.len().saturating_sub(1));
    store
        .insert_raw_batch_for_tests(JournalSequence::new(1), encoded)
        .expect("truncated raw journal bytes should insert as a raw persisted fixture");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("truncated raw journal tail bytes should fail during ordered visit");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_tail_store_rejects_duplicate_batch_id_at_different_sequence() {
    let mut store = JournalTailStore::init(test_memory(214));
    let first = batch(1);
    let duplicate_id = JournalBatch::new(
        first.batch_id(),
        [0xAA; 16],
        JournalSequence::new(2),
        vec![row_put_record(2)],
    )
    .expect("duplicate id batch shape should build");
    store
        .append_batch(&first)
        .expect("first batch should append");
    store
        .append_batch(&duplicate_id)
        .expect("duplicate id at different sequence is detected during ordered read");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(()))
        .expect_err("duplicate batch ids above watermark should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn journal_inspection_resumes_exact_duplicate_identity_proof_without_a_seen_set() {
    let mut store = JournalTailStore::init(test_memory(225));
    for sequence in 1..=3 {
        store
            .append_batch(&batch(sequence))
            .expect("batch should append");
    }
    let limits = JournalInspectionLimits::for_tests(2, (MAX_JOURNAL_BATCH_BYTES as usize) * 2);

    let first = store
        .inspect_page(JournalInspectionCheckpoint::BeforeFirst, limits)
        .expect("first journal page should inspect");
    assert_eq!(
        first.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 1 },
    );
    assert!(!first.exhausted());

    let second = store
        .inspect_page(first.checkpoint().clone(), limits)
        .expect("second journal page should inspect");
    assert_eq!(
        second.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 2 },
    );
    assert!(!second.exhausted());

    let third = store
        .inspect_page(second.checkpoint().clone(), limits)
        .expect("third journal page should stop inside duplicate-ID proof");
    assert!(matches!(
        third.checkpoint(),
        JournalInspectionCheckpoint::CheckingBatchIdentity {
            sequence: 3,
            next_prior_sequence: 2,
            ..
        },
    ));
    assert!(!third.exhausted());

    let fourth = store
        .inspect_page(third.checkpoint().clone(), limits)
        .expect("fourth journal page should finish the tail");
    assert_eq!(
        fourth.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 3 },
    );
    assert!(fourth.exhausted());
}

#[test]
fn journal_inspection_reports_a_nonadjacent_duplicate_batch_identity() {
    let mut store = JournalTailStore::init(test_memory(226));
    let first = batch(1);
    let third = JournalBatch::new(
        first.batch_id(),
        [0xAA; 16],
        JournalSequence::new(3),
        vec![row_put_record(3)],
    )
    .expect("duplicate-id batch should build");
    store
        .append_batch(&first)
        .expect("first batch should append");
    store
        .append_batch(&batch(2))
        .expect("second batch should append");
    store
        .append_batch(&third)
        .expect("third batch should append");
    let limits = JournalInspectionLimits::for_tests(2, (MAX_JOURNAL_BATCH_BYTES as usize) * 2);

    let first_page = store
        .inspect_page(JournalInspectionCheckpoint::BeforeFirst, limits)
        .expect("first page should inspect");
    let second_page = store
        .inspect_page(first_page.checkpoint().clone(), limits)
        .expect("second page should inspect");
    let third_page = store
        .inspect_page(second_page.checkpoint().clone(), limits)
        .expect("nonadjacent duplicate ID should be a progressable finding");

    assert_eq!(
        third_page.issue(),
        Some(JournalIntegrityIssue::DuplicateBatchIdentity {
            sequence: 3,
            prior_sequence: 1,
        }),
    );
    assert_eq!(
        third_page.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 3 },
    );
    assert!(third_page.exhausted());
    assert!(!third_page.batch_identity_blocked());
}

#[test]
fn journal_inspection_reports_a_sequence_gap_and_resumes_at_the_next_physical_batch() {
    let mut store = JournalTailStore::init(test_memory(228));
    let gap_batch = batch(2);
    store
        .insert_raw_batch_for_tests(
            gap_batch.journal_sequence(),
            encode_journal_batch(&gap_batch).expect("gap batch should encode"),
        )
        .expect("gap fixture should insert");
    let limits = JournalInspectionLimits::for_tests(2, (MAX_JOURNAL_BATCH_BYTES as usize) * 2);

    let gap = store
        .inspect_page(JournalInspectionCheckpoint::BeforeFirst, limits)
        .expect("sequence gap should be a progressable finding");
    assert_eq!(
        gap.issue(),
        Some(JournalIntegrityIssue::SequenceGap {
            expected_sequence: 1,
            next_present_sequence: 2,
        }),
    );
    assert_eq!(
        gap.checkpoint(),
        &JournalInspectionCheckpoint::BeforeBatch { sequence: 2 },
    );
    assert!(!gap.exhausted());
    assert!(gap.batch_identity_blocked());

    let resumed = store
        .inspect_page(gap.checkpoint().clone(), limits)
        .expect("inspection should resume at the physical batch after the gap");
    assert_eq!(
        resumed.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 2 },
    );
    assert!(resumed.exhausted());
    assert_eq!(resumed.issue(), None);
    assert!(resumed.batch_identity_blocked());
}

#[test]
fn journal_inspection_reports_a_malformed_batch_without_hiding_tail_exhaustion() {
    let mut store = JournalTailStore::init(test_memory(229));
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[0] = b'X';
    store
        .insert_raw_batch_for_tests(JournalSequence::new(1), encoded)
        .expect("malformed raw bytes should insert as a persisted fixture");
    let limits = JournalInspectionLimits::for_tests(2, (MAX_JOURNAL_BATCH_BYTES as usize) * 2);

    let page = store
        .inspect_page(JournalInspectionCheckpoint::BeforeFirst, limits)
        .expect("malformed batch should be a progressable finding");

    assert!(matches!(
        page.issue(),
        Some(JournalIntegrityIssue::MalformedBatch {
            sequence: 1,
            incompatible_format: false,
            ..
        }),
    ));
    assert_eq!(
        page.checkpoint(),
        &JournalInspectionCheckpoint::AfterBatch { sequence: 1 },
    );
    assert!(page.exhausted());
    assert!(page.batch_identity_blocked());
}

#[test]
fn journal_proof_identity_tracks_tail_append_and_fold_topology() {
    let mut store = JournalTailStore::init(test_memory(227));
    store
        .initialize_current_tail_control()
        .expect("current tail control should initialize");
    let empty = store
        .proof_identity()
        .expect("empty proof identity should capture");

    let first = batch(1);
    store.append_batch(&first).expect("batch should append");
    let appended = store
        .proof_identity()
        .expect("appended proof identity should capture");
    assert_ne!(appended, empty);
    assert_eq!(appended.next_append_sequence(), 2);
    assert!(appended.physical_record_count() > empty.physical_record_count());

    let retirement = store
        .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
        .expect("retirement should preflight");
    store.apply_prepared_batch_retirement(retirement);
    let folded = store
        .proof_identity()
        .expect("folded proof identity should capture");

    assert_ne!(folded, appended);
    assert_eq!(folded.fold_sequence(), 1);
    assert_eq!(folded.fold_epoch(), 1);
    assert_eq!(folded.data_mutation_revision(), 2);
    assert_eq!(folded.next_append_sequence(), 2);
}

#[test]
fn journal_tail_store_is_empty_before_append() {
    let store = JournalTailStore::init(test_memory(215));

    assert!(store.is_empty());
}

#[test]
fn journal_tail_tiny_append_stays_within_one_memory_manager_bucket() {
    let memory = VectorMemory::default();
    let manager = MemoryManager::init(memory.clone());
    let mut store = JournalTailStore::init(manager.get(MemoryId::new(17)));

    store
        .append_batch(&batch(1))
        .expect("tiny batch should append");

    assert!(
        memory.size() <= SINGLE_MEMORY_MANAGER_BUCKET_PAGES,
        "tiny journal append should not allocate extra MemoryManager buckets; pages={}",
        memory.size()
    );
}

#[test]
fn journal_tail_chunk_storable_bound_caps_raw_tail_value_bytes() {
    assert_eq!(
        RawJournalChunk::BOUND,
        ic_stable_structures::storable::Bound::Bounded {
            max_size: JOURNAL_TAIL_CHUNK_BYTES,
            is_fixed_size: false,
        }
    );
}
