use super::{
    FoldWatermark, JournalBatch, JournalRecord, JournalSequence, JournalTailStore,
    JournalTailVisit,
    codec::{
        JOURNAL_BATCH_FORMAT_VERSION_CURRENT, MAX_JOURNAL_BATCH_BYTES, RawJournalBatch,
        decode_journal_batch, encode_journal_batch,
    },
    store::{JOURNAL_TAIL_CHUNK_BYTES, RawJournalChunk},
};
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        schema::{AcceptedSchemaRevision, empty_accepted_schema_candidate_for_tests},
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
    types::EntityTag,
};
use ic_stable_structures::{
    Memory, Storable, VectorMemory,
    memory_manager::{MemoryId, MemoryManager},
};
use std::borrow::Cow;

const SINGLE_MEMORY_MANAGER_BUCKET_PAGES: u64 = 1 + 128;

fn raw_data_store_key(fill: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::try_from_typed_key(EntityTag::new(1), &fill)
        .expect("test key should encode")
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
fn journal_batch_decode_rejects_pre_0_200_version() {
    let mut encoded = encode_journal_batch(&batch(1)).expect("journal batch should encode");
    encoded[4] = JOURNAL_BATCH_FORMAT_VERSION_CURRENT.saturating_sub(1);

    let err = decode_journal_batch(&encoded)
        .expect_err("pre-0.200 journal batch versions must fail closed");

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
    let first_record_tag_offset = 9 + 16 + 16 + 8 + 4;
    encoded[first_record_tag_offset] = 0xFF;

    let err = decode_journal_batch(&encoded).expect_err("unknown record tag should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
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
            Ok(JournalTailVisit::Continue)
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
            Ok(JournalTailVisit::Continue)
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
    store.append_batch(&batch(1)).expect("batch should append");
    store.append_batch(&batch(2)).expect("batch should append");
    store
        .persist_fold_watermark(FoldWatermark::new(JournalSequence::new(2), 1))
        .expect("fold watermark should persist");

    store.clear_batches_through(JournalSequence::new(2));

    let mut visited = Vec::new();
    store
        .visit_batches_after(
            store
                .fold_watermark()
                .expect("fold watermark should be readable")
                .highest_folded_journal_sequence(),
            |batch| {
                visited.push(batch.journal_sequence().get());
                Ok(JournalTailVisit::Continue)
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
        .visit_batches_after(JournalSequence::new(0), |_| Ok(JournalTailVisit::Continue))
        .expect_err("prefix-only journal batch should fail before republish");

    store
        .append_batch(&batch)
        .expect("republishing the full batch should fill missing chunks");

    let mut visited = Vec::new();
    store
        .visit_batches_after(JournalSequence::new(0), |batch| {
            visited.push(batch.journal_sequence().get());
            Ok(JournalTailVisit::Continue)
        })
        .expect("repaired journal batch should visit cleanly");
    assert_eq!(visited, vec![1]);
    assert_eq!(store.len(), 1);
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
    store.append_batch(&batch(2)).expect("batch should append");

    let err = store
        .visit_batches_after(JournalSequence::new(0), |_| Ok(JournalTailVisit::Continue))
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
        .visit_batches_after(JournalSequence::new(0), |_| Ok(JournalTailVisit::Continue))
        .expect_err("corrupt raw journal tail bytes should fail during ordered visit");

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
        .visit_batches_after(JournalSequence::new(0), |_| Ok(JournalTailVisit::Continue))
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
        .visit_batches_after(JournalSequence::new(0), |_| Ok(JournalTailVisit::Continue))
        .expect_err("duplicate batch ids above watermark should fail closed");

    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Store);
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
