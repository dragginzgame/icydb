use super::*;
use crate::{
    db::{
        codec::MAX_ROW_BYTES,
        data::DecodedDataStoreKey,
        journal::{
            DatabaseCommitSequence, JournalBatch, encode_journal_batch, journal_batch_encoded_len,
            journal_record_payload_len,
        },
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    },
    error::{ErrorClass, ErrorOrigin},
    testing::test_memory,
};

fn batch(journal_sequence: u64, database_commit_sequence: u64) -> JournalBatch {
    let batch_id = u8::try_from(journal_sequence).expect("test sequence should fit u8");
    JournalBatch::new_with_database_commit_sequence(
        [batch_id; 16],
        [0xA5; 16],
        JournalSequence::new(journal_sequence),
        DatabaseCommitSequence::new(database_commit_sequence),
        Vec::new(),
    )
    .expect("test journal batch should build")
}

#[test]
fn full_and_prefix_reads_share_chunk_order_but_keep_distinct_completeness_rules() {
    let sequence = JournalSequence::new(1);
    let encoded = encode_journal_batch(&multi_chunk_batch(1, 1)).unwrap();
    for missing_chunk in [0, 1] {
        let mut store =
            JournalTailStore::init(test_memory(241)).expect("fixture storage should initialize");
        assert!(
            store
                .raw_batch_bytes_for_sequence(sequence)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .raw_batch_prefix_bytes_for_sequence(sequence)
                .unwrap()
                .is_none()
        );
        store
            .insert_raw_batch_for_tests(sequence, encoded.clone())
            .unwrap();
        assert_eq!(
            store.raw_batch_bytes_for_sequence(sequence).unwrap(),
            Some(encoded.clone())
        );
        assert_eq!(
            store.raw_batch_prefix_bytes_for_sequence(sequence).unwrap(),
            Some(encoded.clone())
        );
        assert!(
            store
                .map
                .remove(&JournalTailKey::new(sequence, missing_chunk))
                .is_some()
        );
        for result in [
            store.raw_batch_bytes_for_sequence(sequence),
            store.raw_batch_prefix_bytes_for_sequence(sequence),
        ] {
            let error = result.unwrap_err();
            assert_eq!(error.class(), ErrorClass::Corruption);
            assert_eq!(error.origin(), ErrorOrigin::Store);
        }
    }
    let mut store =
        JournalTailStore::init(test_memory(241)).expect("fixture storage should initialize");
    let prefix = encoded[..JOURNAL_TAIL_CHUNK_BYTES as usize].to_vec();
    store
        .insert_raw_batch_for_tests(sequence, prefix.clone())
        .unwrap();
    assert_eq!(
        store.raw_batch_prefix_bytes_for_sequence(sequence).unwrap(),
        Some(prefix)
    );
    assert_eq!(
        store
            .raw_batch_bytes_for_sequence(sequence)
            .unwrap_err()
            .class(),
        ErrorClass::Corruption
    );
}

fn set_control(store: &mut JournalTailStore, control: JournalTailControl) {
    store.map.insert(
        JournalTailKey::tail_convergence_control(),
        RawJournalChunk::from_bytes(encode_tail_control(control)),
    );
}

fn multi_chunk_batch(journal_sequence: u64, database_commit_sequence: u64) -> JournalBatch {
    let key = DecodedDataStoreKey::new_primary_key_value(
        EntityTag::new(1),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .to_raw()
    .unwrap();
    let record = JournalRecord::row_put(
        "test::RetirementEntity",
        key,
        vec![0xA5; 3 * JOURNAL_TAIL_CHUNK_BYTES as usize],
        [0xC1; 16],
    )
    .unwrap();
    JournalBatch::new_with_database_commit_sequence(
        [u8::try_from(journal_sequence).unwrap(); 16],
        [0xA5; 16],
        JournalSequence::new(journal_sequence),
        DatabaseCommitSequence::new(database_commit_sequence),
        vec![record],
    )
    .unwrap()
}

#[test]
fn prior_identity_inspection_does_not_trust_a_corrupted_matching_header() {
    let mut store =
        JournalTailStore::init(test_memory(174)).expect("fixture storage should initialize");
    store.initialize_current_tail_control().unwrap();
    let first = batch(1, 7);
    let second = batch(2, 8);
    store.append_batch(&first).unwrap();
    store.append_batch(&second).unwrap();
    let forged = JournalBatch::new_with_database_commit_sequence(
        second.batch_id(),
        [0xA5; 16],
        JournalSequence::new(1),
        DatabaseCommitSequence::new(7),
        Vec::new(),
    )
    .unwrap();
    let mut bytes = encode_journal_batch(&forged).unwrap();
    // An empty batch ends in its fingerprint. Its fixed identity header
    // still parses, but full decoding must reject the tampered fingerprint.
    *bytes.last_mut().unwrap() ^= 1;
    assert_eq!(
        inspect_raw_journal_batch_fixed_header(&bytes)
            .unwrap()
            .batch_id(),
        second.batch_id(),
    );
    store.map.insert(
        JournalTailKey::new(JournalSequence::new(1), 0),
        RawJournalChunk::from_bytes(bytes),
    );
    let page = store
        .inspect_page(
            JournalInspectionCheckpoint::BeforeBatch { sequence: 2 },
            JournalInspectionLimits::for_tests(2, MAX_JOURNAL_BATCH_BYTES as usize * 2),
        )
        .unwrap();
    assert!(page.batch_identity_blocked());
    assert_eq!(page.issue(), None);
    assert!(page.exhausted());
}

#[test]
fn retirement_uses_next_header_but_consumption_validates_all_chunks() {
    for missing_chunk in [None, Some(1), Some(3)] {
        let memory = crate::testing::test_memory_owner(174);
        let mut store =
            JournalTailStore::init(memory.open()).expect("fixture storage should initialize");
        store.initialize_current_tail_control().unwrap();
        let first = batch(1, 7);
        let second = multi_chunk_batch(2, 8);
        let second_bytes = encode_journal_batch(&second).unwrap();
        store.append_batch(&first).unwrap();
        store.append_batch(&second).unwrap();
        if let Some(chunk) = missing_chunk {
            assert!(
                store
                    .map
                    .remove(&JournalTailKey::new(JournalSequence::new(2), chunk))
                    .is_some()
            );
        }

        let watermark = FoldWatermark::new(JournalSequence::new(1), 1);
        let retirement = store.prepare_batch_retirement(&first, watermark).unwrap();
        store.apply_prepared_batch_retirement(retirement);
        drop(store);
        let reopened =
            JournalTailStore::init(memory.open()).expect("fixture storage should initialize");
        let control = reopened.current_tail_control().unwrap();
        assert_eq!(control.batch_count(), 1);
        assert_eq!(control.record_count(), 1);
        assert_eq!(control.encoded_batch_bytes(), second_bytes.len() as u64);
        assert_eq!(
            control.head_database_commit_sequence(),
            Some(DatabaseCommitSequence::new(8))
        );
        assert!((reopened.has_stored_batch()));
        let next = reopened.next_batch_after(JournalSequence::new(1));
        if missing_chunk.is_some() {
            assert_eq!(next.unwrap_err().class(), ErrorClass::Corruption);
        } else {
            assert_eq!(next.unwrap().unwrap(), second);
        }
    }
}

#[test]
fn retirement_key_collection_skips_chunk_payloads_and_other_sequences() {
    use ic_storage::{StorageConfig, VectorMemory, testing::MeteredMemory};

    let backing = MeteredMemory::new(VectorMemory::default());
    let runtime = crate::testing::test_memory_runtime(backing.clone(), StorageConfig::default());
    let memory = runtime
        .open_memory(&ic_storage::StableKey::parse("icydb.core_tests.slot_174.v1").unwrap())
        .unwrap();
    let mut map = StableBTreeMap::create(memory).unwrap();
    for sequence in 1..=3 {
        for chunk in [0, 1, u32::MAX] {
            map.insert(
                JournalTailKey::new(JournalSequence::new(sequence), chunk),
                RawJournalChunk::from_bytes(vec![31; JOURNAL_TAIL_CHUNK_BYTES as usize]),
            );
        }
    }
    backing.reset();
    let sequence = JournalSequence::new(2);
    assert_eq!(
        JournalTailStore::retirement_keys(&map, sequence),
        [0, 1, u32::MAX].map(|chunk| JournalTailKey::new(sequence, chunk))
    );
    let counts = backing.counts();
    assert!(!counts.overflowed);
    assert!(
        counts.read_bytes < u64::from(JOURNAL_TAIL_CHUNK_BYTES),
        "{counts:?}"
    );
    assert_eq!(counts.writes, 0);
    assert_eq!(counts.grows, 0);
    assert!(JournalTailStore::retirement_keys(&map, JournalSequence::new(4)).is_empty());
    backing.reset();
    assert!(map.range(..).next().is_some());
    assert!(backing.counts().read_bytes >= u64::from(JOURNAL_TAIL_CHUNK_BYTES));
}

#[test]
fn retirement_rejects_missing_malformed_or_miskeyed_next_headers_without_effects() {
    for invalid_header in [0, 1, 2] {
        let mut store =
            JournalTailStore::init(test_memory(174)).expect("fixture storage should initialize");
        store.initialize_current_tail_control().unwrap();
        let first = batch(1, 7);
        store.append_batch(&first).unwrap();
        store.append_batch(&multi_chunk_batch(2, 8)).unwrap();
        let key = JournalTailKey::new(JournalSequence::new(2), 0);
        match invalid_header {
            0 => {
                store.map.remove(&key).unwrap();
            }
            1 => {
                store
                    .map
                    .insert(key, RawJournalChunk::from_bytes(vec![0xFF]));
            }
            2 => {
                store.map.insert(
                    key,
                    RawJournalChunk::from_bytes(encode_journal_batch(&batch(3, 8)).unwrap()),
                );
            }
            _ => unreachable!(),
        }
        let before = store.current_tail_control().unwrap();
        let watermark = store.fold_watermark().unwrap();
        let error = store
            .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
            .err()
            .unwrap();
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(store.current_tail_control().unwrap(), before);
        assert_eq!(store.fold_watermark().unwrap(), watermark);
        assert_eq!(
            store
                .next_batch_after(JournalSequence::new(0))
                .unwrap()
                .unwrap(),
            first
        );
    }
}

#[test]
fn exact_controls_append_replay_retire_and_reopen() {
    let memory = crate::testing::test_memory_owner(176);
    let mut store =
        JournalTailStore::init(memory.open()).expect("fixture storage should initialize");
    store
        .initialize_current_tail_control()
        .expect("empty current control should initialize");
    let first = batch(1, 7);
    let second = batch(2, 8);
    let first_bytes = encode_journal_batch(&first).expect("first batch should encode");
    let second_bytes = encode_journal_batch(&second).expect("second batch should encode");

    store
        .append_batch(&first)
        .expect("first batch should append");
    let first_control = store.current_tail_control().expect("control should decode");
    assert_eq!(first_control.batch_count(), 1);
    assert_eq!(first_control.record_count(), 0);
    assert_eq!(
        first_control.encoded_batch_bytes(),
        first_bytes.len() as u64
    );
    assert_eq!(
        first_control.head_database_commit_sequence(),
        Some(DatabaseCommitSequence::new(7)),
    );

    store
        .append_batch(&first)
        .expect("identical replay should be idempotent");
    assert_eq!(store.current_tail_control().unwrap(), first_control);
    store
        .append_batch(&second)
        .expect("second batch should append");
    let both = store.current_tail_control().expect("control should decode");
    assert_eq!(both.batch_count(), 2);
    assert_eq!(
        both.encoded_batch_bytes(),
        (first_bytes.len() + second_bytes.len()) as u64
    );
    assert_eq!(
        both.head_database_commit_sequence(),
        Some(DatabaseCommitSequence::new(7)),
    );

    let retirement = store
        .prepare_batch_retirement(&first, FoldWatermark::new(JournalSequence::new(1), 1))
        .expect("first retirement should preflight");
    store.apply_prepared_batch_retirement(retirement);
    drop(store);

    let mut reopened =
        JournalTailStore::init(memory.open()).expect("fixture storage should initialize");
    let remaining = reopened
        .current_tail_control()
        .expect("control should reopen");
    assert_eq!(remaining.batch_count(), 1);
    assert_eq!(remaining.encoded_batch_bytes(), second_bytes.len() as u64);
    assert_eq!(
        remaining.head_database_commit_sequence(),
        Some(DatabaseCommitSequence::new(8)),
    );
    let retirement = reopened
        .prepare_batch_retirement(&second, FoldWatermark::new(JournalSequence::new(2), 2))
        .expect("second retirement should preflight");
    reopened.apply_prepared_batch_retirement(retirement);
    assert_eq!(
        reopened
            .current_tail_control()
            .expect("control should decode"),
        JournalTailControl::empty(),
    );
    assert!(!(reopened.has_stored_batch()));
}

#[test]
fn exact_maximum_encoded_batch_append_is_counted_without_approximation() {
    let record = |ordinal, payload_len| {
        let key = DecodedDataStoreKey::new_primary_key_value(
            EntityTag::new(1),
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(ordinal)),
        )
        .to_raw()
        .unwrap();
        JournalRecord::row_put(
            "test::MaximumEntity",
            key,
            vec![0xA5; payload_len],
            [0xC1; 16],
        )
        .unwrap()
    };
    let mut records = (1..4)
        .map(|ordinal| record(ordinal, MAX_ROW_BYTES as usize))
        .collect::<Vec<_>>();
    let final_empty = record(4, 0);
    let used = JOURNAL_BATCH_FIXED_HEADER_BYTES
        + records
            .iter()
            .map(journal_record_payload_len)
            .sum::<usize>();
    let final_payload_len = (MAX_JOURNAL_BATCH_BYTES as usize)
        .checked_sub(used)
        .and_then(|remaining| remaining.checked_sub(journal_record_payload_len(&final_empty)))
        .expect("maximum envelope should retain one final row payload");
    assert!(final_payload_len <= MAX_ROW_BYTES as usize);
    records.push(record(4, final_payload_len));
    let batch = JournalBatch::new_with_database_commit_sequence(
        [0xC1; 16],
        [0xC2; 16],
        JournalSequence::new(1),
        DatabaseCommitSequence::new(1),
        records,
    )
    .unwrap();
    assert_eq!(
        journal_batch_encoded_len(&batch),
        MAX_JOURNAL_BATCH_BYTES as usize
    );

    let mut store =
        JournalTailStore::init(test_memory(174)).expect("fixture storage should initialize");
    store.initialize_current_tail_control().unwrap();
    store.append_batch(&batch).unwrap();
    let control = store.current_tail_control().unwrap();
    assert_eq!(control.batch_count(), 1);
    assert_eq!(
        control.record_count(),
        u64::try_from(batch.records().len()).unwrap()
    );
    assert_eq!(
        control.encoded_batch_bytes(),
        u64::from(MAX_JOURNAL_BATCH_BYTES),
    );
}

#[test]
fn append_rejects_nonmonotonic_database_sequence_before_tail_mutation() {
    let mut store =
        JournalTailStore::init(test_memory(177)).expect("fixture storage should initialize");
    store.initialize_current_tail_control().unwrap();
    store.append_batch(&batch(1, 10)).unwrap();

    let error = store
        .append_batch(&batch(2, 9))
        .expect_err("database order must advance across local appends");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    assert_eq!(store.current_tail_control().unwrap().batch_count(), 1);
}

#[test]
fn aggregate_overflow_and_retirement_underflow_fail_closed() {
    let mut overflow =
        JournalTailStore::init(test_memory(178)).expect("fixture storage should initialize");
    overflow.initialize_current_tail_control().unwrap();
    set_control(
        &mut overflow,
        JournalTailControl {
            batch_count: u64::MAX,
            record_count: 0,
            encoded_batch_bytes: u64::MAX,
            head_database_commit_sequence: Some(DatabaseCommitSequence::new(1)),
        },
    );
    assert!(overflow.append_batch(&batch(1, 2)).is_err());
    assert!(!(overflow.has_stored_batch()));

    let mut underflow =
        JournalTailStore::init(test_memory(179)).expect("fixture storage should initialize");
    underflow.initialize_current_tail_control().unwrap();
    let retained = batch(1, 3);
    underflow.append_batch(&retained).unwrap();
    let encoded_len = encode_journal_batch(&retained).unwrap().len() as u64;
    set_control(
        &mut underflow,
        JournalTailControl {
            batch_count: 1,
            record_count: 0,
            encoded_batch_bytes: encoded_len - 1,
            head_database_commit_sequence: Some(DatabaseCommitSequence::new(3)),
        },
    );
    assert!(
        underflow
            .prepare_batch_retirement(&retained, FoldWatermark::new(JournalSequence::new(1), 1),)
            .is_err()
    );
    assert!((underflow.has_stored_batch()));
}

#[test]
fn missing_malformed_and_mismatched_head_controls_fail_closed() {
    let mut store =
        JournalTailStore::init(test_memory(180)).expect("fixture storage should initialize");
    assert!(store.current_tail_control().is_err());
    store.map.insert(
        JournalTailKey::tail_convergence_control(),
        RawJournalChunk::from_bytes(vec![0xFF; TAIL_CONVERGENCE_BYTES]),
    );
    assert!(store.current_tail_control().is_err());

    let mut entity_authority =
        JournalTailStore::init(test_memory(181)).expect("fixture storage should initialize");
    entity_authority.initialize_current_tail_control().unwrap();
    entity_authority
        .map
        .remove(&JournalTailKey::entity_mutation_revision_control());
    let error = entity_authority
        .validate_current_tail_authority()
        .expect_err("current tail authority requires its entity-revision control");
    assert_eq!(error.class(), ErrorClass::Corruption);
    assert_eq!(error.origin(), ErrorOrigin::Store);
    entity_authority.map.insert(
        JournalTailKey::entity_mutation_revision_control(),
        RawJournalChunk::from_bytes(vec![0xFF; ENTITY_MUTATION_REVISION_CONTROL_BYTES]),
    );
    assert!(entity_authority.validate_current_tail_authority().is_err());

    let mut mismatch =
        JournalTailStore::init(test_memory(175)).expect("fixture storage should initialize");
    mismatch.initialize_current_tail_control().unwrap();
    let retained = batch(1, 4);
    mismatch.append_batch(&retained).unwrap();
    let mut control = mismatch.current_tail_control().unwrap();
    control.head_database_commit_sequence = Some(DatabaseCommitSequence::new(5));
    set_control(&mut mismatch, control);
    assert!(mismatch.append_batch(&retained).is_err());
    assert!(
        mismatch
            .prepare_batch_retirement(&retained, FoldWatermark::new(JournalSequence::new(1), 1),)
            .is_err()
    );
}
