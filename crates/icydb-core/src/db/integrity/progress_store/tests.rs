use super::*;
use crate::{
    db::{
        MutationJobAdvanceRequest, MutationJobIdempotencyKey, MutationJobPhase,
        MutationJobRestartReason, MutationJobStatus, ReadSetRevisionProof, ReadSetStoreIdentity,
        ReadSetStoreRevision, integrity::progress_codec::current_job_codec_fixture,
        mutation_job::MutationJobTransition,
    },
    testing::test_memory,
    traits::Path,
};

#[test]
fn retained_store_corruption_does_not_initialize_a_replacement() {
    crate::testing::assert_retained_store_header_preserved(|memory| {
        InspectionProgressStore::open(memory)
            .map_err(|_| crate::error::InternalError::store_corruption())
    });
}

struct RetentionCanister;

impl CanisterKind for RetentionCanister {
    const COMMIT_STABLE_KEY: &'static str = "tests::retention::commit";
    const STARTUP_STABLE_KEY: &'static str = "tests::retention::startup";
    const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "tests::retention::progress";

    fn integrity_progress_memory_id() -> Result<u64, ic_storage::RuntimeOpenError> {
        Ok(253)
    }
}

impl Path for RetentionCanister {
    const PATH: &'static str = "tests::RetentionCanister";
}

fn current_resumable_record() -> ResumableJobRecord {
    let proof = ReadSetRevisionProof::from_parts(
        [1; 16],
        7,
        1,
        [2; 32],
        vec![ReadSetStoreRevision::new(
            ReadSetStoreIdentity::from_bytes([3; 32]),
            11,
            13,
        )],
    )
    .expect("bounded canonical proof should admit");
    ResumableJobRecord::new(
        ResumableJobId::try_from_bytes([4; 32])
            .expect("nonzero resumable job identity should admit"),
        proof,
        vec![5, 6],
    )
    .expect("current resumable record should admit")
}

fn mutation_job_id(byte: u8) -> MutationJobId {
    MutationJobId::try_from_bytes([byte; 32]).expect("nonzero mutation job id should admit")
}

fn current_mutation_record(byte: u8) -> MutationJobRecord {
    MutationJobRecord::new(mutation_job_id(byte), vec![1, 2, 3], vec![4, 5])
        .expect("current mutation record should admit")
}

fn current_integrity_record(byte: u8) -> IntegrityJob {
    let mut job = current_job_codec_fixture();
    let job_id =
        IntegrityJobId::try_from_bytes([byte; 32]).expect("nonzero integrity job id should admit");
    job.id = job_id;
    match &mut job.last_receipt.receipt {
        crate::db::IntegrityJobReceipt::Page(page) => page.job_id = job_id,
        crate::db::IntegrityJobReceipt::Abort(receipt) => receipt.job_id = job_id,
    }
    job.validate()
        .expect("rewritten integrity job should admit");
    job
}

fn insert_mutation_record(store: &mut InspectionProgressStore, record: &MutationJobRecord) {
    assert!(matches!(
        store
            .insert_mutation(record)
            .expect("mutation record should insert"),
        InsertMutationJobResult::Inserted,
    ));
}

fn mutation_request(byte: u8, sequence: u64, key: &str) -> MutationJobAdvanceRequest {
    MutationJobAdvanceRequest::new(
        mutation_job_id(byte),
        sequence,
        MutationJobIdempotencyKey::new(key).expect("bounded replay key should admit"),
    )
}

#[test]
fn progress_header_rejects_future_version_and_checksum_corruption() {
    let mut future = encode_progress_header();
    future[PROGRESS_HEADER_MAGIC.len()] = PROGRESS_HEADER_VERSION + 1;
    assert_eq!(
        decode_progress_header(&future),
        Err(IntegrityJobError::IncompatibleProgressFormat),
    );

    let mut corrupt = encode_progress_header();
    let last = corrupt
        .last_mut()
        .expect("current progress header has a checksum");
    *last ^= 0xff;
    assert_eq!(
        decode_progress_header(&corrupt),
        Err(IntegrityJobError::CorruptProgressHeader),
    );
}

#[test]
fn current_wire_jobs_reopen_without_changing_other_progress_families() {
    let owner = crate::testing::test_memory_owner(246);
    let integrity = current_job_codec_fixture();
    let resumable = current_resumable_record();
    let mutation = current_mutation_record(7);
    let mut store = InspectionProgressStore::open(owner.open()).unwrap();
    store.insert_new(&integrity).unwrap();
    store.insert_resumable(&resumable).unwrap();
    store.insert_mutation(&mutation).unwrap();
    drop(store);
    let mut store = InspectionProgressStore::open(owner.open()).unwrap();
    assert_eq!(store.load(integrity.id).unwrap(), integrity);
    assert_eq!(
        store.load_resumable(resumable.state().job_id).unwrap(),
        resumable
    );
    assert_eq!(
        store.load_mutation(mutation.state().job_id).unwrap(),
        mutation
    );
    assert_eq!(store.inventory().unwrap().records.len(), 3);
    // Replaying a start returns the retained receipt rather than replacing it.
    assert!(matches!(
        store.insert_new(&integrity).unwrap(),
        InsertJobResult::Occupied(_)
    ));
}

#[test]
fn checksummed_wire_corruption_cannot_restore_or_erase_job_authority() {
    let job = current_job_codec_fixture();
    let mut payload = encode_integrity_job_payload(&job).unwrap();
    // The fixture's frozen prefix ends at its first checkpoint tag.
    assert_eq!(payload[162], 0);
    payload[162] = 127;
    let corrupt = encode_progress_record_payload(
        &payload,
        *JOB_RECORD_MAGIC,
        MAX_PROGRESS_RECORD_BYTES as usize,
        IntegrityJobError::CapacityExceeded,
        |_| IntegrityJobError::CapacityExceeded,
    )
    .unwrap();
    let mut store = InspectionProgressStore::open(test_memory(245)).unwrap();
    let key = ProgressRecordKey::from_job_id(job.id);
    store
        .map
        .insert(key, ProgressRecordBytes(corrupt.clone()))
        .expect("fixture storage operation should succeed");
    assert_eq!(
        store.load(job.id),
        Err(IntegrityJobError::CorruptProgressRecord)
    );
    assert_eq!(store.map.get(&key).unwrap().unwrap().0, corrupt);
}

#[test]
fn current_wire_job_record_preserves_envelope_and_payload() {
    let job = current_job_codec_fixture();
    let encoded = encode_job_record(&job).expect("current job should encode");

    assert_eq!(encoded[JOB_RECORD_MAGIC.len()], 1);
    assert_eq!(&encoded[..8], b"ICYIJWIR");
    assert_eq!(
        decode_job_record(&encoded, job.id).expect("current job should decode"),
        job,
    );

    let mut corrupt = encoded;
    let last = corrupt
        .last_mut()
        .expect("current job record has a payload");
    *last ^= 0xff;
    assert_eq!(
        decode_job_record(&corrupt, job.id),
        Err(IntegrityJobError::CorruptProgressRecord),
    );
}

#[test]
fn current_resumable_record_is_direct_bounded_and_checksum_protected() {
    let record = current_resumable_record();
    let encoded =
        encode_resumable_job_record(&record).expect("current resumable record should encode");

    assert_eq!(encoded.len(), 175);
    assert_eq!(encoded[RESUMABLE_JOB_RECORD_MAGIC.len()], 1);
    assert!(!encoded[PROGRESS_JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
    assert_eq!(
        decode_resumable_job_record(&encoded, record.state().job_id)
            .expect("current resumable record should decode"),
        record,
    );

    let mut future = encoded.clone();
    future[RESUMABLE_JOB_RECORD_MAGIC.len()] = PROGRESS_JOB_RECORD_VERSION + 1;
    assert_eq!(
        decode_resumable_job_record(&future, record.state().job_id),
        Err(ResumableJobError::IncompatibleProgressFormat),
    );

    let mut corrupt = encoded;
    let last = corrupt
        .last_mut()
        .expect("current resumable record has a payload");
    *last ^= 0xff;
    assert_eq!(
        decode_resumable_job_record(&corrupt, record.state().job_id),
        Err(ResumableJobError::CorruptProgressStore),
    );
}

#[test]
fn current_mutation_record_is_distinct_bounded_and_checksum_protected() {
    let record = current_mutation_record(7);
    let encoded =
        encode_mutation_job_record(&record).expect("current mutation record should encode");

    assert_eq!(encoded.len(), 97);
    assert_eq!(encoded[MUTATION_JOB_RECORD_MAGIC.len()], 1);
    assert!(!encoded[PROGRESS_JOB_RECORD_HEADER_BYTES..].starts_with(b"DIDL"));
    assert_eq!(
        decode_mutation_job_record(&encoded, record.state().job_id)
            .expect("current mutation record should decode"),
        record,
    );

    let mut future = encoded.clone();
    future[MUTATION_JOB_RECORD_MAGIC.len()] = PROGRESS_JOB_RECORD_VERSION + 1;
    assert_eq!(
        decode_mutation_job_record(&future, record.state().job_id),
        Err(MutationJobError::IncompatibleProgressFormat),
    );

    let mut corrupt = encoded;
    let last = corrupt
        .last_mut()
        .expect("current mutation record has a payload");
    *last ^= 0xff;
    assert_eq!(
        decode_mutation_job_record(&corrupt, record.state().job_id),
        Err(MutationJobError::CorruptProgressStore),
    );

    let mut oversized = vec![0; MAX_MUTATION_JOB_RECORD_BYTES + 1];
    oversized[..MUTATION_JOB_RECORD_MAGIC.len()].copy_from_slice(MUTATION_JOB_RECORD_MAGIC);
    oversized[MUTATION_JOB_RECORD_MAGIC.len()] = PROGRESS_JOB_RECORD_VERSION;
    assert_eq!(
        decode_mutation_job_record(&oversized, record.state().job_id),
        Err(MutationJobError::CorruptProgressStore),
    );
}

#[test]
fn job_envelopes_preserve_family_rejections() {
    fn check<E: std::fmt::Debug + PartialEq>(
        encoded: &[u8],
        max_bytes: usize,
        decode: impl Fn(&[u8]) -> Result<(), E>,
        incompatible: E,
        corrupt: E,
    ) {
        assert_eq!(decode(encoded), Ok(()));
        for end in 0..encoded.len() {
            let expected = if end < PROGRESS_JOB_RECORD_HEADER_BYTES {
                &incompatible
            } else {
                &corrupt
            };
            assert_eq!(&decode(&encoded[..end]).unwrap_err(), expected);
        }
        for offset in [0, 8, 9, 13, encoded.len() - 1] {
            let mut invalid = encoded.to_vec();
            invalid[offset] ^= 0xff;
            let expected = if offset < 9 { &incompatible } else { &corrupt };
            assert_eq!(&decode(&invalid).unwrap_err(), expected);
        }
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert_eq!(decode(&trailing).unwrap_err(), corrupt);
        // A consistent length and checksum cannot bypass the family ceiling.
        let mut oversized = encoded.to_vec();
        oversized.resize(max_bytes + 1, 0);
        let payload = &oversized[PROGRESS_JOB_RECORD_HEADER_BYTES..];
        let len = u32::try_from(payload.len()).unwrap();
        let checksum = crc32c(payload);
        oversized[9..13].copy_from_slice(&len.to_be_bytes());
        oversized[13..17].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(decode(&oversized).unwrap_err(), corrupt);
    }

    let integrity = current_job_codec_fixture();
    check(
        &encode_job_record(&integrity).unwrap(),
        MAX_PROGRESS_RECORD_BYTES as usize,
        |bytes| decode_job_record(bytes, integrity.id).map(|_| ()),
        IntegrityJobError::IncompatibleProgressFormat,
        IntegrityJobError::CorruptProgressRecord,
    );
    let resumable = current_resumable_record();
    check(
        &encode_resumable_job_record(&resumable).unwrap(),
        MAX_PROGRESS_RECORD_BYTES as usize,
        |bytes| decode_resumable_job_record(bytes, resumable.state().job_id).map(|_| ()),
        ResumableJobError::IncompatibleProgressFormat,
        ResumableJobError::CorruptProgressStore,
    );
    let mutation = current_mutation_record(7);
    check(
        &encode_mutation_job_record(&mutation).unwrap(),
        MAX_MUTATION_JOB_RECORD_BYTES,
        |bytes| decode_mutation_job_record(bytes, mutation.state().job_id).map(|_| ()),
        MutationJobError::IncompatibleProgressFormat,
        MutationJobError::CorruptProgressStore,
    );
}

#[test]
fn mutation_progress_replacement_is_exact_idempotent_and_fail_closed() {
    let before = current_mutation_record(21);
    let (after, _) = before
        .apply_transition(
            &mutation_request(21, 0, "atomic-forward"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![9],
                8,
                3,
                0,
            ),
        )
        .expect("bounded atomic successor should admit");
    let operation = MutationProgressRecordOp::replace(&before, &after)
        .expect("exact mutation progress replacement should admit");
    let mut store = InspectionProgressStore::open(test_memory(252))
        .expect("isolated progress store should open");
    assert!(matches!(
        store
            .insert_mutation(&before)
            .expect("before record should insert"),
        InsertMutationJobResult::Inserted,
    ));

    store
        .preflight_mutation_progress(&operation)
        .expect("exact before bytes should preflight");
    store
        .apply_mutation_progress(&operation)
        .expect("exact before bytes should advance");
    store
        .apply_mutation_progress(&operation)
        .expect("exact after bytes should replay idempotently");
    store
        .verify_mutation_progress(&operation)
        .expect("exact after bytes should verify");
    assert_eq!(
        store
            .load_mutation(before.state().job_id)
            .expect("advanced record should load"),
        after,
    );
    assert_eq!(
        store.preflight_mutation_progress(&operation),
        Err(MutationJobError::CorruptProgressStore),
        "opening a new marker against after-state must not reset progress",
    );

    let (unexpected, _) = after
        .apply_transition(
            &mutation_request(21, 1, "unexpected"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![10],
                1,
                0,
                0,
            ),
        )
        .expect("third valid state should admit");
    store
        .replace_mutation(&unexpected)
        .expect("test should install neither-side state");
    assert_eq!(
        store.apply_mutation_progress(&operation),
        Err(MutationJobError::CorruptProgressStore),
    );
    assert_eq!(
        store.verify_mutation_progress(&operation),
        Err(MutationJobError::CorruptProgressStore),
    );
}

#[test]
fn mutation_record_sizes_are_fixed_for_current_and_maximal_states() {
    let initial = current_mutation_record(8);
    let (active, _) = initial
        .apply_transition(
            &mutation_request(8, 0, "forward-0"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Verify,
                vec![6],
                13,
                4,
                0,
            ),
        )
        .expect("bounded active transition should admit");
    let (completed, _) = active
        .apply_transition(
            &mutation_request(8, 1, "verify-0"),
            MutationJobTransition::new(
                MutationJobStatus::Completed,
                MutationJobPhase::Verify,
                Vec::new(),
                9,
                0,
                0,
            ),
        )
        .expect("bounded completion should admit");
    let (restart, _) = initial
        .apply_transition(
            &mutation_request(8, 0, "restart"),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(MutationJobRestartReason::AcceptedSchemaChanged),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("bounded restart should admit");
    let maximal_initial = MutationJobRecord::new(
        mutation_job_id(9),
        vec![1; crate::db::MAX_MUTATION_JOB_INTENT_BYTES],
        vec![2; crate::db::MAX_MUTATION_JOB_CONTINUATION_BYTES],
    )
    .expect("maximum initial record should admit");
    let (maximal_active, _) = maximal_initial
        .apply_transition(
            &MutationJobAdvanceRequest::new(
                mutation_job_id(9),
                0,
                MutationJobIdempotencyKey::new(
                    "k".repeat(crate::db::MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES),
                )
                .expect("maximum replay key should admit"),
            ),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![2; crate::db::MAX_MUTATION_JOB_CONTINUATION_BYTES],
                crate::db::MAX_MUTATION_JOB_STEP_KEYS_SCANNED,
                crate::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
                0,
            ),
        )
        .expect("maximum active record should admit");

    assert_eq!(
        encode_mutation_job_record(&initial).map(|bytes| bytes.len()),
        Ok(97)
    );
    assert_eq!(
        encode_mutation_job_record(&active).map(|bytes| bytes.len()),
        Ok(167)
    );
    assert_eq!(
        encode_mutation_job_record(&completed).map(|bytes| bytes.len()),
        Ok(165),
    );
    assert_eq!(
        encode_mutation_job_record(&restart).map(|bytes| bytes.len()),
        Ok(166)
    );
    assert_eq!(
        encode_mutation_job_record(&maximal_initial).map(|bytes| bytes.len()),
        Ok(18_524),
    );
    assert_eq!(
        encode_mutation_job_record(&maximal_active).map(|bytes| bytes.len()),
        Ok(18_842),
    );
}

#[test]
fn mutation_key_domain_and_shared_capacity_reservation_are_enforced() {
    let shared_bytes = [11; 32];
    let mutation_key = ProgressRecordKey::from_mutation_job_id(
        MutationJobId::try_from_bytes(shared_bytes).expect("mutation id should admit"),
    )
    .expect("mutation progress key should derive");
    let resumable_key = ProgressRecordKey::from_resumable_job_id(
        ResumableJobId::try_from_bytes(shared_bytes).expect("resumable id should admit"),
    )
    .expect("resumable progress key should derive");
    let integrity_key = ProgressRecordKey::from_job_id(
        IntegrityJobId::try_from_bytes(shared_bytes).expect("integrity id should admit"),
    );
    assert_ne!(mutation_key, resumable_key);
    assert_ne!(mutation_key, integrity_key);

    let mut store = InspectionProgressStore::open(test_memory(251))
        .expect("isolated progress store should open");
    store
        .insert_resumable(&current_resumable_record())
        .expect("generic job should consume one shared slot");
    for byte in 1..=54 {
        assert!(matches!(
            store
                .insert_mutation(&current_mutation_record(byte))
                .expect("record inside shared capacity should insert"),
            InsertMutationJobResult::Inserted,
        ));
    }
    assert_eq!(
        store
            .inventory()
            .expect("55 current records should inventory")
            .retained_count,
        55,
    );
    assert!(matches!(
        store
            .insert_mutation(&current_mutation_record(55))
            .expect("the 56th non-integrity record should insert"),
        InsertMutationJobResult::Inserted,
    ));
    assert_eq!(
        store
            .inventory()
            .expect("56 current records should inventory")
            .retained_count,
        56,
    );
    assert!(matches!(
        store.insert_mutation(&current_mutation_record(56)),
        Err(MutationJobError::CapacityExceeded),
    ));

    for byte in 200..=206 {
        assert!(matches!(
            store
                .insert_new(&current_integrity_record(byte))
                .expect("reserved integrity record should insert"),
            InsertJobResult::Inserted,
        ));
    }
    assert_eq!(
        store
            .inventory()
            .expect("63 current records should inventory")
            .retained_count,
        63,
    );
    assert!(matches!(
        store
            .insert_new(&current_integrity_record(207))
            .expect("the 64th integrity record should insert"),
        InsertJobResult::Inserted,
    ));
    let full = store.inventory().expect("full store should inventory");
    assert_eq!(full.retained_count, 64);
    assert_eq!(full.hard_limit, 64);
    assert_eq!(full.reserved_integrity_headroom, 8);
    assert_eq!(full.integrity_count, 8);
    assert_eq!(full.resumable_count, 1);
    assert_eq!(full.mutation_count, 55);
    assert!(matches!(
        store.insert_new(&current_integrity_record(208)),
        Err(IntegrityJobError::CapacityExceeded),
    ));
}

#[test]
fn progress_reservation_survives_reopen_and_refuses_growth_at_capacity() {
    let memory = crate::testing::test_memory_owner(249);
    let mut store = InspectionProgressStore::open(memory.open()).unwrap();
    for byte in 1..=56 {
        assert!(matches!(
            store
                .insert_mutation(&current_mutation_record(byte))
                .unwrap(),
            InsertMutationJobResult::Inserted
        ));
    }
    for byte in 200..=207 {
        assert!(matches!(
            store.insert_new(&current_integrity_record(byte)).unwrap(),
            InsertJobResult::Inserted
        ));
    }
    let pages = memory.size();
    drop(store);
    let mut reopened = InspectionProgressStore::open(memory.open()).unwrap();
    let full = reopened.inventory().unwrap();
    assert_eq!(full.retained_count, 64);
    assert_eq!(full.integrity_count, 8);
    assert_eq!(full.mutation_count, 56);
    assert!(matches!(
        reopened.insert_mutation(&current_mutation_record(57)),
        Err(MutationJobError::CapacityExceeded)
    ));
    assert!(matches!(
        reopened.insert_new(&current_integrity_record(208)),
        Err(IntegrityJobError::CapacityExceeded)
    ));
    assert_eq!(reopened.inventory().unwrap(), full);
    assert_eq!(memory.size(), pages);
}

#[test]
fn mutation_store_load_replay_replace_and_acknowledge_are_exact() {
    let mut store = InspectionProgressStore::open(test_memory(250))
        .expect("isolated progress store should open");
    let initial = current_mutation_record(10);
    assert!(matches!(
        store
            .insert_mutation(&initial)
            .expect("initial mutation record should insert"),
        InsertMutationJobResult::Inserted,
    ));
    assert!(matches!(
        store
            .insert_mutation(&initial)
            .expect("duplicate identity should load retained record"),
        InsertMutationJobResult::Occupied(record) if *record == initial,
    ));
    assert_eq!(
        store.load_mutation(mutation_job_id(10)),
        Ok(initial.clone())
    );
    assert_eq!(
        store.acknowledge_mutation(mutation_job_id(10), 0),
        Err(MutationJobError::Active),
    );

    let request = mutation_request(10, 0, "restart");
    let (terminal, receipt) = initial
        .apply_transition(
            &request,
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(MutationJobRestartReason::BatchPolicyChanged),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("terminal transition should admit");
    store
        .replace_mutation(&terminal)
        .expect("terminal replacement should persist");
    assert_eq!(
        store.load_mutation(mutation_job_id(10)).and_then(|record| {
            let replay = record.exact_replay(&request)?;
            Ok(replay.cloned())
        }),
        Ok(Some(receipt)),
    );
    assert_eq!(
        store.acknowledge_mutation(mutation_job_id(10), 0),
        Err(MutationJobError::StaleSequence {
            expected: 0,
            actual: 1,
        }),
    );
    assert_eq!(store.acknowledge_mutation(mutation_job_id(10), 1), Ok(()));
    assert_eq!(store.acknowledge_mutation(mutation_job_id(10), 1), Ok(()));
    assert_eq!(
        store.load_mutation(mutation_job_id(10)),
        Err(MutationJobError::NotFound),
    );
}

#[cfg(feature = "sql")]
#[test]
fn mutation_cancellation_is_exact_zero_state_and_absent_idempotent() {
    let mut store = InspectionProgressStore::open(test_memory(248))
        .expect("isolated progress store should open");
    let initial = current_mutation_record(31);
    insert_mutation_record(&mut store, &initial);
    assert_eq!(
        store.cancel_unadvanced_mutation(mutation_job_id(31), 1, |_| Ok(())),
        Err(MutationJobError::StaleSequence {
            expected: 1,
            actual: 0,
        }),
    );
    assert_eq!(
        store.cancel_unadvanced_mutation(mutation_job_id(31), 0, |_| Ok(())),
        Ok(()),
    );
    assert_eq!(
        store.cancel_unadvanced_mutation(mutation_job_id(31), 0, |_| {
            Err(MutationJobError::CorruptProgressStore)
        }),
        Ok(()),
        "an absent retry must not invoke continuation validation",
    );

    let advanced_initial = current_mutation_record(32);
    let (advanced, _) = advanced_initial
        .apply_transition(
            &mutation_request(32, 0, "advanced"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![6],
                1,
                0,
                0,
            ),
        )
        .expect("advanced state should admit");
    insert_mutation_record(&mut store, &advanced);
    for expected_sequence in [0, 1] {
        assert_eq!(
            store.cancel_unadvanced_mutation(mutation_job_id(32), expected_sequence, |_| Ok(()),),
            Err(MutationJobError::StaleSequence {
                expected: 0,
                actual: 1,
            }),
        );
    }

    let terminal_initial = current_mutation_record(33);
    let (terminal, _) = terminal_initial
        .apply_transition(
            &mutation_request(33, 0, "terminal"),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(MutationJobRestartReason::BatchPolicyChanged),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("terminal state should admit");
    insert_mutation_record(&mut store, &terminal);
    assert_eq!(
        store.cancel_unadvanced_mutation(mutation_job_id(33), 1, |_| Ok(())),
        Err(MutationJobError::StaleSequence {
            expected: 0,
            actual: 1,
        }),
    );

    let malformed_continuation = current_mutation_record(34);
    insert_mutation_record(&mut store, &malformed_continuation);
    assert_eq!(
        store.cancel_unadvanced_mutation(mutation_job_id(34), 0, |_| {
            Err(MutationJobError::CorruptProgressStore)
        }),
        Err(MutationJobError::CorruptProgressStore),
    );
    assert_eq!(
        store.load_mutation(mutation_job_id(34)),
        Ok(malformed_continuation),
        "failed validation must retain the record",
    );
}

#[test]
fn admin_retirement_preserves_exact_evidence_and_recovers_shared_capacity() {
    let memory = crate::testing::test_memory_owner(242);
    let mut store = InspectionProgressStore::open(memory.open()).unwrap();
    for byte in 1..=56 {
        store
            .insert_mutation(&current_mutation_record(byte))
            .unwrap();
    }
    for byte in 200..=207 {
        store.insert_new(&current_integrity_record(byte)).unwrap();
    }
    let before = store.retirement_inventory().unwrap();
    assert_eq!(before.len(), 64);
    let key = ProgressRecordKey::from_job_id(current_integrity_record(200).id);
    let evidence = before
        .iter()
        .find(|entry| entry.record_key == key.0)
        .unwrap()
        .clone();
    assert!(matches!(
        store.insert_mutation(&current_mutation_record(57)),
        Err(MutationJobError::CapacityExceeded)
    ));
    let header = store.map.get(&PROGRESS_HEADER_KEY).unwrap().unwrap();
    let header_claim = ProgressRetirementRecord {
        record_key: PROGRESS_HEADER_KEY.0,
        record_digest: retirement_digest(PROGRESS_HEADER_KEY, &header.0),
        encoded_bytes: header.0.len() as u64,
    };
    assert_eq!(
        store.retire_exact(&header_claim),
        Err(MutationJobError::InvalidJobId)
    );
    store
        .map
        .insert(
            key,
            ProgressRecordBytes(b"corrupt retained payload".to_vec()),
        )
        .unwrap();
    assert_eq!(
        store.retire_exact(&evidence),
        Err(MutationJobError::IdentityConflict)
    );
    assert_eq!(store.retirement_inventory().unwrap().len(), 64);
    drop(store);
    let mut store = InspectionProgressStore::open(memory.open()).unwrap();
    let current = store.retirement_inventory().unwrap();
    let corrupt = current
        .iter()
        .find(|entry| entry.record_key == key.0)
        .unwrap();
    assert!(store.retire_exact(corrupt).unwrap());
    assert!(!store.retire_exact(corrupt).unwrap());
    // Return below the non-integrity reservation, then admit fresh work.
    for record in current
        .iter()
        .filter(|record| record.record_key != key.0)
        .take(8)
    {
        assert!(store.retire_exact(record).unwrap());
    }
    assert_eq!(store.retirement_inventory().unwrap().len(), 55);
    assert!(matches!(
        store.insert_mutation(&current_mutation_record(57)),
        Ok(InsertMutationJobResult::Inserted)
    ));
    assert_eq!(
        store.map.get(&PROGRESS_HEADER_KEY).unwrap().unwrap(),
        header
    );
}

#[test]
fn admin_retirement_uses_raw_keys_for_all_progress_families() {
    let mut store = InspectionProgressStore::open(test_memory(241)).unwrap();
    store.insert_new(&current_integrity_record(201)).unwrap();
    store.insert_resumable(&current_resumable_record()).unwrap();
    store.insert_mutation(&current_mutation_record(35)).unwrap();
    let records = store.retirement_inventory().unwrap();
    assert_eq!(records.len(), 3);
    for record in records {
        assert!(store.retire_exact(&record).unwrap());
    }
    assert_eq!(store.job_count().unwrap(), 0);
    assert_eq!(store.inventory().unwrap().retained_count, 0);
}

#[test]
fn progress_inventory_is_complete_family_bounded_and_fail_closed() {
    let mut store = InspectionProgressStore::open(test_memory(247))
        .expect("isolated progress store should open");
    let integrity = current_integrity_record(201);
    let resumable = current_resumable_record();
    let mutation = current_mutation_record(35);
    assert!(matches!(
        store
            .insert_new(&integrity)
            .expect("integrity record should insert"),
        InsertJobResult::Inserted,
    ));
    store
        .insert_resumable(&resumable)
        .expect("resumable record should insert");
    assert!(matches!(
        store
            .insert_mutation(&mutation)
            .expect("mutation record should insert"),
        InsertMutationJobResult::Inserted,
    ));

    let inventory = store.inventory().expect("valid records should inventory");
    assert_eq!(inventory.retained_count, 3);
    assert_eq!(inventory.hard_limit, 64);
    assert_eq!(inventory.reserved_integrity_headroom, 8);
    assert_eq!(inventory.integrity_count, 1);
    assert_eq!(inventory.resumable_count, 1);
    assert_eq!(inventory.mutation_count, 1);
    let expected_record_bytes = encode_job_record(&integrity)
        .expect("integrity record should encode")
        .len()
        .saturating_add(
            encode_resumable_job_record(&resumable)
                .expect("resumable record should encode")
                .len(),
        )
        .saturating_add(
            encode_mutation_job_record(&mutation)
                .expect("mutation record should encode")
                .len(),
        );
    assert_eq!(
        inventory.retained_record_bytes,
        u64::try_from(expected_record_bytes).expect("bounded records fit u64")
    );
    assert_eq!(inventory.records.len(), 3);
    assert!(inventory.records.iter().all(|record| {
        record.lifecycle == ProgressJobLifecycle::Active && record.sequence == Some(0)
    }));
    assert!(inventory.records.iter().any(|record| {
        record.family == ProgressJobFamily::Integrity && record.job_id == integrity.id.to_bytes()
    }));
    assert!(inventory.records.iter().any(|record| {
        record.family == ProgressJobFamily::Resumable
            && record.job_id == resumable.state().job_id.to_bytes()
    }));
    assert!(inventory.records.iter().any(|record| {
        record.family == ProgressJobFamily::Mutation
            && record.job_id == mutation.state().job_id.to_bytes()
    }));

    store
        .map
        .insert(
            ProgressRecordKey([202; 32]),
            ProgressRecordBytes(b"undecodable-retained-slot".to_vec()),
        )
        .expect("fixture storage operation should succeed");
    assert_eq!(
        store.inventory(),
        Err(MutationJobError::CorruptProgressStore),
        "one undecodable slot must fail the whole inventory",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one retention scenario checks corrupt keys, expiry and preserved receipts together"
)]
fn retention_passes_corrupt_keys_without_losing_expiration_or_receipts() {
    use crate::db::integrity::{
        DeepIntegrityPageStatus, IntegrityJobReceipt, IntegrityPendingTerminal,
        IntegrityReceiptReplayKey, IntegrityTerminalOutcome, run_next_integrity_retention_page,
    };

    with_progress_store::<RetentionCanister, _>(|store| {
        let mismatched = encode_job_record(&current_integrity_record(99)).unwrap();
        for byte in 1..=20 {
            store
                .map
                .insert(
                    ProgressRecordKey([byte; 32]),
                    ProgressRecordBytes(if byte % 2 == 0 {
                        mismatched.clone()
                    } else {
                        vec![0]
                    }),
                )
                .expect("fixture storage operation should succeed");
        }
        let first = store.scan_after(None, 16)?;
        assert_eq!(first.records_scanned, 16);
        assert_eq!(first.job_ids.len(), 16);
        assert_eq!(first.next_checkpoint.unwrap().to_bytes(), [16; 32]);
        assert!(!first.exhausted);
        for id in first.job_ids {
            assert_eq!(id.to_bytes()[0], id.to_bytes()[31]);
            assert!(matches!(
                store.load(id),
                Err(IntegrityJobError::CorruptProgressRecord
                    | IntegrityJobError::IncompatibleProgressFormat)
            ));
        }
        let mut expired = current_integrity_record(21);
        expired.lease_deadline_nanos = 1;
        store
            .map
            .insert(
                ProgressRecordKey::from_job_id(expired.id),
                ProgressRecordBytes(encode_job_record(&expired)?),
            )
            .expect("fixture storage operation should succeed");
        for (byte, acknowledged) in [(22, true), (23, false)] {
            let mut terminal = current_integrity_record(byte);
            terminal.lease_deadline_nanos = 1;
            terminal.state = IntegrityJobState::Terminal {
                outcome: IntegrityTerminalOutcome::DeepCompleteClean,
                receipt_acknowledged: acknowledged,
            };
            terminal.pages_completed = 1;
            terminal.last_receipt.replay_key = IntegrityReceiptReplayKey::Continue {
                acknowledged_sequence: 0,
            };
            let IntegrityJobReceipt::Page(page) = &mut terminal.last_receipt.receipt else {
                panic!("fixture should have a page");
            };
            page.status =
                DeepIntegrityPageStatus::Terminal(IntegrityTerminalOutcome::DeepCompleteClean);
            page.page_sequence = 1;
            page.pages_completed = 1;
            terminal.validate()?;
            store
                .map
                .insert(
                    ProgressRecordKey::from_job_id(terminal.id),
                    ProgressRecordBytes(encode_job_record(&terminal)?),
                )
                .expect("fixture storage operation should succeed");
        }
        Ok(())
    })
    .unwrap();

    run_next_integrity_retention_page::<RetentionCanister>();
    with_progress_store::<RetentionCanister, _>(|store| {
        assert_eq!(
            store.load(current_integrity_record(21).id)?.state,
            IntegrityJobState::InProgress
        );
        Ok(())
    })
    .unwrap();
    run_next_integrity_retention_page::<RetentionCanister>();
    with_progress_store::<RetentionCanister, _>(|store| {
        assert_eq!(
            store.load(current_integrity_record(21).id)?.state,
            IntegrityJobState::TerminalPending(IntegrityPendingTerminal::Expired),
        );
        assert_eq!(
            store.load(current_integrity_record(22).id),
            Err(IntegrityJobError::JobNotFound)
        );
        assert!(matches!(
            store.load(current_integrity_record(23).id)?.state,
            IntegrityJobState::Terminal {
                receipt_acknowledged: false,
                ..
            }
        ));
        assert_eq!(
            store.job_count()?,
            22,
            "corrupt records and outstanding receipts stay retained"
        );
        Ok(())
    })
    .unwrap();
    // Wrapping revisits the corrupt prefix without stalling or deleting it.
    run_next_integrity_retention_page::<RetentionCanister>();
}

#[test]
fn integrity_scan_skips_other_progress_record_families() {
    let mut store = InspectionProgressStore::open(test_memory(252))
        .expect("isolated progress store should open");
    let integrity = current_job_codec_fixture();
    assert!(matches!(
        store
            .insert_new(&integrity)
            .expect("integrity job should insert"),
        InsertJobResult::Inserted,
    ));
    store
        .insert_resumable(&current_resumable_record())
        .expect("generic resumable job should insert");
    assert!(matches!(
        store
            .insert_mutation(&current_mutation_record(12))
            .expect("mutation job should insert"),
        InsertMutationJobResult::Inserted,
    ));

    let page = store
        .scan_after(None, 8)
        .expect("integrity scan should ignore other record families");
    assert_eq!(page.job_ids, vec![integrity.id]);
    assert_eq!(page.records_scanned, 3);
    assert!(page.exhausted);

    let mut checkpoint = None;
    let mut scanned = 0;
    let mut ids = Vec::new();
    loop {
        let page = store.scan_after(checkpoint, 1).unwrap();
        assert_eq!(page.records_scanned, 1);
        assert!(page.next_checkpoint > checkpoint);
        scanned += page.records_scanned;
        ids.extend(page.job_ids);
        checkpoint = page.next_checkpoint;
        if page.exhausted {
            break;
        }
        assert!(scanned < 3);
    }
    assert_eq!(scanned, 3);
    assert_eq!(ids, vec![integrity.id]);
    assert!(matches!(
        store.scan_after(None, 0),
        Err(IntegrityJobError::CapacityExceeded)
    ));
}
