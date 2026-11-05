use super::*;

fn job_id() -> MutationJobId {
    MutationJobId::try_from_bytes([7; 32]).expect("nonzero mutation job id should admit")
}

fn request(sequence: u64, key: &str) -> MutationJobAdvanceRequest {
    MutationJobAdvanceRequest::new(
        job_id(),
        sequence,
        MutationJobIdempotencyKey::new(key).expect("bounded replay key should admit"),
    )
}

fn initial_record() -> MutationJobRecord {
    MutationJobRecord::new(job_id(), vec![1, 2, 3], vec![4, 5])
        .expect("bounded mutation record should admit")
}

#[test]
fn identities_and_variable_components_enforce_current_bounds() {
    assert_eq!(
        MutationJobId::try_from_bytes([0; 32]),
        Err(MutationJobError::InvalidJobId),
    );
    assert_eq!(
        MutationJobIdempotencyKey::new(""),
        Err(MutationJobError::InvalidIdempotencyKey),
    );
    assert!(MutationJobIdempotencyKey::new("k".repeat(256)).is_ok());
    assert_eq!(
        MutationJobIdempotencyKey::new("k".repeat(257)),
        Err(MutationJobError::InvalidIdempotencyKey),
    );
    assert_eq!(
        MutationJobRecord::new(job_id(), vec![1], Vec::new()),
        Err(MutationJobError::CorruptProgressStore),
    );

    assert!(MutationJobRecord::new(job_id(), vec![1; 16 * 1024], vec![2; 2 * 1024]).is_ok());
    assert!(matches!(
        MutationJobRecord::new(job_id(), vec![1; 16 * 1024 + 1], Vec::new()),
        Err(MutationJobError::PayloadTooLarge {
            kind: MutationJobPayloadKind::Intent,
            ..
        }),
    ));
    assert!(matches!(
        MutationJobRecord::new(job_id(), vec![1], vec![2; 2 * 1024 + 1]),
        Err(MutationJobError::PayloadTooLarge {
            kind: MutationJobPayloadKind::Continuation,
            ..
        }),
    ));

    let maximum_key =
        MutationJobIdempotencyKey::new("k".repeat(MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES))
            .expect("maximum replay key should admit");
    let request = MutationJobAdvanceRequest::new(job_id(), 0, maximum_key);
    let (record, _) = initial_record()
        .apply_transition(
            &request,
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![7],
                1,
                0,
                0,
            ),
        )
        .expect("maximum replay identity should retain");
    assert_eq!(
        record
            .last_receipt
            .as_ref()
            .map(retained_receipt_encoded_len),
        Some(Ok(318)),
    );

    let maximum_key =
        MutationJobIdempotencyKey::new("k".repeat(MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES))
            .expect("maximum replay key should admit");
    let (restart, _) = initial_record()
        .apply_transition(
            &MutationJobAdvanceRequest::new(job_id(), 0, maximum_key),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(MutationJobRestartReason::BatchPolicyChanged),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("maximum restart receipt should retain");
    assert_eq!(
        restart
            .last_receipt
            .as_ref()
            .map(retained_receipt_encoded_len),
        Some(Ok(319)),
    );
}

#[test]
fn current_payload_round_trips_every_lifecycle() {
    let initial = initial_record();
    let (active, _) = initial
        .apply_transition(
            &request(0, "forward-0"),
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
            &request(1, "verify-0"),
            MutationJobTransition::new(
                MutationJobStatus::Completed,
                MutationJobPhase::Verify,
                Vec::new(),
                9,
                0,
                0,
            ),
        )
        .expect("clean terminal transition should admit");
    let (restart, _) = initial
        .apply_transition(
            &request(0, "restart"),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(
                    MutationJobRestartReason::ManagedTimestampRegression,
                ),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("typed restart transition should admit");
    let (oversized_candidate, _) = initial
        .apply_transition(
            &request(0, "candidate-exceeds-policy"),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(
                    MutationJobRestartReason::CandidateExceedsBatchPolicy,
                ),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("candidate policy restart should admit");
    let (execution_budget, _) = initial
        .apply_transition(
            &request(0, "execution-budget-policy"),
            MutationJobTransition::new(
                MutationJobStatus::RestartRequired(
                    MutationJobRestartReason::ExecutionBudgetPolicyExceeded,
                ),
                MutationJobPhase::Forward,
                Vec::new(),
                0,
                0,
                0,
            ),
        )
        .expect("execution budget policy restart should admit");

    for record in [
        initial,
        active,
        completed,
        restart,
        oversized_candidate,
        execution_budget,
    ] {
        let bytes =
            encode_mutation_job_payload(&record).expect("current mutation payload should encode");
        assert!(!bytes.starts_with(b"DIDL"));
        assert_eq!(
            decode_mutation_job_payload(&bytes).expect("current mutation payload should decode"),
            record,
        );
    }
}

#[test]
fn every_restart_reason_round_trips_in_progress_and_wire() {
    let reasons = [
        MutationJobRestartReason::AcceptedSchemaChanged,
        MutationJobRestartReason::TargetAllocationChanged,
        MutationJobRestartReason::IntentIneligible,
        MutationJobRestartReason::BatchPolicyChanged,
        MutationJobRestartReason::UnsupportedContinuation,
        MutationJobRestartReason::ManagedTimestampRegression,
        MutationJobRestartReason::CandidateExceedsBatchPolicy,
        MutationJobRestartReason::ExecutionBudgetPolicyExceeded,
        MutationJobRestartReason::TargetRowRejected,
    ];
    for (tag, reason) in (0_u8..).zip(reasons) {
        let status = MutationJobStatus::RestartRequired(reason);
        let mut bytes = Vec::new();
        write_status(&mut bytes, status);
        assert_eq!(bytes, [2, tag]);
        assert_eq!(read_status(&mut ByteReader::new(&bytes)), Ok(status));
        let wire = icydb_schema::wire::encode_one(status).unwrap();
        assert_eq!(icydb_schema::wire::decode_one(&wire), Ok(status));

        let (record, _) = initial_record()
            .apply_transition(
                &request(0, "terminal"),
                MutationJobTransition::new(status, MutationJobPhase::Forward, Vec::new(), 0, 0, 0),
            )
            .unwrap();
        let payload = encode_mutation_job_payload(&record).unwrap();
        assert_eq!(decode_mutation_job_payload(&payload), Ok(record));
    }
    for tag in 9..=u8::MAX {
        assert_eq!(
            read_status(&mut ByteReader::new(&[2, tag])),
            Err(MutationJobError::CorruptProgressStore),
        );
    }
}

#[test]
fn public_state_request_receipt_and_error_are_wire_compatible() {
    let state = initial_record().state().clone();
    let request = request(0, "wire-request");
    let receipt = MutationJobAdvanceReceipt {
        request_sequence: 0,
        committed_sequence: 1,
        status: MutationJobStatus::Active,
        phase: MutationJobPhase::Forward,
        keys_scanned: 8,
        rows_updated: 3,
        keys_scanned_total: 8,
        rows_updated_total: 3,
        verify_restarts_total: 0,
    };
    let error = MutationJobError::TargetMutationFailed(
        MutationJobTargetFailureReason::StagingByteBudgetExceeded,
    );

    let state_bytes = icydb_schema::wire::encode_one(&state).expect("mutation state should encode");
    let request_bytes =
        icydb_schema::wire::encode_one(&request).expect("mutation request should encode");
    let receipt_bytes =
        icydb_schema::wire::encode_one(&receipt).expect("mutation receipt should encode");
    let error_bytes = icydb_schema::wire::encode_one(&error).expect("mutation error should encode");
    assert_eq!(
        icydb_schema::wire::decode_one::<MutationJobState>(&state_bytes)
            .expect("mutation state should decode"),
        state,
    );
    assert_eq!(
        icydb_schema::wire::decode_one::<MutationJobAdvanceRequest>(&request_bytes)
            .expect("mutation request should decode"),
        request,
    );
    assert_eq!(
        icydb_schema::wire::decode_one::<MutationJobAdvanceReceipt>(&receipt_bytes)
            .expect("mutation receipt should decode"),
        receipt,
    );
    assert_eq!(
        icydb_schema::wire::decode_one::<MutationJobError>(&error_bytes)
            .expect("mutation error should decode"),
        error,
    );
}

#[test]
fn exact_replay_precedes_stale_and_terminal_rejection() {
    let initial = initial_record();
    let (verifying, _) = initial
        .apply_transition(
            &request(0, "forward-0"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Verify,
                vec![7],
                8,
                3,
                0,
            ),
        )
        .expect("Forward exhaustion should enter Verify");
    let terminal_request = request(1, "verify-0");
    let (completed, receipt) = verifying
        .apply_transition(
            &terminal_request,
            MutationJobTransition::new(
                MutationJobStatus::Completed,
                MutationJobPhase::Verify,
                Vec::new(),
                8,
                0,
                0,
            ),
        )
        .expect("terminal transition should admit");

    assert_eq!(
        completed
            .exact_replay(&terminal_request)
            .expect("exact replay lookup should succeed"),
        Some(&receipt),
    );
    assert_eq!(
        completed.ensure_can_advance(&request(1, "different")),
        Err(MutationJobError::StaleSequence {
            expected: 1,
            actual: 2,
        }),
    );
    assert_eq!(
        completed.ensure_can_advance(&request(2, "next")),
        Err(MutationJobError::Completed),
    );
}

#[test]
fn payload_decode_is_bounded_fallible_and_rejects_trailing_bytes() {
    let bytes = encode_mutation_job_payload(&initial_record())
        .expect("current mutation payload should encode");
    for end in 0..bytes.len() {
        assert_eq!(
            decode_mutation_job_payload(&bytes[..end]),
            Err(MutationJobError::CorruptProgressStore),
            "truncation at byte {end} must retain corruption classification",
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        decode_mutation_job_payload(&trailing),
        Err(MutationJobError::CorruptProgressStore),
    );
    let mut unknown_status = encode_mutation_job_payload(&initial_record())
        .expect("current mutation payload should encode");
    unknown_status[32 + 8] = u8::MAX;
    assert_eq!(
        decode_mutation_job_payload(&unknown_status),
        Err(MutationJobError::CorruptProgressStore),
    );
    let mut zero_job_id = encode_mutation_job_payload(&initial_record())
        .expect("current mutation payload should encode");
    zero_job_id[..32].fill(0);
    assert_eq!(
        decode_mutation_job_payload(&zero_job_id),
        Err(MutationJobError::CorruptProgressStore),
    );

    let initial = initial_record();
    let mut bytes =
        encode_mutation_job_payload(&initial).expect("current mutation payload should encode");
    let intent_len_offset = 32 + 8 + 1 + 1 + 3 * 8;
    let continuation_len_offset = intent_len_offset + 4 + initial.canonical_intent.len();
    let continuation_offset = continuation_len_offset + 4;
    let continuation_end = continuation_offset + initial.engine_continuation.len();
    bytes[continuation_len_offset..continuation_offset].fill(0);
    bytes.drain(continuation_offset..continuation_end);
    assert_eq!(
        decode_mutation_job_payload(&bytes),
        Err(MutationJobError::CorruptProgressStore),
    );
}

#[test]
fn transition_totals_fail_closed_on_overflow() {
    assert_eq!(
        initial_record().apply_transition(
            &request(0, "empty-active-continuation"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                Vec::new(),
                1,
                0,
                0,
            ),
        ),
        Err(MutationJobError::CorruptProgressStore),
    );

    let mut record = initial_record();
    record.state.keys_scanned_total = u64::MAX;
    record.state.rows_updated_total = u64::MAX;
    record.last_receipt = Some(RetainedMutationJobReceipt {
        receipt: MutationJobAdvanceReceipt {
            request_sequence: 0,
            committed_sequence: 1,
            status: MutationJobStatus::Active,
            phase: MutationJobPhase::Forward,
            keys_scanned: 1,
            rows_updated: 1,
            keys_scanned_total: u64::MAX,
            rows_updated_total: u64::MAX,
            verify_restarts_total: 0,
        },
        idempotency_key: MutationJobIdempotencyKey::new("prior")
            .expect("bounded replay key should admit"),
    });
    record.state.sequence = 1;
    assert_eq!(
        record.apply_transition(
            &request(1, "overflow"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![7],
                1,
                1,
                0,
            ),
        ),
        Err(MutationJobError::CounterOverflow),
    );

    let mut sequence_record = initial_record();
    sequence_record.state.sequence = u64::MAX;
    sequence_record.last_receipt = Some(RetainedMutationJobReceipt {
        receipt: MutationJobAdvanceReceipt {
            request_sequence: u64::MAX - 1,
            committed_sequence: u64::MAX,
            status: MutationJobStatus::Active,
            phase: MutationJobPhase::Forward,
            keys_scanned: 0,
            rows_updated: 0,
            keys_scanned_total: 0,
            rows_updated_total: 0,
            verify_restarts_total: 0,
        },
        idempotency_key: MutationJobIdempotencyKey::new("prior-sequence")
            .expect("bounded replay key should admit"),
    });
    assert_eq!(
        sequence_record.apply_transition(
            &request(u64::MAX, "sequence-overflow"),
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Forward,
                vec![7],
                0,
                0,
                0,
            ),
        ),
        Err(MutationJobError::CounterOverflow),
    );
}
