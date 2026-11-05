use super::*;

#[test]
fn cancellation_accepts_only_current_initial_continuation_shape() {
    let initial = MutationJobEngineContinuation::initial(
        Ulid::from_bytes([1; 16]),
        7,
        [2; 32],
        1,
        [3; 16],
        [4; 32],
        [5; 32],
        Timestamp::from_millis(6),
    )
    .expect("current initial continuation should encode");
    assert_eq!(
        validate_current_initial_mutation_job_continuation(&initial.bytes),
        Ok(()),
    );

    let mut verify = DecodedMutationJobEngineContinuation::decode(&initial.bytes)
        .expect("current initial continuation should decode");
    verify.phase = MutationJobEnginePhase::Verify;
    verify.retained_verify_revision = Some(9);
    let verify = verify.encode().expect("Verify continuation should encode");
    assert_eq!(
        validate_current_initial_mutation_job_continuation(&verify.bytes),
        Err(MutationJobError::CorruptProgressStore),
    );

    let mut corrupt = initial.bytes;
    corrupt[0] ^= 0xff;
    assert_eq!(
        validate_current_initial_mutation_job_continuation(&corrupt),
        Err(MutationJobError::CorruptProgressStore),
    );
}

#[test]
fn resumable_batch_policy_identity_covers_every_current_policy_input() {
    assert_eq!(
        MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES,
        crate::db::MAX_MUTATION_JOB_CONTINUATION_BYTES,
    );
    assert_eq!(
        MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED,
        usize::try_from(crate::db::MAX_MUTATION_JOB_STEP_KEYS_SCANNED)
            .expect("scanned-key limit should fit usize"),
    );
    assert_eq!(
        MAX_RESUMABLE_UPDATE_FORWARD_ROWS,
        usize::try_from(crate::db::MAX_MUTATION_JOB_STEP_ROWS_UPDATED)
            .expect("updated-row limit should fit usize"),
    );
    assert_eq!(MAX_RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED, 4_096);
    assert_eq!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 0x3a31_c25a);
    assert_ne!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 1);

    for index in 0..RESUMABLE_UPDATE_BATCH_POLICY_INPUTS.len() {
        let mut changed = RESUMABLE_UPDATE_BATCH_POLICY_INPUTS;
        changed[index] = changed[index].wrapping_add(1);
        assert_ne!(
            resumable_update_batch_policy_identity(changed),
            RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
            "current policy input {index} must participate in the batch-policy identity",
        );
    }
}

#[test]
fn target_execution_failures_use_only_bounded_public_reasons() {
    let staging = InternalError::mutation_batch_staged_bytes_exceeded(Some(101), 100);
    assert!(matches!(
        classify_mutation_job_target_execution_failure(&staging),
        MutationJobTargetExecutionFailure::Target(
            MutationJobTargetFailureReason::StagingByteBudgetExceeded
        )
    ));

    let budget = InternalError::execution_budget_exceeded(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::RowsVisited,
        100,
        101,
        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Execution,
        icydb_diagnostic_code::DiagnosticExecutionLane::Mutation,
        7,
    );
    assert!(matches!(
        classify_mutation_job_target_execution_failure(&budget),
        MutationJobTargetExecutionFailure::Restart(
            MutationJobRestartReason::ExecutionBudgetPolicyExceeded
        )
    ));

    for kind in [
        icydb_diagnostic_code::DiagnosticConstraintKind::Check,
        icydb_diagnostic_code::DiagnosticConstraintKind::NotNull,
        icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule,
    ] {
        let value_path = (kind == icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule)
            .then(|| {
                crate::error::ConstraintValuePath::new(vec![
                    crate::error::ConstraintValuePathComponent::RootField { field_id: 1 },
                ])
            });
        let constraint = InternalError::mutation_constraint_violation(
            crate::error::AcceptedConstraintFactContext::write_admission(
                1, [7; 16], 1, 1, kind, None, value_path,
            ),
        );
        assert!(matches!(
            classify_mutation_job_target_execution_failure(&constraint),
            MutationJobTargetExecutionFailure::Restart(MutationJobRestartReason::TargetRowRejected)
        ));
        assert_eq!(
            mutation_job_execution_budget_restart_reason(&constraint),
            None
        );
    }

    for other in [
        InternalError::executor_invariant(),
        InternalError::accepted_row_constraint_program_corrupt(),
    ] {
        assert!(matches!(
            classify_mutation_job_target_execution_failure(&other),
            MutationJobTargetExecutionFailure::Target(MutationJobTargetFailureReason::Other)
        ));
    }
}

#[test]
fn intermediate_verify_checkpoint_retains_the_original_revision_baseline() {
    use crate::db::key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue};

    let initial = MutationJobEngineContinuation::initial(
        Ulid::from_bytes([1; 16]),
        7,
        [2; 32],
        1,
        [3; 16],
        [4; 32],
        [5; 32],
        Timestamp::from_millis(6),
    )
    .expect("current initial continuation should encode");
    let mut verify = DecodedMutationJobEngineContinuation::decode(&initial.bytes)
        .expect("current initial continuation should decode");
    verify.phase = MutationJobEnginePhase::Verify;
    verify.retained_verify_revision = Some(91);
    verify.checkpoint = Some(
        DecodedDataStoreKey::new_primary_key_value(
            EntityTag::new(7),
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(208)),
        )
        .to_raw()
        .expect("Verify checkpoint should encode"),
    );

    let retained = DecodedMutationJobEngineContinuation::decode(
        &verify
            .encode()
            .expect("intermediate Verify continuation should encode")
            .bytes,
    )
    .expect("intermediate Verify continuation should decode");
    assert_eq!(retained.retained_verify_revision, Some(91));
    assert!(retained.checkpoint.is_some());
}

#[test]
fn verify_revision_comparison_detects_drift_before_or_during_a_scan() {
    assert!(!mutation_verify_revision_changed(91, 91));
    assert!(mutation_verify_revision_changed(91, 92));
    assert!(mutation_verify_revision_changed(91, 90));
}

#[test]
fn scan_and_staging_limits_admit_one_maximum_valid_row() {
    let maximum_scan_charge =
        RawDataStoreKey::MAX_STORED_SIZE_USIZE + crate::db::codec::MAX_ROW_BYTES as usize;
    assert!(maximum_scan_charge <= MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES);
    assert!(maximum_scan_charge <= MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES);

    let maximum_staged_charge =
        RawDataStoreKey::MAX_STORED_SIZE_USIZE + (2 * crate::db::codec::MAX_ROW_BYTES as usize);
    assert!(
        maximum_staged_charge
            <= crate::db::executor::mutation::structural::MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES
    );

    let maximum_row_buffer_peak =
        crate::db::executor::mutation::structural::MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES
            + maximum_scan_charge
            + crate::db::codec::MAX_ROW_BYTES as usize;
    assert!(maximum_row_buffer_peak < 32 * 1024 * 1024);
}

#[test]
fn scan_byte_formula_accepts_exact_limit_and_stops_before_max_plus_one() {
    assert_eq!(
        resumable_scan_bytes_after_row(
            0,
            RawDataStoreKey::MAX_STORED_SIZE_USIZE,
            MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES - RawDataStoreKey::MAX_STORED_SIZE_USIZE,
            MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES,
        ),
        Some(MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES),
    );
    assert_eq!(
        resumable_scan_bytes_after_row(
            MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
            0,
            0,
            MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
        ),
        Some(MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES),
    );
    assert_eq!(
        resumable_scan_bytes_after_row(
            MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
            0,
            1,
            MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
        ),
        None,
    );
}

#[test]
fn retained_continuation_distinguishes_unsupported_format_from_corruption() {
    let mut unsupported_version = RESUMABLE_UPDATE_CONTINUATION_MAGIC.to_vec();
    unsupported_version.push(RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION.saturating_add(1));
    unsupported_version.extend_from_slice(&crc32c(&unsupported_version).to_be_bytes());
    assert!(matches!(
        decode_retained_mutation_job_continuation(&unsupported_version),
        Err(MutationJobExecutionPreparationError::Restart(
            MutationJobRestartReason::UnsupportedContinuation
        ))
    ));

    let malformed_current = [
        RESUMABLE_UPDATE_CONTINUATION_MAGIC[0],
        RESUMABLE_UPDATE_CONTINUATION_MAGIC[1],
        RESUMABLE_UPDATE_CONTINUATION_MAGIC[2],
        RESUMABLE_UPDATE_CONTINUATION_MAGIC[3],
        RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION,
    ];
    assert!(matches!(
        decode_retained_mutation_job_continuation(&malformed_current),
        Err(MutationJobExecutionPreparationError::Failure(
            MutationJobError::CorruptProgressStore
        ))
    ));

    let unknown_magic = [
        b'O',
        b'L',
        b'D',
        b'U',
        RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION,
    ];
    assert!(matches!(
        decode_retained_mutation_job_continuation(&unknown_magic),
        Err(MutationJobExecutionPreparationError::Failure(
            MutationJobError::CorruptProgressStore
        ))
    ));
}
