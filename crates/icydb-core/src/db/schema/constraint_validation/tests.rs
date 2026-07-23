use super::*;
use crate::{
    db::{
        data::DecodedDataStoreKey,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{AcceptedCheckExprV1, ConstraintActivationKind, ConstraintOrigin},
    },
    types::EntityTag,
};

fn activation(state: ConstraintActivationState) -> ConstraintActivationSnapshot {
    let id = ConstraintId::new(7).expect("test activation ID should be non-zero");
    ConstraintActivationSnapshot::new(
        id,
        "pending_policy".to_string(),
        ConstraintOrigin::Generated,
        ConstraintActivationKind::Check {
            expression: Box::new(AcceptedCheckExprV1::True),
        },
        state,
        AcceptedSchemaFingerprint::new([0xA5; 32]),
        11,
    )
}

fn raw_key(entity: EntityTag, value: u64) -> RawDataStoreKey {
    DecodedDataStoreKey::new_primary_key_value(
        entity,
        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(value)),
    )
    .to_raw()
    .expect("test key should encode")
}

#[test]
fn validation_job_round_trips_current_forward_identity() {
    let entity = EntityTag::new(41);
    let activation = activation(ConstraintActivationState::Validating);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Checked".to_string(), &activation, None)
            .expect("validating activation should start a job");
    job.checkpoint = Some(raw_key(entity, 7));
    job.rows_scanned = 3;

    let bytes = encode_constraint_validation_job(&job).expect("job should encode");
    let decoded = decode_constraint_validation_job(&bytes).expect("job should decode");

    assert_eq!(decoded, job);
}

#[test]
fn validation_job_rejects_wrong_phase_closure_and_stale_activation() {
    let entity = EntityTag::new(42);
    let validating = activation(ConstraintActivationState::Validating);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Checked".to_string(), &validating, None)
            .expect("validating activation should start a job");
    job.phase = ConstraintValidationPhase::Verify;
    assert!(encode_constraint_validation_job(&job).is_err());

    job.phase = ConstraintValidationPhase::Forward;
    let enforcing = activation(ConstraintActivationState::EnforcingNewWrites);
    assert!(job.validate(Some(&enforcing)).is_err());
}

#[test]
fn validation_job_rejects_cross_entity_checkpoint_and_unbounded_receipt() {
    let entity = EntityTag::new(43);
    let activation = activation(ConstraintActivationState::Validating);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Checked".to_string(), &activation, None)
            .expect("validating activation should start a job");
    job.checkpoint = Some(raw_key(EntityTag::new(44), 7));
    assert!(encode_constraint_validation_job(&job).is_err());

    job.checkpoint = None;
    job.findings_seen = 65;
    job.last_receipt = Some(ConstraintValidationReceipt::new(
        1,
        (0..65)
            .map(|value| ConstraintValidationFinding::new(raw_key(entity, value), Vec::new(), 1))
            .collect(),
    ));
    assert!(encode_constraint_validation_job(&job).is_err());
}

#[test]
fn validation_job_decode_rejects_noncurrent_profile_and_oversized_bytes() {
    let entity = EntityTag::new(46);
    let activation = activation(ConstraintActivationState::Validating);
    let job =
        ConstraintValidationJob::start(entity, "tests::Checked".to_string(), &activation, None)
            .expect("validating activation should start a job");
    let mut wire = ConstraintValidationJobWire::from_job(&job);
    wire.contract_profile = u32::from_be_bytes(*b"ICJZ");
    let bytes = Encode!(&wire).expect("noncurrent test wire should encode");
    let error = decode_constraint_validation_job(&bytes)
        .expect_err("noncurrent job profile must fail closed");
    assert_eq!(
        error.class,
        crate::error::ErrorClass::IncompatiblePersistedFormat
    );
    assert_eq!(error.origin, crate::error::ErrorOrigin::Serialize);

    assert!(
        decode_constraint_validation_job(&vec![0; MAX_CONSTRAINT_VALIDATION_JOB_BYTES + 1])
            .is_err(),
        "oversized job bytes must reject before decoding",
    );
}

#[test]
fn forward_and_verify_progress_preserve_receipt_and_revision_invariants() {
    let entity = EntityTag::new(45);
    let activation = activation(ConstraintActivationState::Validating);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Checked".to_string(), &activation, None)
            .expect("validating activation should start a job");
    let finding = ConstraintValidationFinding::new(raw_key(entity, 1), Vec::new(), 1);
    job.record_forward_page(
        Some(raw_key(entity, 1)),
        1,
        vec![finding],
        true,
        Some(vec![ConstraintStoreRevision::new(
            "tests::Store".to_string(),
            1,
        )]),
    )
    .expect("finding page should remain Forward");
    assert_eq!(job.phase(), ConstraintValidationPhase::Forward);
    let receipt = job
        .last_receipt()
        .cloned()
        .expect("finding page should be retained");
    assert!(!job.acknowledge_receipt(None));
    assert!(job.acknowledge_receipt(Some(receipt.page_sequence())));
    assert!(
        !job.acknowledge_receipt(Some(receipt.page_sequence())),
        "an acknowledgement without a retained receipt must not advance",
    );

    job.record_forward_page(
        Some(raw_key(entity, 2)),
        2,
        Vec::new(),
        true,
        Some(vec![ConstraintStoreRevision::new(
            "tests::Store".to_string(),
            3,
        )]),
    )
    .expect("clean Forward exhaustion should enter Verify");
    assert_eq!(job.phase(), ConstraintValidationPhase::Verify);
    assert_eq!(
        job.captured_store_revisions()
            .expect("Verify should retain one revision")[0]
            .revision(),
        3,
    );

    job.restart_forward(0, Vec::new())
        .expect("revision drift should restart Forward");
    assert_eq!(job.phase(), ConstraintValidationPhase::Forward);
    assert!(job.captured_store_revisions().is_none());
}
