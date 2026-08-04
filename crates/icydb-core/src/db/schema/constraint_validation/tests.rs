use super::*;
use crate::{
    db::{
        data::DecodedDataStoreKey,
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedCheckExprV1, AcceptedNamedTypeIdentity, AcceptedRuleOperation,
            AcceptedRuleTarget, AcceptedTargetPath, AcceptedTargetPathComponent,
            ConstraintActivationKind, ConstraintOrigin, MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
            composite_catalog::{CompositeFieldId, CompositeTypeId},
            enum_catalog::{EnumTypeId, EnumVariantId},
        },
    },
    types::EntityTag,
};

fn rewrite_current_job_bytes(bytes: &[u8], rewrite: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let body_len = bytes
        .len()
        .checked_sub(CONSTRAINT_VALIDATION_JOB_CHECKSUM_BYTES)
        .expect("current job bytes should include a checksum");
    let mut body = bytes[..body_len].to_vec();
    rewrite(&mut body);
    let checksum = crc32c(&body).to_be_bytes();
    body.extend_from_slice(&checksum);
    body
}

fn golden_job() -> ConstraintValidationJob {
    ConstraintValidationJob {
        entity_tag: EntityTag::new(1),
        entity_path: "x".to_string(),
        constraint_id: ConstraintId::new(1).expect("golden constraint ID should be non-zero"),
        activation_epoch: 1,
        activation_fingerprint: ConstraintActivationFingerprint::new([0x11; 32]),
        base_schema_fingerprint: AcceptedSchemaFingerprint::new([0x22; 32]),
        phase: ConstraintValidationPhase::Forward,
        checkpoint: None,
        captured_store_revisions: None,
        staged_generation: None,
        rows_scanned: 0,
        findings_seen: 0,
        restarts: 0,
        forward_findings: 0,
        receipt_sequence: 0,
        last_receipt: None,
    }
}

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

fn targeted_activation(root_field_id: FieldId) -> ConstraintActivationSnapshot {
    let id = ConstraintId::new(8).expect("test activation ID should be non-zero");
    ConstraintActivationSnapshot::new(
        id,
        "pending_targeted_policy".to_string(),
        ConstraintOrigin::Generated,
        ConstraintActivationKind::TargetedRule {
            target: AcceptedRuleTarget::new(
                root_field_id,
                AcceptedNamedTypeIdentity::Composite(
                    CompositeTypeId::new(3).expect("test composite ID should be non-zero"),
                ),
            ),
            operation: Box::new(AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 8 }),
        },
        ConstraintActivationState::Validating,
        AcceptedSchemaFingerprint::new([0xB6; 32]),
        12,
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
fn historical_finding_output_retains_only_exact_numeric_repair_authority() {
    let entity = EntityTag::new(41);
    let activation = targeted_activation(FieldId::new(5));
    let finding = ConstraintValidationFinding::new_targeted(
        raw_key(entity, 9),
        vec![FieldId::new(5), FieldId::new(7)],
        AcceptedTargetPath::new(vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(5)),
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: CompositeTypeId::new(3)
                    .expect("test composite identity should be non-zero"),
                member_id: CompositeFieldId::new(2)
                    .expect("test member identity should be non-zero"),
            },
        ]),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION.raw(),
    );

    let output = crate::db::schema::constraint_validation_finding_output(
        [0xCC; 16],
        entity,
        &activation,
        &finding,
    )
    .expect("bounded historical finding should project");

    assert_eq!(output.accepted_schema_fingerprint(), [0xCC; 16]);
    assert_eq!(output.entity_tag(), 41);
    assert_eq!(output.constraint_id(), activation.id().get());
    assert!(!output.primary_key().is_empty());
    assert_eq!(output.field_ids(), &[5, 7]);
    assert_eq!(
        output
            .value_path()
            .expect("targeted finding should retain its bounded repair path")
            .components(),
        &[
            crate::error::ConstraintValuePathComponent::RootField { field_id: 5 },
            crate::error::ConstraintValuePathComponent::RecordMember {
                composite_type_id: 3,
                member_id: 2,
            },
        ],
    );
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
    assert_eq!(
        encode_constraint_validation_job(&job)
            .expect("repeat encoding should remain deterministic"),
        bytes,
    );
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
fn validation_job_v1_golden_bytes_remain_stable() {
    let bytes = encode_constraint_validation_job(&golden_job()).expect("golden job should encode");
    let mut expected = CONSTRAINT_VALIDATION_JOB_MAGIC.to_vec();
    expected.push(CONSTRAINT_VALIDATION_JOB_CODEC_VERSION);
    expected.extend_from_slice(&1_u64.to_be_bytes());
    expected.extend_from_slice(&1_u32.to_be_bytes());
    expected.push(b'x');
    expected.extend_from_slice(&1_u32.to_be_bytes());
    expected.extend_from_slice(&1_u64.to_be_bytes());
    expected.extend_from_slice(&[0x11; 32]);
    expected.extend_from_slice(&[0x22; 32]);
    expected.extend_from_slice(&[1, 0, 0, 0]);
    expected.extend_from_slice(&[0; 5 * size_of::<u64>()]);
    expected.push(0);
    expected.extend_from_slice(&[48, 56, 155, 49]);

    assert_eq!(bytes, expected);
}

#[test]
fn validation_job_decode_rejects_noncurrent_version_and_oversized_bytes() {
    let bytes = encode_constraint_validation_job(&golden_job()).expect("job should encode");
    let noncurrent = rewrite_current_job_bytes(&bytes, |body| {
        body[CONSTRAINT_VALIDATION_JOB_MAGIC.len()] = 2;
    });
    let error = decode_constraint_validation_job(&noncurrent)
        .expect_err("noncurrent job version must fail closed");
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
fn validation_job_decode_rejects_truncation_corruption_and_trailing_bytes() {
    let bytes = encode_constraint_validation_job(&golden_job()).expect("job should encode");
    for len in 0..bytes.len() {
        assert!(
            decode_constraint_validation_job(&bytes[..len]).is_err(),
            "truncated current bytes at {len} must fail closed",
        );
    }

    let mut corrupt = bytes.clone();
    corrupt[9] ^= 1;
    assert!(decode_constraint_validation_job(&corrupt).is_err());

    let bad_magic = rewrite_current_job_bytes(&bytes, |body| body[0] ^= 1);
    assert!(decode_constraint_validation_job(&bad_magic).is_err());

    let trailing = rewrite_current_job_bytes(&bytes, |body| body.push(0));
    assert!(decode_constraint_validation_job(&trailing).is_err());
}

#[test]
fn validation_job_decode_rejects_bounded_lengths_and_unknown_tags() {
    let bytes = encode_constraint_validation_job(&golden_job()).expect("job should encode");
    let oversized_entity_path = rewrite_current_job_bytes(&bytes, |body| {
        let path_len_offset = CONSTRAINT_VALIDATION_JOB_MAGIC.len() + 1 + size_of::<u64>();
        body[path_len_offset..path_len_offset + size_of::<u32>()].copy_from_slice(
            &u32::try_from(MAX_CONSTRAINT_VALIDATION_ENTITY_PATH_BYTES + 1)
                .expect("test path bound should fit u32")
                .to_be_bytes(),
        );
    });
    assert!(decode_constraint_validation_job(&oversized_entity_path).is_err());

    let unknown_phase = rewrite_current_job_bytes(&bytes, |body| {
        let phase_offset = CONSTRAINT_VALIDATION_JOB_MAGIC.len()
            + 1
            + size_of::<u64>()
            + size_of::<u32>()
            + golden_job().entity_path().len()
            + size_of::<u32>()
            + size_of::<u64>()
            + 32
            + 32;
        body[phase_offset] = u8::MAX;
    });
    assert!(decode_constraint_validation_job(&unknown_phase).is_err());

    let mut reader = ConstraintValidationJobReader::new(&[u8::MAX]);
    assert!(decode_path_component(&mut reader).is_err());
}

#[test]
fn validation_job_path_component_tags_round_trip_exhaustively() {
    let composite_type_id = CompositeTypeId::new(3).expect("test composite ID should be non-zero");
    let components = [
        AcceptedTargetPathComponent::RootField(FieldId::new(1)),
        AcceptedTargetPathComponent::RecordMember {
            composite_type_id,
            member_id: CompositeFieldId::new(4).expect("test member ID should be non-zero"),
        },
        AcceptedTargetPathComponent::TupleElement {
            composite_type_id,
            ordinal: 5,
        },
        AcceptedTargetPathComponent::Newtype { composite_type_id },
        AcceptedTargetPathComponent::EnumVariant {
            enum_type_id: EnumTypeId::new(6).expect("test enum ID should be non-zero"),
            variant_id: EnumVariantId::new(7).expect("test variant ID should be non-zero"),
        },
        AcceptedTargetPathComponent::ListElement { index: 8 },
        AcceptedTargetPathComponent::SetElement { index: 9 },
        AcceptedTargetPathComponent::MapEntryKey { index: 10 },
        AcceptedTargetPathComponent::MapEntryValue { index: 11 },
    ];

    for (index, component) in components.iter().enumerate() {
        let mut writer = ConstraintValidationJobWriter::new();
        encode_path_component(&mut writer, component);
        let bytes = writer.finish().expect("path component should encode");
        assert_eq!(bytes[0], u8::try_from(index + 1).expect("tag should fit"));
        let mut reader = ConstraintValidationJobReader::new(&bytes);
        assert_eq!(
            decode_path_component(&mut reader).expect("path component should decode"),
            *component,
        );
        reader.finish().expect("component should consume all bytes");
    }
}

#[test]
fn targeted_finding_path_round_trips_and_remains_activation_bound() {
    let entity = EntityTag::new(47);
    let root_field_id = FieldId::new(2);
    let activation = targeted_activation(root_field_id);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Targeted".to_string(), &activation, None)
            .expect("targeted activation should start a job");
    let path = AcceptedTargetPath::new(vec![
        AcceptedTargetPathComponent::RootField(root_field_id),
        AcceptedTargetPathComponent::Newtype {
            composite_type_id: CompositeTypeId::new(3)
                .expect("test composite ID should be non-zero"),
        },
    ]);
    job.record_forward_page(
        Some(raw_key(entity, 1)),
        1,
        vec![ConstraintValidationFinding::new_targeted(
            raw_key(entity, 1),
            vec![root_field_id],
            path.clone(),
            1,
        )],
        true,
        Some(vec![ConstraintStoreRevision::new(
            "tests::Store".to_string(),
            1,
        )]),
    )
    .expect("targeted finding page should retain its evidence");

    let bytes = encode_constraint_validation_job(&job).expect("targeted job should encode");
    let decoded = decode_constraint_validation_job(&bytes).expect("targeted job should decode");
    decoded
        .validate(Some(&activation))
        .expect("decoded path should remain bound to its activation");
    assert_eq!(
        decoded
            .last_receipt()
            .expect("finding receipt should remain")
            .findings()[0]
            .value_path(),
        Some(&path),
    );

    let wrong_root = targeted_activation(FieldId::new(9));
    assert!(
        decoded.validate(Some(&wrong_root)).is_err(),
        "recovery must reject finding evidence bound to another root",
    );
}

#[test]
fn targeted_finding_path_rejects_unbounded_durable_evidence() {
    let entity = EntityTag::new(48);
    let root_field_id = FieldId::new(2);
    let activation = targeted_activation(root_field_id);
    let mut job =
        ConstraintValidationJob::start(entity, "tests::Targeted".to_string(), &activation, None)
            .expect("targeted activation should start a job");
    let path = AcceptedTargetPath::new(
        (0..=MAX_ACCEPTED_TARGET_PATH_COMPONENTS)
            .map(|_| AcceptedTargetPathComponent::RootField(root_field_id))
            .collect(),
    );

    assert!(
        job.record_forward_page(
            Some(raw_key(entity, 1)),
            1,
            vec![ConstraintValidationFinding::new_targeted(
                raw_key(entity, 1),
                vec![root_field_id],
                path,
                1,
            )],
            false,
            None,
        )
        .is_err(),
        "unbounded concrete paths must reject before durable publication",
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
    let verify_bytes =
        encode_constraint_validation_job(&job).expect("Verify progress should encode");
    assert_eq!(
        decode_constraint_validation_job(&verify_bytes).expect("Verify progress should decode"),
        job,
    );

    job.restart_forward(0, Vec::new())
        .expect("revision drift should restart Forward");
    assert_eq!(job.phase(), ConstraintValidationPhase::Forward);
    assert!(job.captured_store_revisions().is_none());
}
