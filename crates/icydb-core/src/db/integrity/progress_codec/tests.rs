use super::*;
use crate::error::{
    ConstraintValuePath, ConstraintValuePathComponent, InternalError as RuntimeError,
};
use sha2::{Digest, Sha256};

#[test]
fn current_job_payload_rejects_malformed_wire_and_invalid_authority() {
    let job = current_job_codec_fixture();
    let mut alternate = encode_integrity_job_payload(&job).unwrap();
    // The first string follows the two fixed-width identity arrays.
    // Noncanonical length encodings must fail at the persisted boundary.
    assert_eq!(alternate[48], 13);
    alternate.splice(48..49, [0x8d, 0]);
    assert_eq!(decode_integrity_job_payload(&alternate), Err(InternalError));

    let mut invalid = job.clone();
    invalid.accepted_schema_fingerprint[0] ^= 1;
    let raw = ic_wire::native::encode(&invalid, limits()).unwrap();
    assert_eq!(decode_integrity_job_payload(&raw), Err(InternalError));
    let mut invalid = job;
    invalid.pages_completed = 1;
    let raw = ic_wire::native::encode(&invalid, limits()).unwrap();
    assert_eq!(decode_integrity_job_payload(&raw), Err(InternalError));
}

#[test]
fn current_job_payload_preserves_field_specific_allocation_bounds() {
    let job = current_job_codec_fixture();
    let mut wide = job.clone();
    wide.captured_proof_vector.stores[0].store_path = "s".repeat(MAX_INTEGRITY_PATH_BYTES + 1);
    let mut excessive = job;
    let store = excessive.captured_proof_vector.stores[0].clone();
    excessive.captured_proof_vector.stores = (0..=MAX_PROOF_STORES)
        .map(|index| {
            let mut store = store.clone();
            store.store_path = format!("store::{index:03}");
            store
        })
        .collect();
    for invalid in [wide, excessive] {
        assert_eq!(encode_integrity_job_payload(&invalid), Err(InternalError));
        let raw = ic_wire::native::encode(&invalid, limits()).unwrap();
        assert_eq!(decode_integrity_job_payload(&raw), Err(InternalError));
    }
}

#[test]
fn current_job_payload_is_direct_bounded_and_has_a_fixed_golden_vector() {
    let job = current_job_codec_fixture();
    let payload = encode_integrity_job_payload(&job).expect("current payload should encode");

    assert_eq!(
        decode_integrity_job_payload(&payload).expect("current payload should decode"),
        job,
    );
    assert_eq!(payload.len(), 332);
    assert_eq!(
        Sha256::digest(&payload).as_slice(),
        [
            222, 35, 143, 11, 103, 162, 33, 100, 19, 68, 153, 128, 156, 33, 54, 209, 253, 189, 150,
            165, 125, 203, 239, 224, 47, 235, 72, 223, 187, 3, 10, 23,
        ],
    );
}

#[test]
fn current_job_payload_rejects_trailing_truncated_and_reserved_identity_bytes() {
    let payload = encode_integrity_job_payload(&current_job_codec_fixture())
        .expect("current payload should encode");

    let mut trailing = payload.clone();
    trailing.push(0);
    assert!(decode_integrity_job_payload(&trailing).is_err());
    for end in 0..payload.len() {
        assert!(decode_integrity_job_payload(&payload[..end]).is_err());
    }

    let mut reserved_identity = payload;
    reserved_identity[..32].fill(0);
    assert!(decode_integrity_job_payload(&reserved_identity).is_err());
}

#[test]
fn current_job_payload_rejects_oversized_input_before_decoding() {
    let oversized = vec![0; MAX_INTEGRITY_JOB_PAYLOAD_BYTES + 1];
    assert!(decode_integrity_job_payload(&oversized).is_err());
}

#[test]
fn current_job_payload_preserves_resource_terminal_and_acknowledgement() {
    let mut job = current_job_codec_fixture();
    let error = RuntimeError::relation_budget_exceeded(
        icydb_diagnostic_code::DiagnosticExecutionBudgetResource::NestedValueSteps,
        10,
        11,
    );
    let outcome = IntegrityTerminalOutcome::from_internal(&error);
    assert!(matches!(
        outcome,
        IntegrityTerminalOutcome::ResourceLimited(_)
    ));
    // Exhaustion publishes no candidate findings or checkpoint advancement.
    // The maintained page count tracks receipt advancement, including this
    // terminal receipt; it does not prove that the failed atom completed.
    job.pages_completed = 1;
    job.last_receipt = IntegrityReceiptEnvelope {
        replay_key: IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence: 0,
        },
        receipt: IntegrityJobReceipt::Page(DeepIntegrityPage {
            job_id: job.id,
            page_sequence: 1,
            phase: job.checkpoint.phase(),
            status: DeepIntegrityPageStatus::Terminal(outcome.clone()),
            pages_completed: 1,
            findings_seen: 0,
            findings: Vec::new(),
            blocked_verifier_families: Vec::new(),
        }),
    };
    for receipt_acknowledged in [false, true] {
        job.state = IntegrityJobState::Terminal {
            outcome: outcome.clone(),
            receipt_acknowledged,
        };
        job.validate().expect("resource terminal should be valid");
        let payload = encode_integrity_job_payload(&job).expect("job should encode");
        let decoded = decode_integrity_job_payload(&payload).expect("job should decode");
        assert_eq!(decoded, job);
    }
}

#[test]
fn current_job_payload_round_trips_terminal_finding_and_typed_value_path() {
    let mut job = current_job_codec_fixture();
    let outcome = IntegrityTerminalOutcome::DeepCompleteWithFindings;
    let finding = IntegrityFinding {
        diagnostic_code:
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_ACTIVATION_WRITE_BLOCKED
                .raw(),
        class: IntegrityFindingClass::Corruption,
        severity: IntegritySeverity::Error,
        kind: IntegrityFindingKind::ConstraintViolation,
        entity: job.entity.clone(),
        store_path: job.entity.store_path().to_string(),
        phase: IntegrityPhase::FinalProofVectorCheck,
        verifier_family: IntegrityVerifierFamily::ValidatedConstraints,
        physical_key: vec![1, 2, 3],
        primary_key: Some(vec![2, 3]),
        field_paths: vec!["value".to_string()],
        value_path: Some(Box::new(ConstraintValuePath::new(vec![
            ConstraintValuePathComponent::RootField { field_id: 1 },
            ConstraintValuePathComponent::ListElement { index: 2 },
        ]))),
        constraint_id: Some(4),
        constraint_name: Some("accepted-check".to_string()),
        schema_index_id: None,
        relation_id: None,
        expected: Some("accepted".to_string()),
        observed: Some("violated".to_string()),
    };
    job.checkpoint = IntegrityCheckpoint::FinalProof;
    job.state = IntegrityJobState::Terminal {
        outcome: outcome.clone(),
        receipt_acknowledged: false,
    };
    job.findings_seen = 1;
    job.pages_completed = 1;
    job.last_receipt = IntegrityReceiptEnvelope {
        replay_key: IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence: 0,
        },
        receipt: IntegrityJobReceipt::Page(DeepIntegrityPage {
            job_id: job.id,
            page_sequence: 1,
            phase: IntegrityPhase::FinalProofVectorCheck,
            status: DeepIntegrityPageStatus::Terminal(outcome),
            pages_completed: 1,
            findings_seen: 1,
            findings: vec![finding],
            blocked_verifier_families: Vec::new(),
        }),
    };
    job.validate().expect("terminal fixture should be valid");

    for invalid_entity in [
        IntegrityEntityIdentity {
            entity_tag: 0,
            ..job.entity.clone()
        },
        IntegrityEntityIdentity {
            entity_path: String::new(),
            ..job.entity.clone()
        },
    ] {
        let mut corrupt = job.clone();
        let IntegrityJobReceipt::Page(page) = &mut corrupt.last_receipt.receipt else {
            unreachable!("the fixture carries a page receipt")
        };
        page.findings[0].entity = invalid_entity;
        // Bypass the trusted encoder to exercise restoration of corrupt
        // retained domain fields, rather than constructor-only admission.
        let raw = ic_wire::native::encode(&corrupt, limits()).unwrap();
        assert_eq!(decode_integrity_job_payload(&raw), Err(InternalError));
    }

    let payload = encode_integrity_job_payload(&job).expect("terminal job should encode");
    assert_eq!(
        decode_integrity_job_payload(&payload).expect("terminal job should decode"),
        job,
    );
}
