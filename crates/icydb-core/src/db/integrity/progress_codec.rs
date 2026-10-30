//! Bounded IC Wire payload for durable Deep integrity jobs.
//!
//! IcyDB owns field limits, semantic restoration and the checksummed store
//! envelope. Remote Serde projections keep storage admission out of public RPC
//! DTOs; they describe the same Serde layout, without a second byte codec.

use serde::{Deserialize, Deserializer};

use crate::{
    db::{
        data::RawDataStoreKey,
        index::IndexKey,
        integrity::{
            DatabaseIncarnationId, DeepIntegrityPage, DeepIntegrityPageStatus,
            IntegrityAbortReceipt, IntegrityCheckpoint, IntegrityEntityIdentity, IntegrityFinding,
            IntegrityFindingClass, IntegrityFindingKind, IntegrityJob, IntegrityJobId,
            IntegrityJobOwner, IntegrityJobReceipt, IntegrityPhase, IntegritySeverity,
            IntegritySubmissionKey, IntegrityVerifierFamily, MAX_INTEGRITY_PATH_BYTES,
            job::{
                MAX_INTEGRITY_OWNER_BYTES, MAX_INTEGRITY_RECEIPT_FINDINGS,
                MAX_INTEGRITY_SUBMISSION_KEY_BYTES,
            },
        },
        schema::MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
    },
    error::{ConstraintValuePath, ConstraintValuePathComponent},
};

#[cfg(test)]
use crate::db::{
    integrity::{
        IntegrityJobState, IntegrityProofVector, IntegrityReceiptEnvelope,
        IntegrityReceiptReplayKey, IntegrityTerminalOutcome, proof::IntegrityStoreProof,
    },
    journal::JournalTailProofIdentity,
};

pub(super) const MAX_INTEGRITY_JOB_PAYLOAD_BYTES: usize = 512 * 1024 - (8 + 1 + 4 + 4);
pub(super) const MAX_PROOF_STORES: usize = 256;
pub(super) const MAX_BLOCKED_VERIFIER_FAMILIES: usize = 13;
const MAX_PHYSICAL_KEY_BYTES: usize = IndexKey::MAX_STORED_SIZE_USIZE;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InternalError;

// Payload bytes retain the store's existing outer ceiling. The allocation
// allowance includes Wire's eightfold traversal charge, not measured heap use.
const fn limits() -> ic_wire::Limits {
    ic_wire::Limits {
        bytes: MAX_INTEGRITY_JOB_PAYLOAD_BYTES,
        allocation: 16 * 1024 * 1024,
        nodes: 1_048_576,
        depth: 64,
    }
}

pub(super) fn encode_integrity_job_payload(job: &IntegrityJob) -> Result<Vec<u8>, InternalError> {
    job.validate().map_err(|_| InternalError)?;
    validate_bounds(job)?;
    ic_wire::native::encode(job, limits()).map_err(|_| InternalError)
}

pub(super) fn decode_integrity_job_payload(bytes: &[u8]) -> Result<IntegrityJob, InternalError> {
    let job: IntegrityJob = ic_wire::native::decode(bytes, limits()).map_err(|_| InternalError)?;
    IntegrityJobId::try_from_bytes(job.id.to_bytes()).map_err(|_| InternalError)?;
    DatabaseIncarnationId::try_from_bytes(job.database_incarnation_id.to_bytes())
        .map_err(|_| InternalError)?;
    job.validate().map_err(|_| InternalError)?;
    if encode_integrity_job_payload(&job)? != bytes {
        return Err(InternalError);
    }
    Ok(job)
}

// Encoding checks the same product ceilings used by the bounded decode
// projections. Semantic state/receipt/proof closure remains IntegrityJob's job.
fn validate_bounds(job: &IntegrityJob) -> Result<(), InternalError> {
    let proof = &job.captured_proof_vector;
    if proof.stores.len() > MAX_PROOF_STORES
        || proof.index_generations.len() > icydb_schema::MAX_FRAGMENT_INDEXES
        || proof.relation_generations.len() > icydb_schema::MAX_FRAGMENT_RELATIONS
        || job.blocked_verifier_families.len() > MAX_BLOCKED_VERIFIER_FAMILIES
        || job.entity.entity_path().len() > MAX_INTEGRITY_PATH_BYTES
        || job.entity.store_path().len() > MAX_INTEGRITY_PATH_BYTES
        || proof
            .stores
            .iter()
            .any(|store| store.store_path.len() > MAX_INTEGRITY_PATH_BYTES)
        || proof
            .index_generations
            .iter()
            .any(|index| index.store_path.len() > MAX_INTEGRITY_PATH_BYTES)
        || proof
            .relation_generations
            .iter()
            .any(|relation| relation.target_store_path.len() > MAX_INTEGRITY_PATH_BYTES)
    {
        return Err(InternalError);
    }
    match &job.checkpoint {
        IntegrityCheckpoint::Rows(checkpoint)
        | IntegrityCheckpoint::Index { checkpoint, .. }
        | IntegrityCheckpoint::ReverseRelation { checkpoint, .. } => {
            use crate::db::integrity::PhysicalUnitCheckpoint;
            let key = match checkpoint {
                PhysicalUnitCheckpoint::BeforeFirst => &[][..],
                PhysicalUnitCheckpoint::Within { physical_key, .. }
                | PhysicalUnitCheckpoint::After { physical_key } => physical_key,
            };
            if key.len() > MAX_PHYSICAL_KEY_BYTES {
                return Err(InternalError);
            }
        }
        IntegrityCheckpoint::QuickMetadata
        | IntegrityCheckpoint::Journal { .. }
        | IntegrityCheckpoint::FinalProof => {}
    }
    if let IntegrityJobReceipt::Page(page) = &job.last_receipt.receipt {
        if page.findings.len() > MAX_INTEGRITY_RECEIPT_FINDINGS
            || page.blocked_verifier_families.len() > MAX_BLOCKED_VERIFIER_FAMILIES
        {
            return Err(InternalError);
        }
        for finding in &page.findings {
            finding.entity.validate().map_err(|_| InternalError)?;
            if finding.store_path.len() > MAX_INTEGRITY_PATH_BYTES
                || finding.physical_key.len() > MAX_PHYSICAL_KEY_BYTES
                || finding
                    .primary_key
                    .as_ref()
                    .is_some_and(|key| key.len() > RawDataStoreKey::MAX_STORED_SIZE_USIZE)
                || finding.field_paths.len() > icydb_schema::MAX_FRAGMENT_FIELDS
                || finding
                    .field_paths
                    .iter()
                    .any(|path| path.len() > MAX_INTEGRITY_PATH_BYTES)
                || [
                    &finding.constraint_name,
                    &finding.expected,
                    &finding.observed,
                ]
                .iter()
                .any(|value| {
                    value
                        .as_ref()
                        .is_some_and(|value| value.len() > MAX_INTEGRITY_JOB_PAYLOAD_BYTES)
                })
            {
                return Err(InternalError);
            }
        }
    }
    Ok(())
}

// Wire owns bounded traversal and fallible allocation. These adapters only
// project admitted containers back into the existing domain APIs.
pub(super) fn deserialize_list<'de, D, T, const N: usize>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    ic_wire::List::<T, N>::deserialize(d).map(ic_wire::List::into_vec)
}

pub(super) fn deserialize_text<'de, D: Deserializer<'de>, const N: usize>(
    d: D,
) -> Result<String, D::Error> {
    ic_wire::Text::<N>::deserialize(d).map(ic_wire::Text::into_string)
}

pub(super) fn deserialize_physical_key<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<u8>, D::Error> {
    ic_wire::Bytes::<MAX_PHYSICAL_KEY_BYTES>::deserialize(d).map(ic_wire::Bytes::into_vec)
}

pub(super) fn deserialize_owner<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<IntegrityJobOwner, D::Error> {
    IntegrityJobOwner::new(deserialize_text::<D, MAX_INTEGRITY_OWNER_BYTES>(d)?)
        .map_err(|_| serde::de::Error::custom("invalid integrity owner"))
}

pub(super) fn deserialize_submission_key<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<IntegritySubmissionKey, D::Error> {
    IntegritySubmissionKey::new(deserialize_text::<D, MAX_INTEGRITY_SUBMISSION_KEY_BYTES>(
        d,
    )?)
    .map_err(|_| serde::de::Error::custom("invalid integrity submission key"))
}

#[derive(Deserialize)]
#[serde(remote = "IntegrityEntityIdentity")]
pub(super) struct EntityPayload {
    entity_tag: u64,
    #[serde(deserialize_with = "deserialize_text::<_, MAX_INTEGRITY_PATH_BYTES>")]
    entity_path: String,
    #[serde(deserialize_with = "deserialize_text::<_, MAX_INTEGRITY_PATH_BYTES>")]
    store_path: String,
}

#[derive(Deserialize)]
#[serde(remote = "IntegrityJobReceipt")]
pub(super) enum Receipt {
    Page(#[serde(deserialize_with = "PagePayload::deserialize")] DeepIntegrityPage),
    Abort(IntegrityAbortReceipt),
}

#[derive(Deserialize)]
#[serde(remote = "DeepIntegrityPage")]
struct PagePayload {
    job_id: IntegrityJobId,
    page_sequence: u64,
    phase: IntegrityPhase,
    status: DeepIntegrityPageStatus,
    pages_completed: u64,
    findings_seen: u64,
    #[serde(deserialize_with = "deserialize_findings")]
    findings: Vec<IntegrityFinding>,
    #[serde(deserialize_with = "deserialize_list::<_, _, MAX_BLOCKED_VERIFIER_FAMILIES>")]
    blocked_verifier_families: Vec<IntegrityVerifierFamily>,
}

#[derive(Deserialize)]
struct BoundedFinding(#[serde(deserialize_with = "Finding::deserialize")] IntegrityFinding);

fn deserialize_findings<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<IntegrityFinding>, D::Error> {
    // Both the bounded transport and domain projection reserve fallibly.
    let bounded = ic_wire::List::<BoundedFinding, MAX_INTEGRITY_RECEIPT_FINDINGS>::deserialize(d)?;
    let mut findings = Vec::new();
    findings
        .try_reserve_exact(bounded.as_slice().len())
        .map_err(serde::de::Error::custom)?;
    findings.extend(bounded.into_vec().into_iter().map(|finding| finding.0));
    Ok(findings)
}

#[derive(Deserialize)]
#[serde(remote = "IntegrityFinding")]
struct Finding {
    diagnostic_code: u16,
    class: IntegrityFindingClass,
    severity: IntegritySeverity,
    kind: IntegrityFindingKind,
    #[serde(deserialize_with = "EntityPayload::deserialize")]
    entity: IntegrityEntityIdentity,
    #[serde(deserialize_with = "deserialize_text::<_, MAX_INTEGRITY_PATH_BYTES>")]
    store_path: String,
    phase: IntegrityPhase,
    verifier_family: IntegrityVerifierFamily,
    #[serde(deserialize_with = "deserialize_physical_key")]
    physical_key: Vec<u8>,
    #[serde(deserialize_with = "deserialize_primary_key")]
    primary_key: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_field_paths")]
    field_paths: Vec<String>,
    #[serde(deserialize_with = "deserialize_value_path")]
    value_path: Option<Box<ConstraintValuePath>>,
    constraint_id: Option<u32>,
    #[serde(deserialize_with = "deserialize_optional_text")]
    constraint_name: Option<String>,
    schema_index_id: Option<u32>,
    relation_id: Option<u32>,
    #[serde(deserialize_with = "deserialize_optional_text")]
    expected: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_text")]
    observed: Option<String>,
}

fn deserialize_primary_key<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<ic_wire::Bytes<{ RawDataStoreKey::MAX_STORED_SIZE_USIZE }>>::deserialize(d)
        .map(|value| value.map(ic_wire::Bytes::into_vec))
}

fn deserialize_optional_text<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<ic_wire::Text<MAX_INTEGRITY_JOB_PAYLOAD_BYTES>>::deserialize(d)
        .map(|value| value.map(ic_wire::Text::into_string))
}

fn deserialize_field_paths<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let paths = ic_wire::List::<
        ic_wire::Text<MAX_INTEGRITY_PATH_BYTES>,
        { icydb_schema::MAX_FRAGMENT_FIELDS },
    >::deserialize(d)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(paths.as_slice().len())
        .map_err(serde::de::Error::custom)?;
    values.extend(paths.into_vec().into_iter().map(ic_wire::Text::into_string));
    Ok(values)
}

fn deserialize_value_path<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<Box<ConstraintValuePath>>, D::Error> {
    Option::<ic_wire::List<ConstraintValuePathComponent, MAX_ACCEPTED_TARGET_PATH_COMPONENTS>>::deserialize(d)
        .map(|value| value.map(|components| Box::new(ConstraintValuePath::new(components.into_vec()))))
}

#[cfg(test)]
pub(super) fn current_job_codec_fixture() -> IntegrityJob {
    let id = IntegrityJobId::try_from_bytes([1; 32]).expect("fixture job ID is nonzero");
    let database_incarnation_id = DatabaseIncarnationId::for_tests(2);
    let entity = IntegrityEntityIdentity {
        entity_tag: 3,
        entity_path: "fixture::entity".to_string(),
        store_path: "fixture::store".to_string(),
    };
    let checkpoint = IntegrityCheckpoint::QuickMetadata;
    let captured_proof_vector = IntegrityProofVector {
        database_incarnation_id,
        accepted_schema_version: 1,
        accepted_schema_fingerprint: [4; 16],
        inspection_plan_fingerprint: [5; 32],
        database_control_fingerprint: [6; 32],
        allocation_registry_generation: 7,
        stores: vec![IntegrityStoreProof {
            store_path: "fixture::store".to_string(),
            data_generation: 8,
            index_generation: 9,
            journal: JournalTailProofIdentity::from_persisted_parts(1, 0, 0, 1, 2),
        }],
        index_generations: Vec::new(),
        relation_generations: Vec::new(),
    };
    let receipt = IntegrityJobReceipt::Page(DeepIntegrityPage {
        job_id: id,
        page_sequence: 0,
        phase: IntegrityPhase::QuickMetadata,
        status: DeepIntegrityPageStatus::InProgress,
        pages_completed: 0,
        findings_seen: 0,
        findings: Vec::new(),
        blocked_verifier_families: Vec::new(),
    });
    let job = IntegrityJob {
        id,
        database_incarnation_id,
        owner: IntegrityJobOwner::new("fixture-owner").expect("fixture owner is valid"),
        submission_key: IntegritySubmissionKey::new("fixture-submission")
            .expect("fixture submission key is valid"),
        entity,
        accepted_schema_version: 1,
        accepted_schema_fingerprint: [4; 16],
        inspection_plan_fingerprint: [5; 32],
        checkpoint,
        captured_proof_vector,
        state: IntegrityJobState::InProgress,
        lease_deadline_nanos: 10,
        findings_seen: 0,
        pages_completed: 0,
        blocked_verifier_families: Vec::new(),
        last_receipt: IntegrityReceiptEnvelope {
            replay_key: IntegrityReceiptReplayKey::Start,
            receipt,
        },
    };
    job.validate().expect("codec fixture is internally valid");
    job
}

#[cfg(test)]
mod tests {
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
                222, 35, 143, 11, 103, 162, 33, 100, 19, 68, 153, 128, 156, 33, 54, 209, 253, 189,
                150, 165, 125, 203, 239, 224, 47, 235, 72, 223, 187, 3, 10, 23,
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
}
