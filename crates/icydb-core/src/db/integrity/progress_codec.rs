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
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(super) use helper_tests::current_job_codec_fixture;
