//! Module: db::integrity::progress_codec
//! Responsibility: bounded direct encoding of the current Deep integrity job payload.
//! Does not own: the stable-map envelope, checksum, job lifecycle, or public Candid DTOs.
//! Boundary: invariant-bearing Deep job <-> current big-endian payload bytes.

use crate::{
    db::{
        data::RawDataStoreKey,
        index::IndexKey,
        integrity::{
            DatabaseIncarnationId, DeepIntegrityPage, DeepIntegrityPageStatus,
            IntegrityAbortReceipt, IntegrityAbortStatus, IntegrityAuthorityClass,
            IntegrityAuthorityDiagnostic, IntegrityCheckpoint, IntegrityEntityIdentity,
            IntegrityFinding, IntegrityFindingClass, IntegrityFindingKind, IntegrityJob,
            IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt, IntegrityJobState,
            IntegrityPendingTerminal, IntegrityPhase, IntegrityProofVector,
            IntegrityReceiptEnvelope, IntegrityReceiptReplayKey, IntegrityResourceDiagnostic,
            IntegritySeverity, IntegritySubmissionKey, IntegrityTerminalOutcome,
            IntegrityVerifierFamily, MAX_INTEGRITY_PATH_BYTES, PhysicalUnitCheckpoint,
            job::MAX_INTEGRITY_RECEIPT_FINDINGS,
            proof::{
                IntegrityIndexGenerationProof, IntegrityRelationGenerationProof,
                IntegrityStoreProof,
            },
        },
        journal::{JournalInspectionCheckpoint, JournalTailProofIdentity},
        schema::MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
    },
    error::{ConstraintValuePath, ConstraintValuePathComponent},
};

pub(super) const MAX_INTEGRITY_JOB_PAYLOAD_BYTES: usize = 512 * 1024 - (8 + 1 + 4 + 4);

const MAX_PROOF_STORES: usize = 256;
const MAX_BLOCKED_VERIFIER_FAMILIES: usize = 13;
const MAX_FINDING_FIELD_PATHS: usize = icydb_schema::MAX_FRAGMENT_FIELDS;
const MAX_FINDING_TEXT_BYTES: usize = MAX_INTEGRITY_JOB_PAYLOAD_BYTES;
const MAX_PHYSICAL_KEY_BYTES: usize = IndexKey::MAX_STORED_SIZE_USIZE;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InternalError;

impl InternalError {
    const fn store_corruption() -> Self {
        Self
    }

    const fn store_unsupported() -> Self {
        Self
    }
}

struct JobWriter {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl JobWriter {
    const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            overflowed: false,
        }
    }

    fn push_u8(&mut self, value: u8) {
        self.push_bytes(&[value]);
    }

    fn push_u16(&mut self, value: u16) {
        self.push_bytes(&value.to_be_bytes());
    }

    fn push_u32(&mut self, value: u32) {
        self.push_bytes(&value.to_be_bytes());
    }

    fn push_u64(&mut self, value: u64) {
        self.push_bytes(&value.to_be_bytes());
    }

    fn push_bool(&mut self, value: bool) {
        self.push_u8(u8::from(value));
    }

    fn push_len(&mut self, value: usize) -> Result<(), InternalError> {
        self.push_u32(u32::try_from(value).map_err(|_| InternalError)?);
        Ok(())
    }

    fn push_bounded_string(&mut self, value: &str, max_bytes: usize) -> Result<(), InternalError> {
        self.push_bounded_len_prefixed_bytes(value.as_bytes(), max_bytes)
    }

    fn push_bounded_len_prefixed_bytes(
        &mut self,
        value: &[u8],
        max_bytes: usize,
    ) -> Result<(), InternalError> {
        if value.len() > max_bytes {
            return Err(InternalError);
        }
        self.push_len(value.len())?;
        self.push_bytes(value);
        Ok(())
    }

    fn push_optional_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.push_u8(1);
                self.push_u32(value);
            }
            None => self.push_u8(0),
        }
    }

    fn push_bytes(&mut self, value: &[u8]) {
        if value.len() > MAX_INTEGRITY_JOB_PAYLOAD_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return;
        }
        self.bytes.extend_from_slice(value);
    }

    fn finish(self) -> Result<Vec<u8>, InternalError> {
        if self.overflowed {
            return Err(InternalError);
        }
        Ok(self.bytes)
    }
}

struct JobReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> JobReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u8(&mut self) -> Result<u8, InternalError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, InternalError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, InternalError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_bool(&mut self) -> Result<bool, InternalError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(InternalError),
        }
    }

    fn read_bounded_count(&mut self, max: usize) -> Result<usize, InternalError> {
        let count = self.read_u32()? as usize;
        if count > max || count > self.remaining() {
            return Err(InternalError);
        }
        Ok(count)
    }

    fn read_bounded_string(&mut self, max_bytes: usize) -> Result<String, InternalError> {
        let bytes = self.read_bounded_len_prefixed_bytes(max_bytes)?;
        let value = std::str::from_utf8(bytes).map_err(|_| InternalError)?;
        Ok(value.to_string())
    }

    fn read_bounded_len_prefixed_bytes(
        &mut self,
        max_bytes: usize,
    ) -> Result<&'a [u8], InternalError> {
        let len = self.read_u32()? as usize;
        if len > max_bytes {
            return Err(InternalError);
        }
        self.read_slice(len)
    }

    fn read_optional_u32(&mut self) -> Result<Option<u32>, InternalError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u32().map(Some),
            _ => Err(InternalError),
        }
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], InternalError> {
        let bytes = self.read_slice(N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], InternalError> {
        let end = self.offset.checked_add(len).ok_or(InternalError)?;
        let value = self.bytes.get(self.offset..end).ok_or(InternalError)?;
        self.offset = end;
        Ok(value)
    }

    const fn finish(self) -> Result<(), InternalError> {
        if self.offset != self.bytes.len() {
            return Err(InternalError);
        }
        Ok(())
    }
}

pub(super) fn encode_integrity_job_payload(job: &IntegrityJob) -> Result<Vec<u8>, InternalError> {
    let mut writer = JobWriter::new();
    writer.push_bytes(&job.id.to_bytes());
    writer.push_bytes(&job.database_incarnation_id.to_bytes());
    writer.push_bounded_string(job.owner.as_str(), super::job::MAX_INTEGRITY_OWNER_BYTES)?;
    writer.push_bounded_string(
        job.submission_key.as_str(),
        super::job::MAX_INTEGRITY_SUBMISSION_KEY_BYTES,
    )?;
    encode_entity_identity(&mut writer, &job.entity)?;
    writer.push_u32(job.accepted_schema_version);
    writer.push_bytes(&job.accepted_schema_fingerprint);
    writer.push_bytes(&job.inspection_plan_fingerprint);
    encode_checkpoint(&mut writer, &job.checkpoint)?;
    encode_proof_vector(&mut writer, &job.captured_proof_vector)?;
    encode_job_state(&mut writer, &job.state);
    writer.push_u64(job.lease_deadline_nanos);
    writer.push_u64(job.findings_seen);
    writer.push_u64(job.pages_completed);
    encode_verifier_families(&mut writer, &job.blocked_verifier_families)?;
    encode_receipt_envelope(&mut writer, &job.last_receipt)?;
    writer.finish()
}

pub(super) fn decode_integrity_job_payload(bytes: &[u8]) -> Result<IntegrityJob, InternalError> {
    if bytes.len() > MAX_INTEGRITY_JOB_PAYLOAD_BYTES {
        return Err(InternalError::store_corruption());
    }

    let mut reader = JobReader::new(bytes);
    let id = decode_job_id(&mut reader)?;
    let database_incarnation_id = decode_database_incarnation_id(&mut reader)?;
    let owner =
        IntegrityJobOwner::new(reader.read_bounded_string(super::job::MAX_INTEGRITY_OWNER_BYTES)?)
            .map_err(|_| InternalError::store_corruption())?;
    let submission_key = IntegritySubmissionKey::new(
        reader.read_bounded_string(super::job::MAX_INTEGRITY_SUBMISSION_KEY_BYTES)?,
    )
    .map_err(|_| InternalError::store_corruption())?;
    let entity = decode_entity_identity(&mut reader)?;
    let accepted_schema_version = reader.read_u32()?;
    let accepted_schema_fingerprint = reader.read_array()?;
    let inspection_plan_fingerprint = reader.read_array()?;
    let checkpoint = decode_checkpoint(&mut reader)?;
    let captured_proof_vector = decode_proof_vector(&mut reader)?;
    let state = decode_job_state(&mut reader)?;
    let lease_deadline_nanos = reader.read_u64()?;
    let findings_seen = reader.read_u64()?;
    let pages_completed = reader.read_u64()?;
    let blocked_verifier_families = decode_verifier_families(&mut reader)?;
    let last_receipt = decode_receipt_envelope(&mut reader)?;
    reader.finish()?;

    let job = IntegrityJob {
        id,
        database_incarnation_id,
        owner,
        submission_key,
        entity,
        accepted_schema_version,
        accepted_schema_fingerprint,
        inspection_plan_fingerprint,
        checkpoint,
        captured_proof_vector,
        state,
        lease_deadline_nanos,
        findings_seen,
        pages_completed,
        blocked_verifier_families,
        last_receipt,
    };
    job.validate()
        .map_err(|_| InternalError::store_corruption())?;
    Ok(job)
}

fn decode_job_id(reader: &mut JobReader<'_>) -> Result<IntegrityJobId, InternalError> {
    IntegrityJobId::try_from_bytes(reader.read_array()?)
        .map_err(|_| InternalError::store_corruption())
}

fn decode_database_incarnation_id(
    reader: &mut JobReader<'_>,
) -> Result<DatabaseIncarnationId, InternalError> {
    DatabaseIncarnationId::try_from_bytes(reader.read_array()?)
        .map_err(|_| InternalError::store_corruption())
}

fn encode_entity_identity(
    writer: &mut JobWriter,
    identity: &IntegrityEntityIdentity,
) -> Result<(), InternalError> {
    writer.push_u64(identity.entity_tag());
    writer.push_bounded_string(identity.entity_path(), MAX_INTEGRITY_PATH_BYTES)?;
    writer.push_bounded_string(identity.store_path(), MAX_INTEGRITY_PATH_BYTES)
}

fn decode_entity_identity(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityEntityIdentity, InternalError> {
    let identity = IntegrityEntityIdentity {
        entity_tag: reader.read_u64()?,
        entity_path: reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?,
        store_path: reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?,
    };
    identity
        .validate()
        .map_err(|_| InternalError::store_corruption())?;
    Ok(identity)
}

fn encode_checkpoint(
    writer: &mut JobWriter,
    checkpoint: &IntegrityCheckpoint,
) -> Result<(), InternalError> {
    match checkpoint {
        IntegrityCheckpoint::QuickMetadata => writer.push_u8(1),
        IntegrityCheckpoint::Rows(checkpoint) => {
            writer.push_u8(2);
            encode_physical_checkpoint(writer, checkpoint)?;
        }
        IntegrityCheckpoint::Index {
            ordinal,
            checkpoint,
        } => {
            writer.push_u8(3);
            writer.push_u32(*ordinal);
            encode_physical_checkpoint(writer, checkpoint)?;
        }
        IntegrityCheckpoint::ReverseRelation {
            ordinal,
            checkpoint,
        } => {
            writer.push_u8(4);
            writer.push_u32(*ordinal);
            encode_physical_checkpoint(writer, checkpoint)?;
        }
        IntegrityCheckpoint::Journal {
            store_ordinal,
            checkpoint,
        } => {
            writer.push_u8(5);
            writer.push_u32(*store_ordinal);
            encode_journal_checkpoint(writer, checkpoint);
        }
        IntegrityCheckpoint::FinalProof => writer.push_u8(6),
    }
    Ok(())
}

fn decode_checkpoint(reader: &mut JobReader<'_>) -> Result<IntegrityCheckpoint, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityCheckpoint::QuickMetadata),
        2 => decode_physical_checkpoint(reader).map(IntegrityCheckpoint::Rows),
        3 => Ok(IntegrityCheckpoint::Index {
            ordinal: reader.read_u32()?,
            checkpoint: decode_physical_checkpoint(reader)?,
        }),
        4 => Ok(IntegrityCheckpoint::ReverseRelation {
            ordinal: reader.read_u32()?,
            checkpoint: decode_physical_checkpoint(reader)?,
        }),
        5 => Ok(IntegrityCheckpoint::Journal {
            store_ordinal: reader.read_u32()?,
            checkpoint: decode_journal_checkpoint(reader)?,
        }),
        6 => Ok(IntegrityCheckpoint::FinalProof),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_physical_checkpoint(
    writer: &mut JobWriter,
    checkpoint: &PhysicalUnitCheckpoint,
) -> Result<(), InternalError> {
    match checkpoint {
        PhysicalUnitCheckpoint::BeforeFirst => writer.push_u8(1),
        PhysicalUnitCheckpoint::Within {
            physical_key,
            verifier_family,
            ordinal,
        } => {
            writer.push_u8(2);
            writer.push_bounded_len_prefixed_bytes(physical_key, MAX_PHYSICAL_KEY_BYTES)?;
            encode_verifier_family(writer, *verifier_family);
            writer.push_u32(*ordinal);
        }
        PhysicalUnitCheckpoint::After { physical_key } => {
            writer.push_u8(3);
            writer.push_bounded_len_prefixed_bytes(physical_key, MAX_PHYSICAL_KEY_BYTES)?;
        }
    }
    Ok(())
}

fn decode_physical_checkpoint(
    reader: &mut JobReader<'_>,
) -> Result<PhysicalUnitCheckpoint, InternalError> {
    match reader.read_u8()? {
        1 => Ok(PhysicalUnitCheckpoint::BeforeFirst),
        2 => Ok(PhysicalUnitCheckpoint::Within {
            physical_key: reader
                .read_bounded_len_prefixed_bytes(MAX_PHYSICAL_KEY_BYTES)?
                .to_vec(),
            verifier_family: decode_verifier_family(reader)?,
            ordinal: reader.read_u32()?,
        }),
        3 => Ok(PhysicalUnitCheckpoint::After {
            physical_key: reader
                .read_bounded_len_prefixed_bytes(MAX_PHYSICAL_KEY_BYTES)?
                .to_vec(),
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_journal_checkpoint(writer: &mut JobWriter, checkpoint: &JournalInspectionCheckpoint) {
    match checkpoint {
        JournalInspectionCheckpoint::BeforeFirst => writer.push_u8(1),
        JournalInspectionCheckpoint::BeforeBatch { sequence } => {
            writer.push_u8(2);
            writer.push_u64(*sequence);
        }
        JournalInspectionCheckpoint::CheckingBatchIdentity {
            sequence,
            batch_id,
            next_prior_sequence,
        } => {
            writer.push_u8(3);
            writer.push_u64(*sequence);
            writer.push_bytes(batch_id);
            writer.push_u64(*next_prior_sequence);
        }
        JournalInspectionCheckpoint::AfterBatch { sequence } => {
            writer.push_u8(4);
            writer.push_u64(*sequence);
        }
    }
}

fn decode_journal_checkpoint(
    reader: &mut JobReader<'_>,
) -> Result<JournalInspectionCheckpoint, InternalError> {
    match reader.read_u8()? {
        1 => Ok(JournalInspectionCheckpoint::BeforeFirst),
        2 => Ok(JournalInspectionCheckpoint::BeforeBatch {
            sequence: reader.read_u64()?,
        }),
        3 => Ok(JournalInspectionCheckpoint::CheckingBatchIdentity {
            sequence: reader.read_u64()?,
            batch_id: reader.read_array()?,
            next_prior_sequence: reader.read_u64()?,
        }),
        4 => Ok(JournalInspectionCheckpoint::AfterBatch {
            sequence: reader.read_u64()?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_proof_vector(
    writer: &mut JobWriter,
    proof: &IntegrityProofVector,
) -> Result<(), InternalError> {
    writer.push_bytes(&proof.database_incarnation_id.to_bytes());
    writer.push_u32(proof.accepted_schema_version);
    writer.push_bytes(&proof.accepted_schema_fingerprint);
    writer.push_bytes(&proof.inspection_plan_fingerprint);
    writer.push_bytes(&proof.database_control_fingerprint);
    writer.push_u64(proof.allocation_registry_generation);

    push_bounded_count(writer, proof.stores.len(), MAX_PROOF_STORES)?;
    for store in &proof.stores {
        writer.push_bounded_string(&store.store_path, MAX_INTEGRITY_PATH_BYTES)?;
        writer.push_u64(store.data_generation);
        writer.push_u64(store.index_generation);
        encode_journal_proof(writer, store.journal);
    }

    push_bounded_count(
        writer,
        proof.index_generations.len(),
        icydb_schema::MAX_FRAGMENT_INDEXES,
    )?;
    for index in &proof.index_generations {
        writer.push_bounded_string(&index.store_path, MAX_INTEGRITY_PATH_BYTES)?;
        writer.push_u32(index.schema_index_id);
        writer.push_u64(index.physical_generation);
    }

    push_bounded_count(
        writer,
        proof.relation_generations.len(),
        icydb_schema::MAX_FRAGMENT_RELATIONS,
    )?;
    for relation in &proof.relation_generations {
        writer.push_bounded_string(&relation.target_store_path, MAX_INTEGRITY_PATH_BYTES)?;
        writer.push_u32(relation.relation_id);
        writer.push_u64(relation.physical_generation);
    }
    Ok(())
}

fn decode_proof_vector(reader: &mut JobReader<'_>) -> Result<IntegrityProofVector, InternalError> {
    let database_incarnation_id = decode_database_incarnation_id(reader)?;
    let accepted_schema_version = reader.read_u32()?;
    let accepted_schema_fingerprint = reader.read_array()?;
    let inspection_plan_fingerprint = reader.read_array()?;
    let database_control_fingerprint = reader.read_array()?;
    let allocation_registry_generation = reader.read_u64()?;

    let store_count = reader.read_bounded_count(MAX_PROOF_STORES)?;
    let mut stores = Vec::with_capacity(store_count);
    for _ in 0..store_count {
        stores.push(IntegrityStoreProof {
            store_path: reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?,
            data_generation: reader.read_u64()?,
            index_generation: reader.read_u64()?,
            journal: decode_journal_proof(reader)?,
        });
    }

    let index_count = reader.read_bounded_count(icydb_schema::MAX_FRAGMENT_INDEXES)?;
    let mut index_generations = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        index_generations.push(IntegrityIndexGenerationProof {
            store_path: reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?,
            schema_index_id: reader.read_u32()?,
            physical_generation: reader.read_u64()?,
        });
    }

    let relation_count = reader.read_bounded_count(icydb_schema::MAX_FRAGMENT_RELATIONS)?;
    let mut relation_generations = Vec::with_capacity(relation_count);
    for _ in 0..relation_count {
        relation_generations.push(IntegrityRelationGenerationProof {
            target_store_path: reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?,
            relation_id: reader.read_u32()?,
            physical_generation: reader.read_u64()?,
        });
    }

    Ok(IntegrityProofVector {
        database_incarnation_id,
        accepted_schema_version,
        accepted_schema_fingerprint,
        inspection_plan_fingerprint,
        database_control_fingerprint,
        allocation_registry_generation,
        stores,
        index_generations,
        relation_generations,
    })
}

fn encode_journal_proof(writer: &mut JobWriter, proof: JournalTailProofIdentity) {
    writer.push_u64(proof.data_mutation_revision());
    writer.push_u64(proof.fold_sequence());
    writer.push_u64(proof.fold_epoch());
    writer.push_u64(proof.next_append_sequence());
    writer.push_u64(proof.physical_record_count());
}

fn decode_journal_proof(
    reader: &mut JobReader<'_>,
) -> Result<JournalTailProofIdentity, InternalError> {
    Ok(JournalTailProofIdentity::from_persisted_parts(
        reader.read_u64()?,
        reader.read_u64()?,
        reader.read_u64()?,
        reader.read_u64()?,
        reader.read_u64()?,
    ))
}

fn encode_job_state(writer: &mut JobWriter, state: &IntegrityJobState) {
    match state {
        IntegrityJobState::InProgress => writer.push_u8(1),
        IntegrityJobState::TerminalPending(pending) => {
            writer.push_u8(2);
            encode_pending_terminal(writer, *pending);
        }
        IntegrityJobState::Terminal {
            outcome,
            receipt_acknowledged,
        } => {
            writer.push_u8(3);
            encode_terminal_outcome(writer, outcome);
            writer.push_bool(*receipt_acknowledged);
        }
    }
}

fn decode_job_state(reader: &mut JobReader<'_>) -> Result<IntegrityJobState, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityJobState::InProgress),
        2 => decode_pending_terminal(reader).map(IntegrityJobState::TerminalPending),
        3 => Ok(IntegrityJobState::Terminal {
            outcome: decode_terminal_outcome(reader)?,
            receipt_acknowledged: reader.read_bool()?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_pending_terminal(writer: &mut JobWriter, pending: IntegrityPendingTerminal) {
    writer.push_u8(match pending {
        IntegrityPendingTerminal::Expired => 1,
        IntegrityPendingTerminal::Aborted => 2,
    });
}

fn decode_pending_terminal(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityPendingTerminal, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityPendingTerminal::Expired),
        2 => Ok(IntegrityPendingTerminal::Aborted),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_terminal_outcome(writer: &mut JobWriter, outcome: &IntegrityTerminalOutcome) {
    match outcome {
        IntegrityTerminalOutcome::DeepCompleteClean => writer.push_u8(1),
        IntegrityTerminalOutcome::DeepCompleteWithFindings => writer.push_u8(2),
        IntegrityTerminalOutcome::Invalidated => writer.push_u8(3),
        IntegrityTerminalOutcome::Uninspectable(diagnostic) => {
            writer.push_u8(4);
            writer.push_u16(diagnostic.diagnostic_code());
            encode_authority_class(writer, diagnostic.class());
        }
        IntegrityTerminalOutcome::ResourceLimited(diagnostic) => {
            writer.push_u8(5);
            writer.push_u16(diagnostic.diagnostic_code());
        }
        IntegrityTerminalOutcome::Expired => writer.push_u8(6),
        IntegrityTerminalOutcome::Aborted => writer.push_u8(7),
    }
}

fn decode_terminal_outcome(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityTerminalOutcome, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityTerminalOutcome::DeepCompleteClean),
        2 => Ok(IntegrityTerminalOutcome::DeepCompleteWithFindings),
        3 => Ok(IntegrityTerminalOutcome::Invalidated),
        4 => Ok(IntegrityTerminalOutcome::Uninspectable(
            IntegrityAuthorityDiagnostic {
                diagnostic_code: reader.read_u16()?,
                class: decode_authority_class(reader)?,
            },
        )),
        5 => Ok(IntegrityTerminalOutcome::ResourceLimited(
            IntegrityResourceDiagnostic {
                diagnostic_code: reader.read_u16()?,
            },
        )),
        6 => Ok(IntegrityTerminalOutcome::Expired),
        7 => Ok(IntegrityTerminalOutcome::Aborted),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_authority_class(writer: &mut JobWriter, class: IntegrityAuthorityClass) {
    writer.push_u8(match class {
        IntegrityAuthorityClass::Corruption => 1,
        IntegrityAuthorityClass::IncompatiblePersistedFormat => 2,
        IntegrityAuthorityClass::InvariantViolation => 3,
        IntegrityAuthorityClass::Unsupported => 4,
        IntegrityAuthorityClass::Internal => 5,
    });
}

fn decode_authority_class(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityAuthorityClass, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityAuthorityClass::Corruption),
        2 => Ok(IntegrityAuthorityClass::IncompatiblePersistedFormat),
        3 => Ok(IntegrityAuthorityClass::InvariantViolation),
        4 => Ok(IntegrityAuthorityClass::Unsupported),
        5 => Ok(IntegrityAuthorityClass::Internal),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_receipt_envelope(
    writer: &mut JobWriter,
    envelope: &IntegrityReceiptEnvelope,
) -> Result<(), InternalError> {
    match envelope.replay_key {
        IntegrityReceiptReplayKey::Start => writer.push_u8(1),
        IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence,
        } => {
            writer.push_u8(2);
            writer.push_u64(acknowledged_sequence);
        }
    }
    encode_job_receipt(writer, &envelope.receipt)
}

fn decode_receipt_envelope(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityReceiptEnvelope, InternalError> {
    let replay_key = match reader.read_u8()? {
        1 => IntegrityReceiptReplayKey::Start,
        2 => IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence: reader.read_u64()?,
        },
        _ => return Err(InternalError::store_corruption()),
    };
    Ok(IntegrityReceiptEnvelope {
        replay_key,
        receipt: decode_job_receipt(reader)?,
    })
}

fn encode_job_receipt(
    writer: &mut JobWriter,
    receipt: &IntegrityJobReceipt,
) -> Result<(), InternalError> {
    match receipt {
        IntegrityJobReceipt::Page(page) => {
            writer.push_u8(1);
            encode_page(writer, page)?;
        }
        IntegrityJobReceipt::Abort(receipt) => {
            writer.push_u8(2);
            encode_abort_receipt(writer, receipt);
        }
    }
    Ok(())
}

fn decode_job_receipt(reader: &mut JobReader<'_>) -> Result<IntegrityJobReceipt, InternalError> {
    match reader.read_u8()? {
        1 => decode_page(reader).map(IntegrityJobReceipt::Page),
        2 => decode_abort_receipt(reader).map(IntegrityJobReceipt::Abort),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_page(writer: &mut JobWriter, page: &DeepIntegrityPage) -> Result<(), InternalError> {
    writer.push_bytes(&page.job_id.to_bytes());
    writer.push_u64(page.page_sequence);
    encode_phase(writer, page.phase);
    match &page.status {
        DeepIntegrityPageStatus::InProgress => writer.push_u8(1),
        DeepIntegrityPageStatus::Terminal(outcome) => {
            writer.push_u8(2);
            encode_terminal_outcome(writer, outcome);
        }
    }
    writer.push_u64(page.pages_completed);
    writer.push_u64(page.findings_seen);
    push_bounded_count(writer, page.findings.len(), MAX_INTEGRITY_RECEIPT_FINDINGS)?;
    for finding in &page.findings {
        encode_finding(writer, finding)?;
    }
    encode_verifier_families(writer, &page.blocked_verifier_families)
}

fn decode_page(reader: &mut JobReader<'_>) -> Result<DeepIntegrityPage, InternalError> {
    let job_id = decode_job_id(reader)?;
    let page_sequence = reader.read_u64()?;
    let phase = decode_phase(reader)?;
    let status = match reader.read_u8()? {
        1 => DeepIntegrityPageStatus::InProgress,
        2 => DeepIntegrityPageStatus::Terminal(decode_terminal_outcome(reader)?),
        _ => return Err(InternalError::store_corruption()),
    };
    let pages_completed = reader.read_u64()?;
    let findings_seen = reader.read_u64()?;
    let finding_count = reader.read_bounded_count(MAX_INTEGRITY_RECEIPT_FINDINGS)?;
    let mut findings = Vec::with_capacity(finding_count);
    for _ in 0..finding_count {
        findings.push(decode_finding(reader)?);
    }
    let blocked_verifier_families = decode_verifier_families(reader)?;
    Ok(DeepIntegrityPage {
        job_id,
        page_sequence,
        phase,
        status,
        pages_completed,
        findings_seen,
        findings,
        blocked_verifier_families,
    })
}

fn encode_abort_receipt(writer: &mut JobWriter, receipt: &IntegrityAbortReceipt) {
    writer.push_bytes(&receipt.job_id.to_bytes());
    writer.push_u64(receipt.page_sequence);
    match &receipt.status {
        IntegrityAbortStatus::TerminationPending(pending) => {
            writer.push_u8(1);
            encode_pending_terminal(writer, *pending);
        }
        IntegrityAbortStatus::Terminal(outcome) => {
            writer.push_u8(2);
            encode_terminal_outcome(writer, outcome);
        }
    }
}

fn decode_abort_receipt(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityAbortReceipt, InternalError> {
    let job_id = decode_job_id(reader)?;
    let page_sequence = reader.read_u64()?;
    let status = match reader.read_u8()? {
        1 => IntegrityAbortStatus::TerminationPending(decode_pending_terminal(reader)?),
        2 => IntegrityAbortStatus::Terminal(decode_terminal_outcome(reader)?),
        _ => return Err(InternalError::store_corruption()),
    };
    Ok(IntegrityAbortReceipt {
        job_id,
        page_sequence,
        status,
    })
}

fn encode_verifier_families(
    writer: &mut JobWriter,
    families: &[IntegrityVerifierFamily],
) -> Result<(), InternalError> {
    push_bounded_count(writer, families.len(), MAX_BLOCKED_VERIFIER_FAMILIES)?;
    for family in families {
        encode_verifier_family(writer, *family);
    }
    Ok(())
}

fn decode_verifier_families(
    reader: &mut JobReader<'_>,
) -> Result<Vec<IntegrityVerifierFamily>, InternalError> {
    let count = reader.read_bounded_count(MAX_BLOCKED_VERIFIER_FAMILIES)?;
    let mut families = Vec::with_capacity(count);
    for _ in 0..count {
        families.push(decode_verifier_family(reader)?);
    }
    Ok(families)
}

fn encode_verifier_family(writer: &mut JobWriter, family: IntegrityVerifierFamily) {
    writer.push_u8(match family {
        IntegrityVerifierFamily::DataKey => 1,
        IntegrityVerifierFamily::RowEnvelope => 2,
        IntegrityVerifierFamily::FieldValue => 3,
        IntegrityVerifierFamily::PrimaryKey => 4,
        IntegrityVerifierFamily::IdentityState => 5,
        IntegrityVerifierFamily::ValidatedConstraints => 6,
        IntegrityVerifierFamily::ForwardIndex => 7,
        IntegrityVerifierFamily::IndexEntry => 8,
        IntegrityVerifierFamily::UniqueIndex => 9,
        IntegrityVerifierFamily::Relation => 10,
        IntegrityVerifierFamily::ReverseRelationEntry => 11,
        IntegrityVerifierFamily::JournalEnvelope => 12,
        IntegrityVerifierFamily::JournalBatchIdentity => 13,
    });
}

fn decode_verifier_family(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityVerifierFamily, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityVerifierFamily::DataKey),
        2 => Ok(IntegrityVerifierFamily::RowEnvelope),
        3 => Ok(IntegrityVerifierFamily::FieldValue),
        4 => Ok(IntegrityVerifierFamily::PrimaryKey),
        5 => Ok(IntegrityVerifierFamily::IdentityState),
        6 => Ok(IntegrityVerifierFamily::ValidatedConstraints),
        7 => Ok(IntegrityVerifierFamily::ForwardIndex),
        8 => Ok(IntegrityVerifierFamily::IndexEntry),
        9 => Ok(IntegrityVerifierFamily::UniqueIndex),
        10 => Ok(IntegrityVerifierFamily::Relation),
        11 => Ok(IntegrityVerifierFamily::ReverseRelationEntry),
        12 => Ok(IntegrityVerifierFamily::JournalEnvelope),
        13 => Ok(IntegrityVerifierFamily::JournalBatchIdentity),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_phase(writer: &mut JobWriter, phase: IntegrityPhase) {
    writer.push_u8(match phase {
        IntegrityPhase::QuickMetadata => 1,
        IntegrityPhase::Rows => 2,
        IntegrityPhase::IndexEntries => 3,
        IntegrityPhase::ReverseRelations => 4,
        IntegrityPhase::JournalTails => 5,
        IntegrityPhase::FinalProofVectorCheck => 6,
    });
}

fn decode_phase(reader: &mut JobReader<'_>) -> Result<IntegrityPhase, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityPhase::QuickMetadata),
        2 => Ok(IntegrityPhase::Rows),
        3 => Ok(IntegrityPhase::IndexEntries),
        4 => Ok(IntegrityPhase::ReverseRelations),
        5 => Ok(IntegrityPhase::JournalTails),
        6 => Ok(IntegrityPhase::FinalProofVectorCheck),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_finding(writer: &mut JobWriter, finding: &IntegrityFinding) -> Result<(), InternalError> {
    writer.push_u16(finding.diagnostic_code);
    encode_finding_class(writer, finding.class);
    encode_severity(writer, finding.severity);
    encode_finding_kind(writer, finding.kind);
    encode_entity_identity(writer, &finding.entity)?;
    writer.push_bounded_string(&finding.store_path, MAX_INTEGRITY_PATH_BYTES)?;
    encode_phase(writer, finding.phase);
    encode_verifier_family(writer, finding.verifier_family);
    writer.push_bounded_len_prefixed_bytes(&finding.physical_key, MAX_PHYSICAL_KEY_BYTES)?;
    encode_optional_bytes(
        writer,
        finding.primary_key.as_deref(),
        RawDataStoreKey::MAX_STORED_SIZE_USIZE,
    )?;

    push_bounded_count(writer, finding.field_paths.len(), MAX_FINDING_FIELD_PATHS)?;
    for path in &finding.field_paths {
        writer.push_bounded_string(path, MAX_INTEGRITY_PATH_BYTES)?;
    }
    encode_optional_value_path(writer, finding.value_path.as_deref())?;
    writer.push_optional_u32(finding.constraint_id);
    encode_optional_string(
        writer,
        finding.constraint_name.as_deref(),
        MAX_FINDING_TEXT_BYTES,
    )?;
    writer.push_optional_u32(finding.schema_index_id);
    writer.push_optional_u32(finding.relation_id);
    encode_optional_string(writer, finding.expected.as_deref(), MAX_FINDING_TEXT_BYTES)?;
    encode_optional_string(writer, finding.observed.as_deref(), MAX_FINDING_TEXT_BYTES)
}

fn decode_finding(reader: &mut JobReader<'_>) -> Result<IntegrityFinding, InternalError> {
    let diagnostic_code = reader.read_u16()?;
    let class = decode_finding_class(reader)?;
    let severity = decode_severity(reader)?;
    let kind = decode_finding_kind(reader)?;
    let entity = decode_entity_identity(reader)?;
    let store_path = reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?;
    let phase = decode_phase(reader)?;
    let verifier_family = decode_verifier_family(reader)?;
    let physical_key = reader
        .read_bounded_len_prefixed_bytes(MAX_PHYSICAL_KEY_BYTES)?
        .to_vec();
    let primary_key = decode_optional_bytes(reader, RawDataStoreKey::MAX_STORED_SIZE_USIZE)?;

    let field_path_count = reader.read_bounded_count(MAX_FINDING_FIELD_PATHS)?;
    let mut field_paths = Vec::with_capacity(field_path_count);
    for _ in 0..field_path_count {
        field_paths.push(reader.read_bounded_string(MAX_INTEGRITY_PATH_BYTES)?);
    }
    let value_path = decode_optional_value_path(reader)?.map(Box::new);
    let constraint_id = reader.read_optional_u32()?;
    let constraint_name = decode_optional_string(reader, MAX_FINDING_TEXT_BYTES)?;
    let schema_index_id = reader.read_optional_u32()?;
    let relation_id = reader.read_optional_u32()?;
    let expected = decode_optional_string(reader, MAX_FINDING_TEXT_BYTES)?;
    let observed = decode_optional_string(reader, MAX_FINDING_TEXT_BYTES)?;

    Ok(IntegrityFinding {
        diagnostic_code,
        class,
        severity,
        kind,
        entity,
        store_path,
        phase,
        verifier_family,
        physical_key,
        primary_key,
        field_paths,
        value_path,
        constraint_id,
        constraint_name,
        schema_index_id,
        relation_id,
        expected,
        observed,
    })
}

fn encode_finding_class(writer: &mut JobWriter, class: IntegrityFindingClass) {
    writer.push_u8(match class {
        IntegrityFindingClass::Corruption => 1,
        IntegrityFindingClass::IncompatiblePersistedFormat => 2,
        IntegrityFindingClass::ResourceLimited => 3,
    });
}

fn decode_finding_class(
    reader: &mut JobReader<'_>,
) -> Result<IntegrityFindingClass, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityFindingClass::Corruption),
        2 => Ok(IntegrityFindingClass::IncompatiblePersistedFormat),
        3 => Ok(IntegrityFindingClass::ResourceLimited),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_severity(writer: &mut JobWriter, severity: IntegritySeverity) {
    writer.push_u8(match severity {
        IntegritySeverity::Error => 1,
        IntegritySeverity::Advisory => 2,
    });
}

fn decode_severity(reader: &mut JobReader<'_>) -> Result<IntegritySeverity, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegritySeverity::Error),
        2 => Ok(IntegritySeverity::Advisory),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_finding_kind(writer: &mut JobWriter, kind: IntegrityFindingKind) {
    writer.push_u8(match kind {
        IntegrityFindingKind::MalformedDataKey => 1,
        IntegrityFindingKind::MalformedRow => 2,
        IntegrityFindingKind::OversizedRow => 3,
        IntegrityFindingKind::InvalidFieldValue => 4,
        IntegrityFindingKind::PrimaryKeyMismatch => 5,
        IntegrityFindingKind::InvalidIdentityValue => 6,
        IntegrityFindingKind::IdentityHighWaterExceeded => 7,
        IntegrityFindingKind::ConstraintViolation => 8,
        IntegrityFindingKind::MissingIndexEntry => 9,
        IntegrityFindingKind::DivergentIndexEntry => 10,
        IntegrityFindingKind::MalformedIndexEntry => 11,
        IntegrityFindingKind::OrphanIndexEntry => 12,
        IntegrityFindingKind::DuplicateUniqueIndexKey => 13,
        IntegrityFindingKind::MissingRelationTarget => 14,
        IntegrityFindingKind::MissingReverseRelationEntry => 15,
        IntegrityFindingKind::DivergentReverseRelationEntry => 16,
        IntegrityFindingKind::MalformedReverseRelationEntry => 17,
        IntegrityFindingKind::OrphanReverseRelationEntry => 18,
        IntegrityFindingKind::MalformedJournalBatch => 19,
        IntegrityFindingKind::JournalSequenceGap => 20,
        IntegrityFindingKind::DuplicateJournalBatchIdentity => 21,
        IntegrityFindingKind::JournalControlMismatch => 22,
    });
}

fn decode_finding_kind(reader: &mut JobReader<'_>) -> Result<IntegrityFindingKind, InternalError> {
    match reader.read_u8()? {
        1 => Ok(IntegrityFindingKind::MalformedDataKey),
        2 => Ok(IntegrityFindingKind::MalformedRow),
        3 => Ok(IntegrityFindingKind::OversizedRow),
        4 => Ok(IntegrityFindingKind::InvalidFieldValue),
        5 => Ok(IntegrityFindingKind::PrimaryKeyMismatch),
        6 => Ok(IntegrityFindingKind::InvalidIdentityValue),
        7 => Ok(IntegrityFindingKind::IdentityHighWaterExceeded),
        8 => Ok(IntegrityFindingKind::ConstraintViolation),
        9 => Ok(IntegrityFindingKind::MissingIndexEntry),
        10 => Ok(IntegrityFindingKind::DivergentIndexEntry),
        11 => Ok(IntegrityFindingKind::MalformedIndexEntry),
        12 => Ok(IntegrityFindingKind::OrphanIndexEntry),
        13 => Ok(IntegrityFindingKind::DuplicateUniqueIndexKey),
        14 => Ok(IntegrityFindingKind::MissingRelationTarget),
        15 => Ok(IntegrityFindingKind::MissingReverseRelationEntry),
        16 => Ok(IntegrityFindingKind::DivergentReverseRelationEntry),
        17 => Ok(IntegrityFindingKind::MalformedReverseRelationEntry),
        18 => Ok(IntegrityFindingKind::OrphanReverseRelationEntry),
        19 => Ok(IntegrityFindingKind::MalformedJournalBatch),
        20 => Ok(IntegrityFindingKind::JournalSequenceGap),
        21 => Ok(IntegrityFindingKind::DuplicateJournalBatchIdentity),
        22 => Ok(IntegrityFindingKind::JournalControlMismatch),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_bytes(
    writer: &mut JobWriter,
    value: Option<&[u8]>,
    max_bytes: usize,
) -> Result<(), InternalError> {
    match value {
        None => writer.push_u8(0),
        Some(value) => {
            writer.push_u8(1);
            writer.push_bounded_len_prefixed_bytes(value, max_bytes)?;
        }
    }
    Ok(())
}

fn decode_optional_bytes(
    reader: &mut JobReader<'_>,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, InternalError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => reader
            .read_bounded_len_prefixed_bytes(max_bytes)
            .map(|bytes| Some(bytes.to_vec())),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_string(
    writer: &mut JobWriter,
    value: Option<&str>,
    max_bytes: usize,
) -> Result<(), InternalError> {
    match value {
        None => writer.push_u8(0),
        Some(value) => {
            writer.push_u8(1);
            writer.push_bounded_string(value, max_bytes)?;
        }
    }
    Ok(())
}

fn decode_optional_string(
    reader: &mut JobReader<'_>,
    max_bytes: usize,
) -> Result<Option<String>, InternalError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => reader.read_bounded_string(max_bytes).map(Some),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_optional_value_path(
    writer: &mut JobWriter,
    value: Option<&ConstraintValuePath>,
) -> Result<(), InternalError> {
    match value {
        None => writer.push_u8(0),
        Some(path) => {
            writer.push_u8(1);
            push_bounded_count(
                writer,
                path.components().len(),
                MAX_ACCEPTED_TARGET_PATH_COMPONENTS,
            )?;
            for component in path.components() {
                encode_value_path_component(writer, *component);
            }
        }
    }
    Ok(())
}

fn decode_optional_value_path(
    reader: &mut JobReader<'_>,
) -> Result<Option<ConstraintValuePath>, InternalError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => {
            let count = reader.read_bounded_count(MAX_ACCEPTED_TARGET_PATH_COMPONENTS)?;
            let mut components = Vec::with_capacity(count);
            for _ in 0..count {
                components.push(decode_value_path_component(reader)?);
            }
            Ok(Some(ConstraintValuePath::new(components)))
        }
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_value_path_component(writer: &mut JobWriter, component: ConstraintValuePathComponent) {
    match component {
        ConstraintValuePathComponent::RootField { field_id } => {
            writer.push_u8(1);
            writer.push_u32(field_id);
        }
        ConstraintValuePathComponent::RecordMember {
            composite_type_id,
            member_id,
        } => {
            writer.push_u8(2);
            writer.push_u32(composite_type_id);
            writer.push_u32(member_id);
        }
        ConstraintValuePathComponent::TupleElement {
            composite_type_id,
            ordinal,
        } => {
            writer.push_u8(3);
            writer.push_u32(composite_type_id);
            writer.push_u32(ordinal);
        }
        ConstraintValuePathComponent::Newtype { composite_type_id } => {
            writer.push_u8(4);
            writer.push_u32(composite_type_id);
        }
        ConstraintValuePathComponent::EnumVariant {
            enum_type_id,
            variant_id,
        } => {
            writer.push_u8(5);
            writer.push_u32(enum_type_id);
            writer.push_u32(variant_id);
        }
        ConstraintValuePathComponent::ListElement { index } => {
            writer.push_u8(6);
            writer.push_u32(index);
        }
        ConstraintValuePathComponent::SetElement { index } => {
            writer.push_u8(7);
            writer.push_u32(index);
        }
        ConstraintValuePathComponent::MapEntryKey { index } => {
            writer.push_u8(8);
            writer.push_u32(index);
        }
        ConstraintValuePathComponent::MapEntryValue { index } => {
            writer.push_u8(9);
            writer.push_u32(index);
        }
    }
}

fn decode_value_path_component(
    reader: &mut JobReader<'_>,
) -> Result<ConstraintValuePathComponent, InternalError> {
    match reader.read_u8()? {
        1 => Ok(ConstraintValuePathComponent::RootField {
            field_id: reader.read_u32()?,
        }),
        2 => Ok(ConstraintValuePathComponent::RecordMember {
            composite_type_id: reader.read_u32()?,
            member_id: reader.read_u32()?,
        }),
        3 => Ok(ConstraintValuePathComponent::TupleElement {
            composite_type_id: reader.read_u32()?,
            ordinal: reader.read_u32()?,
        }),
        4 => Ok(ConstraintValuePathComponent::Newtype {
            composite_type_id: reader.read_u32()?,
        }),
        5 => Ok(ConstraintValuePathComponent::EnumVariant {
            enum_type_id: reader.read_u32()?,
            variant_id: reader.read_u32()?,
        }),
        6 => Ok(ConstraintValuePathComponent::ListElement {
            index: reader.read_u32()?,
        }),
        7 => Ok(ConstraintValuePathComponent::SetElement {
            index: reader.read_u32()?,
        }),
        8 => Ok(ConstraintValuePathComponent::MapEntryKey {
            index: reader.read_u32()?,
        }),
        9 => Ok(ConstraintValuePathComponent::MapEntryValue {
            index: reader.read_u32()?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn push_bounded_count(
    writer: &mut JobWriter,
    count: usize,
    max: usize,
) -> Result<(), InternalError> {
    if count > max {
        return Err(InternalError::store_unsupported());
    }
    writer.push_len(count)
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
    use crate::error::{ConstraintValuePath, ConstraintValuePathComponent};
    use sha2::{Digest, Sha256};

    #[test]
    fn current_job_payload_is_direct_bounded_and_has_a_fixed_golden_vector() {
        let job = current_job_codec_fixture();
        let payload = encode_integrity_job_payload(&job).expect("current payload should encode");

        assert!(!payload.starts_with(b"DIDL"));
        assert_eq!(
            decode_integrity_job_payload(&payload).expect("current payload should decode"),
            job,
        );
        assert_eq!(payload.len(), 476);
        assert_eq!(
            Sha256::digest(&payload).as_slice(),
            [
                142, 240, 95, 129, 56, 238, 194, 34, 201, 84, 64, 178, 177, 33, 48, 112, 227, 132,
                247, 132, 21, 10, 97, 68, 208, 178, 113, 237, 151, 128, 35, 246,
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
        assert!(decode_integrity_job_payload(&payload[..payload.len() - 1]).is_err());

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

        let payload = encode_integrity_job_payload(&job).expect("terminal job should encode");
        assert_eq!(
            decode_integrity_job_payload(&payload).expect("terminal job should decode"),
            job,
        );
    }
}
