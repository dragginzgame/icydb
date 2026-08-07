//! Module: db::resumable_job
//! Responsibility: bounded application-owned resumable job state and receipts.
//! Does not own: application authorization, accumulator meaning, or page planning.
//! Boundary: compare-proof-and-advance session API -> excluded progress storage.

use crate::db::{
    ReadSetRevisionError, ReadSetRevisionProof, ReadSetStoreIdentity, ReadSetStoreRevision,
};
use candid::CandidType;
use serde::Deserialize;
use std::{error::Error as StdError, fmt};

/// Maximum retained application accumulator/state bytes per job.
pub const MAX_RESUMABLE_JOB_STATE_BYTES: usize = 256 * 1024;
/// Maximum retained application receipt bytes per committed request.
pub const MAX_RESUMABLE_JOB_RECEIPT_BYTES: usize = 64 * 1024;
/// Maximum UTF-8 bytes in one application idempotency key.
pub const MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES: usize = 256;
/// Maximum UTF-8 bytes in one retained opaque continuation.
pub const MAX_RESUMABLE_JOB_CONTINUATION_BYTES: usize = 16 * 1024;

/// Nonzero application-owned identity for one durable resumable job.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResumableJobId([u8; 32]);

impl ResumableJobId {
    /// Admit one nonzero application-owned job identity.
    pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, ResumableJobError> {
        if bytes == [0; 32] {
            return Err(ResumableJobError::InvalidJobId);
        }
        Ok(Self(bytes))
    }

    /// Return the application-owned identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Bounded application request identity used for lost-response replay.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ResumableJobIdempotencyKey(String);

impl ResumableJobIdempotencyKey {
    /// Admit one nonempty bounded UTF-8 idempotency key.
    pub fn new(value: impl Into<String>) -> Result<Self, ResumableJobError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES {
            return Err(ResumableJobError::InvalidIdempotencyKey);
        }
        Ok(Self(value))
    }

    /// Borrow the application key.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(in crate::db) const fn validate(&self) -> Result<(), ResumableJobError> {
        if self.0.is_empty() || self.0.len() > MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES {
            return Err(ResumableJobError::InvalidIdempotencyKey);
        }
        Ok(())
    }
}

/// Durable lifecycle of one generic application job.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ResumableJobStatus {
    /// The next expected sequence may advance.
    Active,
    /// Exhaustion committed and only replay or acknowledgement remains.
    Completed,
    /// Protected source authority changed and the job must restart.
    Invalidated,
}

/// Current bounded durable state of one application-owned job.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ResumableJobState {
    /// Application-owned job identity.
    pub job_id: ResumableJobId,
    /// Next sequence expected by compare-proof-and-advance.
    pub sequence: u64,
    /// Current job lifecycle.
    pub status: ResumableJobStatus,
    /// Complete immutable protected-source proof.
    pub proof: ReadSetRevisionProof,
    /// Opaque page continuation retained after the last successful advance.
    pub continuation: Option<String>,
    /// Application-defined bounded accumulator or phase state.
    pub application_state: Vec<u8>,
}

/// Identity and expected sequence for one idempotent advance request.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ResumableJobAdvanceRequest {
    /// Target application job.
    pub job_id: ResumableJobId,
    /// Exact sequence observed before issuing the request.
    pub expected_sequence: u64,
    /// Stable application identity reused after a lost reply.
    pub idempotency_key: ResumableJobIdempotencyKey,
}

impl ResumableJobAdvanceRequest {
    /// Construct one advance request from already admitted identities.
    #[must_use]
    pub const fn new(
        job_id: ResumableJobId,
        expected_sequence: u64,
        idempotency_key: ResumableJobIdempotencyKey,
    ) -> Self {
        Self {
            job_id,
            expected_sequence,
            idempotency_key,
        }
    }
}

/// Bounded temporary next state produced by one synchronous page operation.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ResumableJobAdvance {
    /// Opaque continuation for the next page, or `None` after exhaustion.
    pub continuation: Option<String>,
    /// Complete next application accumulator or phase state.
    pub application_state: Vec<u8>,
    /// Bounded application receipt returned and retained for replay.
    pub application_receipt: Vec<u8>,
}

impl ResumableJobAdvance {
    /// Admit one bounded candidate state and receipt.
    pub fn new(
        continuation: Option<String>,
        application_state: Vec<u8>,
        application_receipt: Vec<u8>,
    ) -> Result<Self, ResumableJobError> {
        let advance = Self {
            continuation,
            application_state,
            application_receipt,
        };
        advance.validate()?;
        Ok(advance)
    }

    pub(in crate::db) fn validate(&self) -> Result<(), ResumableJobError> {
        validate_continuation(self.continuation.as_deref())?;
        if self.application_state.len() > MAX_RESUMABLE_JOB_STATE_BYTES
            || self.application_receipt.len() > MAX_RESUMABLE_JOB_RECEIPT_BYTES
        {
            return Err(ResumableJobError::PayloadTooLarge);
        }
        Ok(())
    }
}

/// Outcome committed for one advance request.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum ResumableJobAdvanceStatus {
    /// Candidate continuation and application state committed.
    Advanced,
    /// Source drift discarded the candidate and invalidated the job.
    Invalidated,
}

/// Replayable receipt for one committed advance request.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ResumableJobAdvanceReceipt {
    /// Sequence named by the request.
    pub request_sequence: u64,
    /// Durable job sequence after this receipt committed.
    pub committed_sequence: u64,
    /// Whether next state or invalidation committed.
    pub status: ResumableJobAdvanceStatus,
    /// Committed next continuation, when advanced.
    pub continuation: Option<String>,
    /// Application-defined replay payload.
    pub application_receipt: Vec<u8>,
    idempotency_key: ResumableJobIdempotencyKey,
}

impl ResumableJobAdvanceReceipt {
    /// Borrow the application request identity retained for replay.
    #[must_use]
    pub const fn idempotency_key(&self) -> &ResumableJobIdempotencyKey {
        &self.idempotency_key
    }
}

/// Typed protocol or persistence failure for generic resumable jobs.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum ResumableJobError {
    /// Job identity was all zeroes.
    InvalidJobId,
    /// Idempotency key was empty or exceeded its byte bound.
    InvalidIdempotencyKey,
    /// State, continuation, or receipt exceeded its bound.
    PayloadTooLarge,
    /// A job with the same application identity already exists.
    AlreadyExists,
    /// The requested job does not exist.
    NotFound,
    /// The request did not name the job's current sequence.
    StaleSequence { expected: u64, actual: u64 },
    /// A non-replay request targeted an invalidated job.
    Invalidated,
    /// A non-replay request targeted a completed job.
    Completed,
    /// Acknowledgement targeted an active job with remaining traversal work.
    NotTerminal,
    /// Protected source authority was invalid or unsupported.
    SourceProof(ReadSetRevisionError),
    /// The shared excluded progress store reached a hard capacity.
    CapacityExceeded,
    /// Retained progress bytes or state closure were corrupt.
    CorruptProgressStore,
    /// Retained progress bytes use an unsupported current format.
    IncompatibleProgressFormat,
    /// An internal database invariant prevented the operation.
    Internal,
    /// The enclosing request exhausted aggregate IcyDB work allowance.
    ExecutionBudgetExceeded {
        resource: u64,
        limit: u64,
        observed: u64,
        scope: u64,
        lane: u64,
        normalized_shape_fingerprint_prefix: u64,
    },
}

impl fmt::Display for ResumableJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("resumable job operation failed")
    }
}

impl StdError for ResumableJobError {}

impl From<ReadSetRevisionError> for ResumableJobError {
    fn from(error: ReadSetRevisionError) -> Self {
        Self::SourceProof(error)
    }
}

/// Failure from protocol handling or the application page closure.
#[derive(Debug)]
pub enum CompareProofAndAdvanceError<E> {
    /// IcyDB rejected proof, sequence, bounds, or progress persistence.
    Protocol(ResumableJobError),
    /// The application page closure returned its own failure.
    Operation(E),
}

impl<E: fmt::Display> fmt::Display for CompareProofAndAdvanceError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(formatter),
            Self::Operation(error) => error.fmt(formatter),
        }
    }
}

impl<E: StdError + 'static> StdError for CompareProofAndAdvanceError<E> {}

impl<E> From<ResumableJobError> for CompareProofAndAdvanceError<E> {
    fn from(error: ResumableJobError) -> Self {
        Self::Protocol(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ResumableJobRecord {
    state: ResumableJobState,
    last_receipt: Option<ResumableJobAdvanceReceipt>,
}

impl ResumableJobRecord {
    pub(in crate::db) fn new(
        job_id: ResumableJobId,
        proof: ReadSetRevisionProof,
        application_state: Vec<u8>,
    ) -> Result<Self, ResumableJobError> {
        let state = ResumableJobState {
            job_id,
            sequence: 0,
            status: ResumableJobStatus::Active,
            proof,
            continuation: None,
            application_state,
        };
        let record = Self {
            state,
            last_receipt: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub(in crate::db) const fn state(&self) -> &ResumableJobState {
        &self.state
    }

    pub(in crate::db) const fn last_receipt(&self) -> Option<&ResumableJobAdvanceReceipt> {
        self.last_receipt.as_ref()
    }

    pub(in crate::db) fn apply_advance(
        &self,
        request: &ResumableJobAdvanceRequest,
        advance: ResumableJobAdvance,
    ) -> Result<(Self, ResumableJobAdvanceReceipt), ResumableJobError> {
        advance.validate()?;
        let committed_sequence = self
            .state
            .sequence
            .checked_add(1)
            .ok_or(ResumableJobError::CapacityExceeded)?;
        let receipt = ResumableJobAdvanceReceipt {
            request_sequence: request.expected_sequence,
            committed_sequence,
            status: ResumableJobAdvanceStatus::Advanced,
            continuation: advance.continuation.clone(),
            application_receipt: advance.application_receipt,
            idempotency_key: request.idempotency_key.clone(),
        };
        let record = Self {
            state: ResumableJobState {
                job_id: self.state.job_id,
                sequence: committed_sequence,
                status: if advance.continuation.is_some() {
                    ResumableJobStatus::Active
                } else {
                    ResumableJobStatus::Completed
                },
                proof: self.state.proof.clone(),
                continuation: advance.continuation,
                application_state: advance.application_state,
            },
            last_receipt: Some(receipt.clone()),
        };
        record.validate()?;
        Ok((record, receipt))
    }

    pub(in crate::db) fn invalidate(
        &self,
        request: &ResumableJobAdvanceRequest,
    ) -> Result<(Self, ResumableJobAdvanceReceipt), ResumableJobError> {
        let committed_sequence = self
            .state
            .sequence
            .checked_add(1)
            .ok_or(ResumableJobError::CapacityExceeded)?;
        let receipt = ResumableJobAdvanceReceipt {
            request_sequence: request.expected_sequence,
            committed_sequence,
            status: ResumableJobAdvanceStatus::Invalidated,
            continuation: None,
            application_receipt: Vec::new(),
            idempotency_key: request.idempotency_key.clone(),
        };
        let record = Self {
            state: ResumableJobState {
                sequence: committed_sequence,
                status: ResumableJobStatus::Invalidated,
                continuation: None,
                ..self.state.clone()
            },
            last_receipt: Some(receipt.clone()),
        };
        record.validate()?;
        Ok((record, receipt))
    }

    pub(in crate::db) fn validate(&self) -> Result<(), ResumableJobError> {
        if self.state.job_id.to_bytes() == [0; 32] {
            return Err(ResumableJobError::InvalidJobId);
        }
        self.state.proof.validate()?;
        validate_continuation(self.state.continuation.as_deref())?;
        if self.state.application_state.len() > MAX_RESUMABLE_JOB_STATE_BYTES {
            return Err(ResumableJobError::PayloadTooLarge);
        }
        if let Some(receipt) = &self.last_receipt {
            receipt.idempotency_key.validate()?;
            validate_continuation(receipt.continuation.as_deref())?;
            let state_matches_receipt = match (self.state.status, receipt.status) {
                (
                    ResumableJobStatus::Active | ResumableJobStatus::Completed,
                    ResumableJobAdvanceStatus::Advanced,
                ) => self.state.continuation == receipt.continuation,
                (ResumableJobStatus::Invalidated, ResumableJobAdvanceStatus::Invalidated) => {
                    self.state.continuation.is_none() && receipt.continuation.is_none()
                }
                _ => false,
            };
            if receipt.application_receipt.len() > MAX_RESUMABLE_JOB_RECEIPT_BYTES
                || receipt.committed_sequence != self.state.sequence
                || receipt.request_sequence.checked_add(1) != Some(receipt.committed_sequence)
                || !state_matches_receipt
            {
                return Err(ResumableJobError::CorruptProgressStore);
            }
        } else if self.state.sequence != 0
            || self.state.status != ResumableJobStatus::Active
            || self.state.continuation.is_some()
        {
            return Err(ResumableJobError::CorruptProgressStore);
        }
        Ok(())
    }
}

fn validate_continuation(continuation: Option<&str>) -> Result<(), ResumableJobError> {
    if continuation.is_some_and(|value| value.len() > MAX_RESUMABLE_JOB_CONTINUATION_BYTES) {
        return Err(ResumableJobError::PayloadTooLarge);
    }
    Ok(())
}

pub(in crate::db) fn encode_resumable_job_payload(
    record: &ResumableJobRecord,
) -> Result<Vec<u8>, ResumableJobError> {
    record.validate()?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&record.state.job_id.to_bytes());
    bytes.extend_from_slice(&record.state.sequence.to_be_bytes());
    bytes.push(match record.state.status {
        ResumableJobStatus::Active => 0,
        ResumableJobStatus::Invalidated => 1,
        ResumableJobStatus::Completed => 2,
    });
    write_proof(&mut bytes, &record.state.proof)?;
    write_optional_string(&mut bytes, record.state.continuation.as_deref())?;
    write_bytes(&mut bytes, &record.state.application_state)?;
    match &record.last_receipt {
        None => bytes.push(0),
        Some(receipt) => {
            bytes.push(1);
            bytes.extend_from_slice(&receipt.request_sequence.to_be_bytes());
            bytes.extend_from_slice(&receipt.committed_sequence.to_be_bytes());
            bytes.push(match receipt.status {
                ResumableJobAdvanceStatus::Advanced => 0,
                ResumableJobAdvanceStatus::Invalidated => 1,
            });
            write_string(&mut bytes, receipt.idempotency_key.as_str())?;
            write_optional_string(&mut bytes, receipt.continuation.as_deref())?;
            write_bytes(&mut bytes, &receipt.application_receipt)?;
        }
    }
    Ok(bytes)
}

pub(in crate::db) fn decode_resumable_job_payload(
    bytes: &[u8],
) -> Result<ResumableJobRecord, ResumableJobError> {
    let mut reader = Reader::new(bytes);
    let job_id = ResumableJobId::try_from_bytes(reader.array()?)?;
    let sequence = reader.u64()?;
    let status = match reader.u8()? {
        0 => ResumableJobStatus::Active,
        1 => ResumableJobStatus::Invalidated,
        2 => ResumableJobStatus::Completed,
        _ => return Err(ResumableJobError::CorruptProgressStore),
    };
    let proof = read_proof(&mut reader)?;
    let continuation = read_optional_string(&mut reader, MAX_RESUMABLE_JOB_CONTINUATION_BYTES)?;
    let application_state = reader.bytes(MAX_RESUMABLE_JOB_STATE_BYTES)?.to_vec();
    let last_receipt = match reader.u8()? {
        0 => None,
        1 => {
            let request_sequence = reader.u64()?;
            let committed_sequence = reader.u64()?;
            let receipt_status = match reader.u8()? {
                0 => ResumableJobAdvanceStatus::Advanced,
                1 => ResumableJobAdvanceStatus::Invalidated,
                _ => return Err(ResumableJobError::CorruptProgressStore),
            };
            let idempotency_key = ResumableJobIdempotencyKey::new(
                reader.string(MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES)?,
            )?;
            let receipt_continuation =
                read_optional_string(&mut reader, MAX_RESUMABLE_JOB_CONTINUATION_BYTES)?;
            let application_receipt = reader.bytes(MAX_RESUMABLE_JOB_RECEIPT_BYTES)?.to_vec();
            Some(ResumableJobAdvanceReceipt {
                request_sequence,
                committed_sequence,
                status: receipt_status,
                continuation: receipt_continuation,
                application_receipt,
                idempotency_key,
            })
        }
        _ => return Err(ResumableJobError::CorruptProgressStore),
    };
    if !reader.is_empty() {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    let record = ResumableJobRecord {
        state: ResumableJobState {
            job_id,
            sequence,
            status,
            proof,
            continuation,
            application_state,
        },
        last_receipt,
    };
    record.validate()?;
    Ok(record)
}

fn write_proof(bytes: &mut Vec<u8>, proof: &ReadSetRevisionProof) -> Result<(), ResumableJobError> {
    proof.validate()?;
    bytes.extend_from_slice(&proof.database_incarnation());
    bytes.extend_from_slice(&proof.accepted_root_revision().to_be_bytes());
    bytes.push(proof.accepted_root_fingerprint_method());
    bytes.extend_from_slice(&proof.accepted_root_fingerprint());
    let count =
        u32::try_from(proof.stores().len()).map_err(|_| ResumableJobError::PayloadTooLarge)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for store in proof.stores() {
        bytes.extend_from_slice(&store.store().to_bytes());
        bytes.extend_from_slice(&store.data_revision().to_be_bytes());
        bytes.extend_from_slice(&store.access_state_revision().to_be_bytes());
    }
    Ok(())
}

fn read_proof(reader: &mut Reader<'_>) -> Result<ReadSetRevisionProof, ResumableJobError> {
    let database_incarnation = reader.array()?;
    let accepted_root_revision = reader.u64()?;
    let accepted_root_fingerprint_method = reader.u8()?;
    let accepted_root_fingerprint = reader.array()?;
    let count = reader.u32()? as usize;
    if count == 0 || count > crate::db::MAX_READ_SET_PROOF_STORES {
        return Err(ResumableJobError::CorruptProgressStore);
    }
    let mut stores = Vec::with_capacity(count);
    for _ in 0..count {
        stores.push(ReadSetStoreRevision::new(
            ReadSetStoreIdentity::from_bytes(reader.array()?),
            reader.u64()?,
            reader.u64()?,
        ));
    }
    ReadSetRevisionProof::from_parts(
        database_incarnation,
        accepted_root_revision,
        accepted_root_fingerprint_method,
        accepted_root_fingerprint,
        stores,
    )
    .map_err(Into::into)
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), ResumableJobError> {
    write_bytes(bytes, value.as_bytes())
}

fn write_optional_string(
    bytes: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), ResumableJobError> {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            write_string(bytes, value)?;
        }
    }
    Ok(())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), ResumableJobError> {
    let len = u32::try_from(value.len()).map_err(|_| ResumableJobError::PayloadTooLarge)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_optional_string(
    reader: &mut Reader<'_>,
    max: usize,
) -> Result<Option<String>, ResumableJobError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => reader.string(max).map(Some),
        _ => Err(ResumableJobError::CorruptProgressStore),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, ResumableJobError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(ResumableJobError::CorruptProgressStore)?;
        self.offset += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, ResumableJobError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ResumableJobError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ResumableJobError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ResumableJobError::CorruptProgressStore)
    }

    fn bytes(&mut self, max: usize) -> Result<&'a [u8], ResumableJobError> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(ResumableJobError::CorruptProgressStore);
        }
        self.take(len)
    }

    fn string(&mut self, max: usize) -> Result<String, ResumableJobError> {
        let bytes = self.bytes(max)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| ResumableJobError::CorruptProgressStore)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ResumableJobError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ResumableJobError::CorruptProgressStore)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(ResumableJobError::CorruptProgressStore)?;
        self.offset = end;
        Ok(bytes)
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proof() -> ReadSetRevisionProof {
        ReadSetRevisionProof::from_parts(
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
        .expect("bounded canonical proof should admit")
    }

    fn job_id() -> ResumableJobId {
        ResumableJobId::try_from_bytes([4; 32]).expect("nonzero job identity should admit")
    }

    #[test]
    fn current_resumable_job_payload_round_trips_state_and_replay_receipt() {
        let record = ResumableJobRecord::new(job_id(), proof(), vec![1, 2, 3])
            .expect("initial resumable record should admit");
        let request = ResumableJobAdvanceRequest::new(
            job_id(),
            0,
            ResumableJobIdempotencyKey::new("page-0")
                .expect("bounded idempotency key should admit"),
        );
        let advance = ResumableJobAdvance::new(
            Some("opaque-continuation".to_string()),
            vec![4, 5],
            vec![6, 7],
        )
        .expect("bounded advance should admit");
        let (advanced, _) = record
            .apply_advance(&request, advance)
            .expect("current request should advance");

        let bytes = encode_resumable_job_payload(&advanced)
            .expect("current resumable payload should encode");
        assert!(!bytes.starts_with(b"DIDL"));
        assert_eq!(
            decode_resumable_job_payload(&bytes).expect("current resumable payload should decode"),
            advanced,
        );
    }

    #[test]
    fn resumable_job_payload_rejects_truncation_and_trailing_bytes() {
        let record = ResumableJobRecord::new(job_id(), proof(), Vec::new())
            .expect("initial resumable record should admit");
        let bytes =
            encode_resumable_job_payload(&record).expect("current resumable payload should encode");

        assert_eq!(
            decode_resumable_job_payload(&bytes[..bytes.len() - 1]),
            Err(ResumableJobError::CorruptProgressStore),
        );
        let mut trailing = bytes;
        trailing.push(0);
        assert_eq!(
            decode_resumable_job_payload(&trailing),
            Err(ResumableJobError::CorruptProgressStore),
        );
    }
}
