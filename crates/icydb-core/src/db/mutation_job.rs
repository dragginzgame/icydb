//! Module: db::mutation_job
//! Responsibility: bounded durable mutation-job lifecycle, replay, and current payload codec.
//! Does not own: SQL lowering, target traversal, row mutation, or commit-marker recovery.
//! Boundary: trusted mutation coordinator -> excluded progress-record envelope.

#[cfg(feature = "sql")]
mod intent;

use candid::CandidType;
use serde::Deserialize;
use std::{error::Error as StdError, fmt};

#[cfg(feature = "sql")]
pub(in crate::db) use intent::CanonicalMutationIntent;

/// Maximum UTF-8 bytes in one mutation-job idempotency key.
pub const MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES: usize = 256;
/// Maximum current engine-continuation bytes retained by one mutation job.
pub const MAX_MUTATION_JOB_CONTINUATION_BYTES: usize = 2 * 1024;
/// Maximum canonical accepted-intent bytes retained by one mutation job.
pub const MAX_MUTATION_JOB_INTENT_BYTES: usize = 16 * 1024;
/// Maximum encoded replay-receipt bytes retained by one mutation job.
pub const MAX_MUTATION_JOB_RECEIPT_BYTES: usize = 8 * 1024;
/// Maximum complete encoded mutation-job record, including its storage envelope.
pub const MAX_MUTATION_JOB_RECORD_BYTES: usize = 64 * 1024;

/// Maximum authoritative keys examined by one mutation-job advance.
pub const MAX_MUTATION_JOB_STEP_KEYS_SCANNED: u64 = 208;
/// Maximum target rows changed by one mutation-job advance.
pub const MAX_MUTATION_JOB_STEP_ROWS_UPDATED: u64 = 56;

/// Nonzero application-owned identity for one durable mutation job incarnation.
///
/// An application must allocate a fresh identity for every logical job. An
/// identity is never reusable after cancellation, acknowledgement, failure,
/// or an absent-record response.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MutationJobId([u8; 32]);

impl MutationJobId {
    /// Admit one nonzero application-owned identity.
    pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, MutationJobError> {
        if bytes == [0; 32] {
            return Err(MutationJobError::InvalidJobId);
        }
        Ok(Self(bytes))
    }

    /// Return the application-owned identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    pub(in crate::db) fn validate(self) -> Result<(), MutationJobError> {
        if self.0 == [0; 32] {
            return Err(MutationJobError::InvalidJobId);
        }
        Ok(())
    }
}

/// Bounded request identity reused exactly after a lost response.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MutationJobIdempotencyKey(String);

impl MutationJobIdempotencyKey {
    /// Admit one nonempty bounded UTF-8 request identity.
    pub fn new(value: impl Into<String>) -> Result<Self, MutationJobError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES {
            return Err(MutationJobError::InvalidIdempotencyKey);
        }
        Ok(Self(value))
    }

    /// Borrow the request identity.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }

    const fn validate(&self) -> Result<(), MutationJobError> {
        if self.0.is_empty() || self.0.len() > MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES {
            return Err(MutationJobError::InvalidIdempotencyKey);
        }
        Ok(())
    }
}

/// Terminal reason why a valid retained mutation job cannot continue safely.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum MutationJobRestartReason {
    /// The accepted schema identity changed after the job started.
    AcceptedSchemaChanged,
    /// The target allocation or database incarnation changed.
    TargetAllocationChanged,
    /// The frozen canonical intent is no longer eligible.
    IntentIneligible,
    /// The engine-owned batch-policy identity changed.
    BatchPolicyChanged,
    /// The retained current record names an unsupported internal continuation.
    UnsupportedContinuation,
    /// The current managed-write time would move a target row backward.
    ManagedTimestampRegression,
    /// One valid mutation candidate cannot fit the current fixed page policy.
    CandidateExceedsBatchPolicy,
}

/// Durable lifecycle of one mutation job.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum MutationJobStatus {
    /// One bounded Forward or Verify step may still run.
    Active,
    /// Stable clean Verify exhaustion committed.
    Completed,
    /// Authority or policy drift requires a new job.
    RestartRequired(MutationJobRestartReason),
}

/// Current convergence phase of one mutation job.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum MutationJobPhase {
    /// Scan authoritative keys and converge stale rows.
    Forward,
    /// Prove a clean scan at one unchanged durable target revision.
    Verify,
}

/// Public bounded state for one retained mutation job.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MutationJobState {
    /// Application-owned job identity.
    pub job_id: MutationJobId,
    /// Sequence expected by the next non-replay advance.
    pub sequence: u64,
    /// Current durable lifecycle.
    pub status: MutationJobStatus,
    /// Current convergence phase.
    pub phase: MutationJobPhase,
    /// Authoritative keys examined across committed advances.
    pub keys_scanned_total: u64,
    /// Rows changed across committed advances.
    pub rows_updated_total: u64,
    /// Verify passes restarted because stable convergence was not proven.
    pub verify_restarts_total: u64,
}

/// Identity and expected sequence for one idempotent bounded advance.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MutationJobAdvanceRequest {
    /// Target mutation job.
    pub job_id: MutationJobId,
    /// Exact sequence observed before issuing this request.
    pub expected_sequence: u64,
    /// Stable request identity reused after a lost reply.
    pub idempotency_key: MutationJobIdempotencyKey,
}

impl MutationJobAdvanceRequest {
    /// Construct one request from already admitted identities.
    #[must_use]
    pub const fn new(
        job_id: MutationJobId,
        expected_sequence: u64,
        idempotency_key: MutationJobIdempotencyKey,
    ) -> Self {
        Self {
            job_id,
            expected_sequence,
            idempotency_key,
        }
    }

    fn validate(&self) -> Result<(), MutationJobError> {
        self.job_id.validate()?;
        self.idempotency_key.validate()
    }
}

/// Replayable result of one committed bounded advance.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MutationJobAdvanceReceipt {
    /// Sequence named by the request.
    pub request_sequence: u64,
    /// Durable sequence after the advance committed.
    pub committed_sequence: u64,
    /// Durable lifecycle after the advance committed.
    pub status: MutationJobStatus,
    /// Durable convergence phase after the advance committed.
    pub phase: MutationJobPhase,
    /// Authoritative keys examined by this advance.
    pub keys_scanned: u64,
    /// Rows changed by this advance.
    pub rows_updated: u64,
    /// Authoritative keys examined across committed advances.
    pub keys_scanned_total: u64,
    /// Rows changed across committed advances.
    pub rows_updated_total: u64,
    /// Verify restarts across committed advances.
    pub verify_restarts_total: u64,
}

/// Variable-sized component constrained by the durable record protocol.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum MutationJobPayloadKind {
    /// Canonical accepted mutation intent.
    Intent,
    /// Private engine continuation.
    Continuation,
    /// Retained replay receipt and request identity.
    Receipt,
    /// Complete progress-store record envelope.
    Record,
}

/// Typed mutation-job protocol, lifecycle, or persistence failure.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum MutationJobError {
    /// Job identity was all zeroes.
    InvalidJobId,
    /// Idempotency key was empty or exceeded its byte bound.
    InvalidIdempotencyKey,
    /// A retained job id was reused for different canonical meaning.
    IdentityConflict,
    /// The requested job does not exist.
    NotFound,
    /// The request did not name the job's current sequence.
    StaleSequence { expected: u64, actual: u64 },
    /// Acknowledgement targeted an active job.
    Active,
    /// A non-replay advance targeted a completed job.
    Completed,
    /// A non-replay advance targeted a job that must restart.
    RestartRequired(MutationJobRestartReason),
    /// A bounded persisted component exceeded its engine-owned limit.
    PayloadTooLarge {
        kind: MutationJobPayloadKind,
        limit: u64,
        observed: u64,
    },
    /// Current accepted authority no longer matches the frozen intent.
    AuthorityMismatch,
    /// The requested mutation cannot be represented by the fixed-intent engine.
    IneligibleIntent,
    /// The shared excluded progress store reached its hard capacity.
    CapacityExceeded,
    /// A checked sequence or cumulative counter would overflow.
    CounterOverflow,
    /// Retained progress bytes or their state closure were corrupt.
    CorruptProgressStore,
    /// Retained progress bytes use an unsupported format version.
    IncompatibleProgressFormat,
    /// Commit or recovery evidence cannot prove one exact transition.
    CommitCorruption,
    /// Target mutation execution failed before progress committed.
    TargetMutationFailed,
    /// Target traversal failed before progress committed.
    TargetQueryFailed,
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

impl fmt::Display for MutationJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("mutation job operation failed")
    }
}

impl StdError for MutationJobError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RetainedMutationJobReceipt {
    receipt: MutationJobAdvanceReceipt,
    idempotency_key: MutationJobIdempotencyKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MutationJobRecord {
    state: MutationJobState,
    canonical_intent: Vec<u8>,
    engine_continuation: Vec<u8>,
    last_receipt: Option<RetainedMutationJobReceipt>,
}

impl MutationJobRecord {
    pub(in crate::db) fn new(
        job_id: MutationJobId,
        canonical_intent: Vec<u8>,
        engine_continuation: Vec<u8>,
    ) -> Result<Self, MutationJobError> {
        let record = Self {
            state: MutationJobState {
                job_id,
                sequence: 0,
                status: MutationJobStatus::Active,
                phase: MutationJobPhase::Forward,
                keys_scanned_total: 0,
                rows_updated_total: 0,
                verify_restarts_total: 0,
            },
            canonical_intent,
            engine_continuation,
            last_receipt: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub(in crate::db) const fn state(&self) -> &MutationJobState {
        &self.state
    }

    pub(in crate::db) const fn canonical_intent(&self) -> &[u8] {
        self.canonical_intent.as_slice()
    }

    pub(in crate::db) const fn engine_continuation(&self) -> &[u8] {
        self.engine_continuation.as_slice()
    }

    /// Prove that cancellation can remove only the exact initial record.
    pub(in crate::db) fn ensure_cancelable_at_sequence(
        &self,
        expected_sequence: u64,
    ) -> Result<&[u8], MutationJobError> {
        self.validate()?;
        if self.state.sequence != expected_sequence {
            return Err(MutationJobError::StaleSequence {
                expected: expected_sequence,
                actual: self.state.sequence,
            });
        }
        if self.state.sequence != 0 {
            return Err(MutationJobError::StaleSequence {
                expected: 0,
                actual: self.state.sequence,
            });
        }
        if self.state.status != MutationJobStatus::Active
            || self.state.phase != MutationJobPhase::Forward
            || self.state.keys_scanned_total != 0
            || self.state.rows_updated_total != 0
            || self.state.verify_restarts_total != 0
            || self.last_receipt.is_some()
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(self.engine_continuation())
    }

    pub(in crate::db) fn exact_replay(
        &self,
        request: &MutationJobAdvanceRequest,
    ) -> Result<Option<&MutationJobAdvanceReceipt>, MutationJobError> {
        self.validate()?;
        request.validate()?;
        if request.job_id != self.state.job_id {
            return Err(MutationJobError::NotFound);
        }
        Ok(self.last_receipt.as_ref().and_then(|retained| {
            (retained.receipt.request_sequence == request.expected_sequence
                && retained.idempotency_key == request.idempotency_key)
                .then_some(&retained.receipt)
        }))
    }

    pub(in crate::db) fn ensure_can_advance(
        &self,
        request: &MutationJobAdvanceRequest,
    ) -> Result<(), MutationJobError> {
        self.validate()?;
        request.validate()?;
        if request.job_id != self.state.job_id {
            return Err(MutationJobError::NotFound);
        }
        if self.state.sequence != request.expected_sequence {
            return Err(MutationJobError::StaleSequence {
                expected: request.expected_sequence,
                actual: self.state.sequence,
            });
        }
        match self.state.status {
            MutationJobStatus::Active => Ok(()),
            MutationJobStatus::Completed => Err(MutationJobError::Completed),
            MutationJobStatus::RestartRequired(reason) => {
                Err(MutationJobError::RestartRequired(reason))
            }
        }
    }

    pub(in crate::db) fn apply_transition(
        &self,
        request: &MutationJobAdvanceRequest,
        transition: MutationJobTransition,
    ) -> Result<(Self, MutationJobAdvanceReceipt), MutationJobError> {
        self.ensure_can_advance(request)?;
        transition.validate(self.state.phase)?;
        let committed_sequence = self
            .state
            .sequence
            .checked_add(1)
            .ok_or(MutationJobError::CounterOverflow)?;
        let keys_scanned_total = self
            .state
            .keys_scanned_total
            .checked_add(transition.keys_scanned)
            .ok_or(MutationJobError::CounterOverflow)?;
        let rows_updated_total = self
            .state
            .rows_updated_total
            .checked_add(transition.rows_updated)
            .ok_or(MutationJobError::CounterOverflow)?;
        let verify_restarts_total = self
            .state
            .verify_restarts_total
            .checked_add(transition.verify_restarts)
            .ok_or(MutationJobError::CounterOverflow)?;
        let receipt = MutationJobAdvanceReceipt {
            request_sequence: request.expected_sequence,
            committed_sequence,
            status: transition.status,
            phase: transition.phase,
            keys_scanned: transition.keys_scanned,
            rows_updated: transition.rows_updated,
            keys_scanned_total,
            rows_updated_total,
            verify_restarts_total,
        };
        let record = Self {
            state: MutationJobState {
                job_id: self.state.job_id,
                sequence: committed_sequence,
                status: transition.status,
                phase: transition.phase,
                keys_scanned_total,
                rows_updated_total,
                verify_restarts_total,
            },
            canonical_intent: self.canonical_intent.clone(),
            engine_continuation: transition.engine_continuation,
            last_receipt: Some(RetainedMutationJobReceipt {
                receipt: receipt.clone(),
                idempotency_key: request.idempotency_key.clone(),
            }),
        };
        record.validate()?;
        Ok((record, receipt))
    }

    pub(in crate::db) fn validate(&self) -> Result<(), MutationJobError> {
        self.state.job_id.validate()?;
        validate_nonempty_bytes(
            &self.canonical_intent,
            MAX_MUTATION_JOB_INTENT_BYTES,
            MutationJobPayloadKind::Intent,
        )?;
        validate_bytes(
            &self.engine_continuation,
            MAX_MUTATION_JOB_CONTINUATION_BYTES,
            MutationJobPayloadKind::Continuation,
        )?;
        if self.state.rows_updated_total > self.state.keys_scanned_total
            || matches!(self.state.status, MutationJobStatus::Active)
                && self.engine_continuation.is_empty()
            || matches!(self.state.status, MutationJobStatus::Completed)
                && (self.state.phase != MutationJobPhase::Verify
                    || !self.engine_continuation.is_empty())
            || matches!(self.state.status, MutationJobStatus::RestartRequired(_))
                && !self.engine_continuation.is_empty()
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        match &self.last_receipt {
            None => {
                if self.state.sequence != 0
                    || self.state.status != MutationJobStatus::Active
                    || self.state.phase != MutationJobPhase::Forward
                    || self.state.keys_scanned_total != 0
                    || self.state.rows_updated_total != 0
                    || self.state.verify_restarts_total != 0
                {
                    return Err(MutationJobError::CorruptProgressStore);
                }
            }
            Some(retained) => {
                retained.idempotency_key.validate()?;
                validate_receipt(&retained.receipt)?;
                if retained.receipt.committed_sequence != self.state.sequence
                    || retained.receipt.request_sequence.checked_add(1)
                        != Some(retained.receipt.committed_sequence)
                    || retained.receipt.status != self.state.status
                    || retained.receipt.phase != self.state.phase
                    || retained.receipt.keys_scanned_total != self.state.keys_scanned_total
                    || retained.receipt.rows_updated_total != self.state.rows_updated_total
                    || retained.receipt.verify_restarts_total != self.state.verify_restarts_total
                {
                    return Err(MutationJobError::CorruptProgressStore);
                }
                let receipt_bytes = retained_receipt_encoded_len(retained)?;
                if receipt_bytes > MAX_MUTATION_JOB_RECEIPT_BYTES {
                    return Err(payload_too_large(
                        MutationJobPayloadKind::Receipt,
                        MAX_MUTATION_JOB_RECEIPT_BYTES,
                        receipt_bytes,
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct MutationJobTransition {
    status: MutationJobStatus,
    phase: MutationJobPhase,
    engine_continuation: Vec<u8>,
    keys_scanned: u64,
    rows_updated: u64,
    verify_restarts: u64,
}

impl MutationJobTransition {
    pub(in crate::db) const fn new(
        status: MutationJobStatus,
        phase: MutationJobPhase,
        engine_continuation: Vec<u8>,
        keys_scanned: u64,
        rows_updated: u64,
        verify_restarts: u64,
    ) -> Self {
        Self {
            status,
            phase,
            engine_continuation,
            keys_scanned,
            rows_updated,
            verify_restarts,
        }
    }

    fn validate(&self, previous_phase: MutationJobPhase) -> Result<(), MutationJobError> {
        validate_bytes(
            &self.engine_continuation,
            MAX_MUTATION_JOB_CONTINUATION_BYTES,
            MutationJobPayloadKind::Continuation,
        )?;
        let expected_verify_restarts = u64::from(
            previous_phase == MutationJobPhase::Verify
                && self.phase == MutationJobPhase::Forward
                && self.status == MutationJobStatus::Active,
        );
        if self.keys_scanned > MAX_MUTATION_JOB_STEP_KEYS_SCANNED
            || self.rows_updated > MAX_MUTATION_JOB_STEP_ROWS_UPDATED
            || self.rows_updated > self.keys_scanned
            || matches!(self.status, MutationJobStatus::Active)
                && self.engine_continuation.is_empty()
            || previous_phase == MutationJobPhase::Verify && self.rows_updated != 0
            || self.verify_restarts != expected_verify_restarts
            || matches!(self.status, MutationJobStatus::Completed)
                && (previous_phase != MutationJobPhase::Verify
                    || self.phase != MutationJobPhase::Verify
                    || self.rows_updated != 0
                    || !self.engine_continuation.is_empty())
            || matches!(self.status, MutationJobStatus::RestartRequired(_))
                && (self.phase != previous_phase
                    || !self.engine_continuation.is_empty()
                    || self.keys_scanned != 0
                    || self.rows_updated != 0)
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(())
    }
}

pub(in crate::db) fn encode_mutation_job_payload(
    record: &MutationJobRecord,
) -> Result<Vec<u8>, MutationJobError> {
    record.validate()?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&record.state.job_id.to_bytes());
    bytes.extend_from_slice(&record.state.sequence.to_be_bytes());
    write_status(&mut bytes, record.state.status);
    write_phase(&mut bytes, record.state.phase);
    bytes.extend_from_slice(&record.state.keys_scanned_total.to_be_bytes());
    bytes.extend_from_slice(&record.state.rows_updated_total.to_be_bytes());
    bytes.extend_from_slice(&record.state.verify_restarts_total.to_be_bytes());
    write_bytes(&mut bytes, &record.canonical_intent)?;
    write_bytes(&mut bytes, &record.engine_continuation)?;
    match &record.last_receipt {
        None => bytes.push(0),
        Some(retained) => {
            bytes.push(1);
            let receipt = &retained.receipt;
            bytes.extend_from_slice(&receipt.request_sequence.to_be_bytes());
            bytes.extend_from_slice(&receipt.committed_sequence.to_be_bytes());
            write_status(&mut bytes, receipt.status);
            write_phase(&mut bytes, receipt.phase);
            bytes.extend_from_slice(&receipt.keys_scanned.to_be_bytes());
            bytes.extend_from_slice(&receipt.rows_updated.to_be_bytes());
            bytes.extend_from_slice(&receipt.keys_scanned_total.to_be_bytes());
            bytes.extend_from_slice(&receipt.rows_updated_total.to_be_bytes());
            bytes.extend_from_slice(&receipt.verify_restarts_total.to_be_bytes());
            write_bytes(&mut bytes, retained.idempotency_key.as_str().as_bytes())?;
        }
    }
    Ok(bytes)
}

pub(in crate::db) fn decode_mutation_job_payload(
    bytes: &[u8],
) -> Result<MutationJobRecord, MutationJobError> {
    if bytes.len() > MAX_MUTATION_JOB_RECORD_BYTES {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let mut reader = Reader::new(bytes);
    let job_id = MutationJobId::try_from_bytes(reader.array()?)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    let state = MutationJobState {
        job_id,
        sequence: reader.u64()?,
        status: read_status(&mut reader)?,
        phase: read_phase(&mut reader)?,
        keys_scanned_total: reader.u64()?,
        rows_updated_total: reader.u64()?,
        verify_restarts_total: reader.u64()?,
    };
    let canonical_intent = reader.bytes(MAX_MUTATION_JOB_INTENT_BYTES)?.to_vec();
    let engine_continuation = reader.bytes(MAX_MUTATION_JOB_CONTINUATION_BYTES)?.to_vec();
    let last_receipt = match reader.u8()? {
        0 => None,
        1 => {
            let receipt = MutationJobAdvanceReceipt {
                request_sequence: reader.u64()?,
                committed_sequence: reader.u64()?,
                status: read_status(&mut reader)?,
                phase: read_phase(&mut reader)?,
                keys_scanned: reader.u64()?,
                rows_updated: reader.u64()?,
                keys_scanned_total: reader.u64()?,
                rows_updated_total: reader.u64()?,
                verify_restarts_total: reader.u64()?,
            };
            let idempotency_key = MutationJobIdempotencyKey::new(
                reader.string(MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES)?,
            )
            .map_err(|_| MutationJobError::CorruptProgressStore)?;
            Some(RetainedMutationJobReceipt {
                receipt,
                idempotency_key,
            })
        }
        _ => return Err(MutationJobError::CorruptProgressStore),
    };
    if !reader.is_empty() {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let record = MutationJobRecord {
        state,
        canonical_intent,
        engine_continuation,
        last_receipt,
    };
    record
        .validate()
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    Ok(record)
}

fn validate_receipt(receipt: &MutationJobAdvanceReceipt) -> Result<(), MutationJobError> {
    if receipt.keys_scanned > MAX_MUTATION_JOB_STEP_KEYS_SCANNED
        || receipt.rows_updated > MAX_MUTATION_JOB_STEP_ROWS_UPDATED
        || receipt.rows_updated > receipt.keys_scanned
        || receipt.rows_updated_total > receipt.keys_scanned_total
        || receipt.keys_scanned > receipt.keys_scanned_total
        || receipt.rows_updated > receipt.rows_updated_total
        || matches!(receipt.status, MutationJobStatus::Completed)
            && (receipt.phase != MutationJobPhase::Verify || receipt.rows_updated != 0)
        || matches!(receipt.status, MutationJobStatus::RestartRequired(_))
            && (receipt.keys_scanned != 0 || receipt.rows_updated != 0)
    {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(())
}

fn retained_receipt_encoded_len(
    retained: &RetainedMutationJobReceipt,
) -> Result<usize, MutationJobError> {
    let status_bytes = match retained.receipt.status {
        MutationJobStatus::RestartRequired(_) => 2,
        MutationJobStatus::Active | MutationJobStatus::Completed => 1,
    };
    8_usize
        .checked_add(8)
        .and_then(|value| value.checked_add(status_bytes))
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_add(5 * 8))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(retained.idempotency_key.as_str().len()))
        .ok_or(MutationJobError::CounterOverflow)
}

fn validate_nonempty_bytes(
    value: &[u8],
    limit: usize,
    kind: MutationJobPayloadKind,
) -> Result<(), MutationJobError> {
    if value.is_empty() {
        return Err(MutationJobError::CorruptProgressStore);
    }
    validate_bytes(value, limit, kind)
}

fn validate_bytes(
    value: &[u8],
    limit: usize,
    kind: MutationJobPayloadKind,
) -> Result<(), MutationJobError> {
    if value.len() > limit {
        return Err(payload_too_large(kind, limit, value.len()));
    }
    Ok(())
}

fn payload_too_large(
    kind: MutationJobPayloadKind,
    limit: usize,
    observed: usize,
) -> MutationJobError {
    MutationJobError::PayloadTooLarge {
        kind,
        limit: u64::try_from(limit).map_or(u64::MAX, |value| value),
        observed: u64::try_from(observed).map_or(u64::MAX, |value| value),
    }
}

fn write_status(bytes: &mut Vec<u8>, status: MutationJobStatus) {
    match status {
        MutationJobStatus::Active => bytes.push(0),
        MutationJobStatus::Completed => bytes.push(1),
        MutationJobStatus::RestartRequired(reason) => {
            bytes.push(2);
            bytes.push(match reason {
                MutationJobRestartReason::AcceptedSchemaChanged => 0,
                MutationJobRestartReason::TargetAllocationChanged => 1,
                MutationJobRestartReason::IntentIneligible => 2,
                MutationJobRestartReason::BatchPolicyChanged => 3,
                MutationJobRestartReason::UnsupportedContinuation => 4,
                MutationJobRestartReason::ManagedTimestampRegression => 5,
                MutationJobRestartReason::CandidateExceedsBatchPolicy => 6,
            });
        }
    }
}

fn read_status(reader: &mut Reader<'_>) -> Result<MutationJobStatus, MutationJobError> {
    match reader.u8()? {
        0 => Ok(MutationJobStatus::Active),
        1 => Ok(MutationJobStatus::Completed),
        2 => Ok(MutationJobStatus::RestartRequired(match reader.u8()? {
            0 => MutationJobRestartReason::AcceptedSchemaChanged,
            1 => MutationJobRestartReason::TargetAllocationChanged,
            2 => MutationJobRestartReason::IntentIneligible,
            3 => MutationJobRestartReason::BatchPolicyChanged,
            4 => MutationJobRestartReason::UnsupportedContinuation,
            5 => MutationJobRestartReason::ManagedTimestampRegression,
            6 => MutationJobRestartReason::CandidateExceedsBatchPolicy,
            _ => return Err(MutationJobError::CorruptProgressStore),
        })),
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

fn write_phase(bytes: &mut Vec<u8>, phase: MutationJobPhase) {
    bytes.push(match phase {
        MutationJobPhase::Forward => 0,
        MutationJobPhase::Verify => 1,
    });
}

fn read_phase(reader: &mut Reader<'_>) -> Result<MutationJobPhase, MutationJobError> {
    match reader.u8()? {
        0 => Ok(MutationJobPhase::Forward),
        1 => Ok(MutationJobPhase::Verify),
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), MutationJobError> {
    let len = u32::try_from(value.len()).map_err(|_| MutationJobError::Internal)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, MutationJobError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        self.offset += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, MutationJobError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, MutationJobError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MutationJobError> {
        self.take(N)?
            .try_into()
            .map_err(|_| MutationJobError::CorruptProgressStore)
    }

    fn bytes(&mut self, max: usize) -> Result<&'a [u8], MutationJobError> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(MutationJobError::CorruptProgressStore);
        }
        self.take(len)
    }

    fn string(&mut self, max: usize) -> Result<String, MutationJobError> {
        let bytes = self.bytes(max)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| MutationJobError::CorruptProgressStore)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MutationJobError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(MutationJobError::CorruptProgressStore)?;
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
                    MutationJobStatus::RestartRequired(
                        MutationJobRestartReason::BatchPolicyChanged,
                    ),
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

        for record in [initial, active, completed, restart, oversized_candidate] {
            let bytes = encode_mutation_job_payload(&record)
                .expect("current mutation payload should encode");
            assert!(!bytes.starts_with(b"DIDL"));
            assert_eq!(
                decode_mutation_job_payload(&bytes)
                    .expect("current mutation payload should decode"),
                record,
            );
        }
    }

    #[test]
    fn public_state_request_receipt_and_error_are_candid_compatible() {
        let state = initial_record().state().clone();
        let request = request(0, "candid-request");
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
        let error = MutationJobError::StaleSequence {
            expected: 0,
            actual: 1,
        };

        let state_bytes = candid::encode_one(&state).expect("mutation state should encode");
        let request_bytes = candid::encode_one(&request).expect("mutation request should encode");
        let receipt_bytes = candid::encode_one(&receipt).expect("mutation receipt should encode");
        let error_bytes = candid::encode_one(&error).expect("mutation error should encode");
        assert_eq!(
            candid::decode_one::<MutationJobState>(&state_bytes)
                .expect("mutation state should decode"),
            state,
        );
        assert_eq!(
            candid::decode_one::<MutationJobAdvanceRequest>(&request_bytes)
                .expect("mutation request should decode"),
            request,
        );
        assert_eq!(
            candid::decode_one::<MutationJobAdvanceReceipt>(&receipt_bytes)
                .expect("mutation receipt should decode"),
            receipt,
        );
        assert_eq!(
            candid::decode_one::<MutationJobError>(&error_bytes)
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
        assert_eq!(
            decode_mutation_job_payload(&bytes[..bytes.len() - 1]),
            Err(MutationJobError::CorruptProgressStore),
        );
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
}
