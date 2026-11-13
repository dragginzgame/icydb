//! Module: db::mutation_job
//! Responsibility: bounded durable mutation-job lifecycle, replay, and current payload codec.
//! Does not own: SQL lowering, target traversal, row mutation, or commit-marker recovery.
//! Boundary: trusted mutation coordinator -> excluded progress-record envelope.

#[cfg(feature = "sql")]
mod intent;

use crate::db::codec::{ByteDecodeError, ByteReader};

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
pub const MAX_MUTATION_JOB_STEP_KEYS_SCANNED: u64 = 4_096;
/// Maximum target rows changed by one mutation-job advance.
pub const MAX_MUTATION_JOB_STEP_ROWS_UPDATED: u64 =
    crate::db::executor::MAX_MUTATION_PROGRESS_BATCH_ROWS_AT_MAX_INDEX_FANOUT as u64;

/// Nonzero application-owned identity for one durable mutation job incarnation.
///
/// An application must allocate a fresh identity for every logical job. An
/// identity is never reusable after cancellation, acknowledgement, failure,
/// or an absent-record response.
#[derive(
    serde::Serialize, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd,
)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
    /// Admitted page work exceeded the execution policy calibrated for it.
    ExecutionBudgetPolicyExceeded,
    /// An accepted constraint rejected a target row before the page committed.
    TargetRowRejected,
}

/// Bounded reason why target mutation execution failed before commit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum MutationJobTargetFailureReason {
    /// Writer and page-packer staged-byte authorities diverged.
    StagingByteBudgetExceeded,
    /// Another target mutation failure occurred without safe semantic detail.
    Other,
}

/// Durable lifecycle of one mutation job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum MutationJobStatus {
    /// One bounded Forward or Verify step may still run.
    Active,
    /// Stable clean Verify exhaustion committed.
    Completed,
    /// Authority, policy or target-row rejection requires a new job.
    RestartRequired(MutationJobRestartReason),
}

/// Current convergence phase of one mutation job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum MutationJobPhase {
    /// Scan authoritative keys and converge stale rows.
    Forward,
    /// Prove a clean scan at one unchanged durable target revision.
    Verify,
}

/// Public bounded state for one retained mutation job.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
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
    StaleSequence {
        /// Sequence requested by the caller.
        expected: u64,
        /// Job's current committed sequence.
        actual: u64,
    },
    /// Acknowledgement targeted an active job.
    Active,
    /// A non-replay advance targeted a completed job.
    Completed,
    /// A non-replay advance targeted a job that must restart.
    RestartRequired(MutationJobRestartReason),
    /// A bounded persisted component exceeded its engine-owned limit.
    PayloadTooLarge {
        /// Identity of the rejected payload or component kind.
        kind: MutationJobPayloadKind,
        /// Maximum admitted value for this bound.
        limit: u64,
        /// Observed value that exceeded the bound.
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
    TargetMutationFailed(MutationJobTargetFailureReason),
    /// Target or recovery admission, or traversal, failed before progress committed.
    TargetQueryFailed,
    /// An internal database invariant prevented the operation.
    Internal,
    /// The enclosing request exhausted aggregate IcyDB work allowance.
    ExecutionBudgetExceeded {
        /// Typed or numeric identity of the exhausted resource.
        resource: u64,
        /// Maximum admitted value for this bound.
        limit: u64,
        /// Observed value that exceeded the bound.
        observed: u64,
        /// Numeric identity of the accounting scope.
        scope: u64,
        /// Numeric identity of the execution lane.
        lane: u64,
        /// Prefix of the normalized query-shape fingerprint.
        normalized_shape_fingerprint_prefix: u64,
    },
}

#[cfg(feature = "sql")]
impl MutationJobError {
    // Identity preparation failures are not evidence of corrupt persisted data
    // or ineligible syntax. Keep the existing bounded resource error intact.
    pub(in crate::db) fn from_internal_error(error: &crate::error::InternalError) -> Self {
        use icydb_diagnostic_code::{
            DiagnosticDetail, DiagnosticFactTag as Fact, RuntimeBoundaryCode,
        };

        if !matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ) {
            return Self::Internal;
        }
        let facts = error.diagnostic_facts();
        let [
            Some(resource),
            Some(limit),
            Some(observed),
            Some(scope),
            Some(lane),
            Some(prefix),
        ] = [
            Fact::BudgetResource,
            Fact::Limit,
            Fact::Actual,
            Fact::ExecutionBudgetScope,
            Fact::ExecutionLane,
            Fact::QueryShapeFingerprintPrefix,
        ]
        .map(|tag| {
            facts
                .iter()
                .find(|(fact, _)| *fact == tag)
                .map(|(_, value)| *value)
        })
        else {
            return Self::Internal;
        };
        Self::ExecutionBudgetExceeded {
            resource,
            limit,
            observed,
            scope,
            lane,
            normalized_shape_fingerprint_prefix: prefix,
        }
    }

    pub(in crate::db) fn from_query_error(error: crate::db::query::intent::QueryError) -> Self {
        match error {
            crate::db::query::intent::QueryError::Execute(error) => {
                if matches!(
                    error.diagnostic().detail(),
                    Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary { .. })
                ) {
                    Self::IneligibleIntent
                } else {
                    Self::from_internal_error(error.as_internal())
                }
            }
            _ => Self::IneligibleIntent,
        }
    }
}

impl fmt::Display for MutationJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("mutation job operation failed")
    }
}

impl From<ByteDecodeError> for MutationJobError {
    fn from(_: ByteDecodeError) -> Self {
        Self::CorruptProgressStore
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
    let mut reader = ByteReader::new(bytes);
    let job_id = MutationJobId::try_from_bytes(reader.read_array()?)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    let state = MutationJobState {
        job_id,
        sequence: reader.read_u64()?,
        status: read_status(&mut reader)?,
        phase: read_phase(&mut reader)?,
        keys_scanned_total: reader.read_u64()?,
        rows_updated_total: reader.read_u64()?,
        verify_restarts_total: reader.read_u64()?,
    };
    let canonical_intent = reader
        .read_bounded_len_prefixed_bytes(MAX_MUTATION_JOB_INTENT_BYTES)?
        .to_vec();
    let engine_continuation = reader
        .read_bounded_len_prefixed_bytes(MAX_MUTATION_JOB_CONTINUATION_BYTES)?
        .to_vec();
    let last_receipt = match reader.read_u8()? {
        0 => None,
        1 => {
            let receipt = MutationJobAdvanceReceipt {
                request_sequence: reader.read_u64()?,
                committed_sequence: reader.read_u64()?,
                status: read_status(&mut reader)?,
                phase: read_phase(&mut reader)?,
                keys_scanned: reader.read_u64()?,
                rows_updated: reader.read_u64()?,
                keys_scanned_total: reader.read_u64()?,
                rows_updated_total: reader.read_u64()?,
                verify_restarts_total: reader.read_u64()?,
            };
            let idempotency_key = MutationJobIdempotencyKey::new(
                reader.read_bounded_string(MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES)?,
            )
            .map_err(|_| MutationJobError::CorruptProgressStore)?;
            Some(RetainedMutationJobReceipt {
                receipt,
                idempotency_key,
            })
        }
        _ => return Err(MutationJobError::CorruptProgressStore),
    };
    reader.finish()?;
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
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
        observed: u64::try_from(observed).unwrap_or(u64::MAX),
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
                MutationJobRestartReason::ExecutionBudgetPolicyExceeded => 7,
                MutationJobRestartReason::TargetRowRejected => 8,
            });
        }
    }
}

fn read_status(reader: &mut ByteReader<'_>) -> Result<MutationJobStatus, MutationJobError> {
    match reader.read_u8()? {
        0 => Ok(MutationJobStatus::Active),
        1 => Ok(MutationJobStatus::Completed),
        2 => Ok(MutationJobStatus::RestartRequired(
            match reader.read_u8()? {
                0 => MutationJobRestartReason::AcceptedSchemaChanged,
                1 => MutationJobRestartReason::TargetAllocationChanged,
                2 => MutationJobRestartReason::IntentIneligible,
                3 => MutationJobRestartReason::BatchPolicyChanged,
                4 => MutationJobRestartReason::UnsupportedContinuation,
                5 => MutationJobRestartReason::ManagedTimestampRegression,
                6 => MutationJobRestartReason::CandidateExceedsBatchPolicy,
                7 => MutationJobRestartReason::ExecutionBudgetPolicyExceeded,
                8 => MutationJobRestartReason::TargetRowRejected,
                _ => return Err(MutationJobError::CorruptProgressStore),
            },
        )),
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

fn write_phase(bytes: &mut Vec<u8>, phase: MutationJobPhase) {
    bytes.push(match phase {
        MutationJobPhase::Forward => 0,
        MutationJobPhase::Verify => 1,
    });
}

fn read_phase(reader: &mut ByteReader<'_>) -> Result<MutationJobPhase, MutationJobError> {
    match reader.read_u8()? {
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

#[cfg(test)]
mod tests;
