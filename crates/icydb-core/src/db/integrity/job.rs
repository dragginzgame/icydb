//! Module: db::integrity::job
//! Responsibility: invariant-bearing Deep job, checkpoint, and receipt vocabulary.
//! Does not own: stable storage, physical traversal, authorization, or time.
//! Boundary: Deep controller <-> current-form progress record codec.

use crate::db::{
    integrity::{
        DatabaseIncarnationId, IntegrityAuthorityDiagnostic, IntegrityEntityIdentity,
        IntegrityFinding, IntegrityPhase, IntegrityProofVector, IntegrityResourceDiagnostic,
        IntegrityVerifierFamily, PhysicalUnitCheckpoint, StorageTraversalCorruption,
    },
    journal::JournalInspectionCheckpoint,
};
use candid::CandidType;
use serde::Deserialize;

pub(in crate::db) const MAX_INTEGRITY_OWNER_BYTES: usize = 256;
pub(in crate::db) const MAX_INTEGRITY_SUBMISSION_KEY_BYTES: usize = 256;
const MAX_INTEGRITY_PATH_BYTES: usize = 4 * 1024;
const MAX_INTEGRITY_RECEIPT_FINDINGS: usize = 64;
pub(in crate::db) const MAX_INTEGRITY_IN_PROGRESS_PAGES: u64 = u64::MAX - 1;

/// Opaque lookup identity for one retained Deep inspection job.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct IntegrityJobId([u8; 32]);

impl IntegrityJobId {
    /// Admit one nonzero current-form lookup identity.
    pub(in crate::db) fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, IntegrityJobError> {
        if bytes == [0; 32] {
            return Err(IntegrityJobError::CorruptProgressRecord);
        }
        Ok(Self(bytes))
    }

    /// Return the canonical lookup bytes.
    #[must_use]
    pub(in crate::db) const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Bounded authorization identity persisted with one job.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityJobOwner(String);

impl IntegrityJobOwner {
    /// Admit one nonempty bounded owner identity.
    pub(in crate::db) fn new(value: impl Into<String>) -> Result<Self, IntegrityJobError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_INTEGRITY_OWNER_BYTES {
            return Err(IntegrityJobError::InvalidOwner);
        }
        Ok(Self(value))
    }

    /// Borrow the canonical owner identity.
    #[must_use]
    pub(in crate::db) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Bounded client idempotency identity for Deep start.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegritySubmissionKey(String);

impl IntegritySubmissionKey {
    /// Admit one nonempty bounded submission key.
    pub(in crate::db) fn new(value: impl Into<String>) -> Result<Self, IntegrityJobError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_INTEGRITY_SUBMISSION_KEY_BYTES {
            return Err(IntegrityJobError::InvalidSubmissionKey);
        }
        Ok(Self(value))
    }

    /// Borrow the canonical submission key.
    #[must_use]
    pub(in crate::db) const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Exact private continuation for the current Deep phase.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityCheckpoint {
    /// Bounded accepted metadata and control closure.
    QuickMetadata,
    /// Canonical source-row interval.
    Rows(PhysicalUnitCheckpoint),
    /// One active forward-index domain in accepted plan order.
    Index {
        ordinal: u32,
        checkpoint: PhysicalUnitCheckpoint,
    },
    /// One active source-owned reverse domain in accepted plan order.
    ReverseRelation {
        ordinal: u32,
        checkpoint: PhysicalUnitCheckpoint,
    },
    /// One participating journal tail in canonical store order.
    Journal {
        store_ordinal: u32,
        checkpoint: JournalInspectionCheckpoint,
    },
    /// No physical traversal remains; only final proof equality may complete.
    FinalProof,
}

impl IntegrityCheckpoint {
    /// Return the canonical phase implied by this checkpoint.
    #[must_use]
    pub(in crate::db) const fn phase(&self) -> IntegrityPhase {
        match self {
            Self::QuickMetadata => IntegrityPhase::QuickMetadata,
            Self::Rows(_) => IntegrityPhase::Rows,
            Self::Index { .. } => IntegrityPhase::IndexEntries,
            Self::ReverseRelation { .. } => IntegrityPhase::ReverseRelations,
            Self::Journal { .. } => IntegrityPhase::JournalTails,
            Self::FinalProof => IntegrityPhase::FinalProofVectorCheck,
        }
    }
}

/// Frozen intent that supersedes advancement after the outstanding page is acknowledged.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityPendingTerminal {
    /// The inactivity lease elapsed.
    Expired,
    /// The authorized owner requested abort.
    Aborted,
}

/// Stable terminal meaning of a completed Deep job.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityTerminalOutcome {
    /// Every phase exhausted cleanly under one unchanged proof.
    DeepCompleteClean,
    /// Every phase exhausted with one or more definite findings.
    DeepCompleteWithFindings,
    /// One proof component changed before completion.
    Invalidated,
    /// Accepted authority could not be inspected.
    Uninspectable(IntegrityAuthorityDiagnostic),
    /// A load-bearing physical traversal could not prove progress.
    UninspectableStorage(StorageTraversalCorruption),
    /// One frozen bounded resource was insufficient.
    ResourceLimited(IntegrityResourceDiagnostic),
    /// The inactivity lease expired.
    Expired,
    /// The authorized owner aborted the job.
    Aborted,
}

/// Durable job lifecycle state.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityJobState {
    /// Physical advancement remains permitted.
    InProgress,
    /// Advancement is frozen while the last page remains unacknowledged.
    TerminalPending(IntegrityPendingTerminal),
    /// One final receipt is retained for replay and acknowledgement.
    Terminal {
        outcome: IntegrityTerminalOutcome,
        receipt_acknowledged: bool,
    },
}

/// Semantic status carried by one bounded Deep page.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum DeepIntegrityPageStatus {
    /// More physical work or the final proof check remains.
    InProgress,
    /// This receipt records the stable terminal result.
    Terminal(IntegrityTerminalOutcome),
}

/// One bounded replayable Deep result page.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct DeepIntegrityPage {
    pub(super) job_id: IntegrityJobId,
    pub(super) page_sequence: u64,
    pub(super) phase: IntegrityPhase,
    pub(super) status: DeepIntegrityPageStatus,
    pub(super) pages_completed: u64,
    pub(super) findings_seen: u64,
    pub(super) findings: Vec<IntegrityFinding>,
    pub(super) blocked_verifier_families: Vec<IntegrityVerifierFamily>,
}

impl DeepIntegrityPage {
    /// Return the opaque job lookup identity.
    #[must_use]
    pub(in crate::db) const fn job_id(&self) -> IntegrityJobId {
        self.job_id
    }

    /// Return the monotonically increasing receipt sequence.
    #[must_use]
    pub(in crate::db) const fn page_sequence(&self) -> u64 {
        self.page_sequence
    }

    /// Return the phase represented by this receipt.
    #[must_use]
    pub(in crate::db) const fn phase(&self) -> IntegrityPhase {
        self.phase
    }

    /// Borrow the current or terminal status.
    #[must_use]
    pub(in crate::db) const fn status(&self) -> &DeepIntegrityPageStatus {
        &self.status
    }

    /// Return cumulative successfully persisted page count.
    #[must_use]
    pub(in crate::db) const fn pages_completed(&self) -> u64 {
        self.pages_completed
    }

    /// Return cumulative definite findings.
    #[must_use]
    pub(in crate::db) const fn findings_seen(&self) -> u64 {
        self.findings_seen
    }

    /// Borrow findings produced only by this page.
    #[must_use]
    pub(in crate::db) const fn findings(&self) -> &[IntegrityFinding] {
        self.findings.as_slice()
    }

    /// Borrow the cumulative canonical blocked-family set.
    #[must_use]
    pub(in crate::db) const fn blocked_verifier_families(&self) -> &[IntegrityVerifierFamily] {
        self.blocked_verifier_families.as_slice()
    }
}

/// Abort receipt status.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityAbortStatus {
    /// Abort is frozen but cannot replace the outstanding page yet.
    TerminationPending(IntegrityPendingTerminal),
    /// The terminal abort result is replayable.
    Terminal(IntegrityTerminalOutcome),
}

/// One bounded abort/expiry receipt.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityAbortReceipt {
    pub(super) job_id: IntegrityJobId,
    pub(super) page_sequence: u64,
    pub(super) status: IntegrityAbortStatus,
}

impl IntegrityAbortReceipt {
    /// Return the opaque job identity.
    #[must_use]
    pub(in crate::db) const fn job_id(&self) -> IntegrityJobId {
        self.job_id
    }

    /// Return the outstanding or terminal receipt sequence.
    #[must_use]
    pub(in crate::db) const fn page_sequence(&self) -> u64 {
        self.page_sequence
    }

    /// Borrow the pending or terminal abort status.
    #[must_use]
    pub(in crate::db) const fn status(&self) -> &IntegrityAbortStatus {
        &self.status
    }
}

/// Persisted receipt body.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityJobReceipt {
    /// Normal start, advancement, or completion page.
    Page(DeepIntegrityPage),
    /// Abort/expiry terminal or pending acknowledgement.
    Abort(IntegrityAbortReceipt),
}

impl IntegrityJobReceipt {
    /// Return the job identity carried by this receipt.
    #[must_use]
    pub(in crate::db) const fn job_id(&self) -> IntegrityJobId {
        match self {
            Self::Page(page) => page.job_id,
            Self::Abort(receipt) => receipt.job_id,
        }
    }

    /// Return the sequence carried by this receipt.
    #[must_use]
    pub(in crate::db) const fn page_sequence(&self) -> u64 {
        match self {
            Self::Page(page) => page.page_sequence,
            Self::Abort(receipt) => receipt.page_sequence,
        }
    }
}

/// Request identity that produced the cached receipt.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum IntegrityReceiptReplayKey {
    /// Initial Deep start.
    Start,
    /// Continue request acknowledging the named prior sequence.
    Continue { acknowledged_sequence: u64 },
}

/// One cached bounded receipt and its exact replay request.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityReceiptEnvelope {
    pub(super) replay_key: IntegrityReceiptReplayKey,
    pub(super) receipt: IntegrityJobReceipt,
}

impl IntegrityReceiptEnvelope {
    /// Borrow the exact replay request identity.
    #[must_use]
    pub(in crate::db) const fn replay_key(&self) -> IntegrityReceiptReplayKey {
        self.replay_key
    }

    /// Borrow the cached bounded receipt.
    #[must_use]
    pub(in crate::db) const fn receipt(&self) -> &IntegrityJobReceipt {
        &self.receipt
    }
}

/// Current invariant-bearing durable Deep record.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct IntegrityJob {
    pub(super) id: IntegrityJobId,
    pub(super) database_incarnation_id: DatabaseIncarnationId,
    pub(super) owner: IntegrityJobOwner,
    pub(super) submission_key: IntegritySubmissionKey,
    pub(super) entity: IntegrityEntityIdentity,
    pub(super) accepted_schema_version: u32,
    pub(super) accepted_schema_fingerprint: [u8; 16],
    pub(super) inspection_plan_fingerprint: [u8; 32],
    pub(super) checkpoint: IntegrityCheckpoint,
    pub(super) captured_proof_vector: IntegrityProofVector,
    pub(super) state: IntegrityJobState,
    pub(super) lease_deadline_nanos: u64,
    pub(super) findings_seen: u64,
    pub(super) pages_completed: u64,
    pub(super) blocked_verifier_families: Vec<IntegrityVerifierFamily>,
    pub(super) last_receipt: IntegrityReceiptEnvelope,
}

impl IntegrityJob {
    /// Validate all persisted cross-field invariants before use.
    pub(super) fn validate(&self) -> Result<(), IntegrityJobError> {
        if self.id != self.last_receipt.receipt.job_id()
            || self.database_incarnation_id != self.captured_proof_vector.database_incarnation_id()
            || self.accepted_schema_version != self.captured_proof_vector.accepted_schema_version()
            || self.accepted_schema_fingerprint
                != self.captured_proof_vector.accepted_schema_fingerprint()
            || self.inspection_plan_fingerprint
                != self.captured_proof_vector.inspection_plan_fingerprint()
            || self.entity.entity_path().is_empty()
            || self.entity.entity_path().len() > MAX_INTEGRITY_PATH_BYTES
            || self.entity.store_path().is_empty()
            || self.entity.store_path().len() > MAX_INTEGRITY_PATH_BYTES
            || self.entity.entity_tag() == 0
            || self.owner.as_str().is_empty()
            || self.owner.as_str().len() > MAX_INTEGRITY_OWNER_BYTES
            || self.submission_key.as_str().is_empty()
            || self.submission_key.as_str().len() > MAX_INTEGRITY_SUBMISSION_KEY_BYTES
            || self.accepted_schema_version == 0
            || self.lease_deadline_nanos == 0
            || matches!(
                self.state,
                IntegrityJobState::InProgress | IntegrityJobState::TerminalPending(_)
            ) && self.pages_completed > MAX_INTEGRITY_IN_PROGRESS_PAGES
            || !strictly_sorted_unique(&self.blocked_verifier_families)
            || self.captured_proof_vector.validate().is_err()
            || !self.checkpoint_is_well_formed()
        {
            return Err(IntegrityJobError::CorruptProgressRecord);
        }

        let receipt_matches_state = match (&self.state, &self.last_receipt.receipt) {
            (
                IntegrityJobState::InProgress | IntegrityJobState::TerminalPending(_),
                IntegrityJobReceipt::Page(page),
            ) => {
                page.status == DeepIntegrityPageStatus::InProgress
                    && page.phase == self.checkpoint.phase()
                    && self.page_matches_counters(page)
            }
            (IntegrityJobState::Terminal { outcome, .. }, IntegrityJobReceipt::Page(page)) => {
                page.status == DeepIntegrityPageStatus::Terminal(outcome.clone())
                    && page.phase == self.checkpoint.phase()
                    && !matches!(
                        outcome,
                        IntegrityTerminalOutcome::Expired | IntegrityTerminalOutcome::Aborted
                    )
                    && self.page_matches_counters(page)
            }
            (IntegrityJobState::Terminal { outcome, .. }, IntegrityJobReceipt::Abort(receipt)) => {
                receipt.status == IntegrityAbortStatus::Terminal(outcome.clone())
                    && matches!(
                        outcome,
                        IntegrityTerminalOutcome::Expired | IntegrityTerminalOutcome::Aborted
                    )
            }
            _ => false,
        };
        if !receipt_matches_state
            || self.last_receipt.receipt.page_sequence() != self.pages_completed
            || !self.replay_key_matches_receipt()
            || !self.terminal_outcome_matches_counts()
        {
            return Err(IntegrityJobError::CorruptProgressRecord);
        }

        Ok(())
    }

    fn page_matches_counters(&self, page: &DeepIntegrityPage) -> bool {
        page.pages_completed == self.pages_completed
            && page.findings_seen == self.findings_seen
            && page.findings.len() <= MAX_INTEGRITY_RECEIPT_FINDINGS
            && u64::try_from(page.findings.len()).is_ok_and(|count| count <= self.findings_seen)
            && page.blocked_verifier_families == self.blocked_verifier_families
    }

    fn replay_key_matches_receipt(&self) -> bool {
        match self.last_receipt.replay_key {
            IntegrityReceiptReplayKey::Start => {
                self.pages_completed == 0
                    && self.last_receipt.receipt.page_sequence() == 0
                    && matches!(
                        &self.last_receipt.receipt,
                        IntegrityJobReceipt::Page(DeepIntegrityPage {
                            status: DeepIntegrityPageStatus::InProgress,
                            ..
                        })
                    )
            }
            IntegrityReceiptReplayKey::Continue {
                acknowledged_sequence,
            } => acknowledged_sequence
                .checked_add(1)
                .is_some_and(|sequence| sequence == self.last_receipt.receipt.page_sequence()),
        }
    }

    const fn terminal_outcome_matches_counts(&self) -> bool {
        match &self.state {
            IntegrityJobState::Terminal {
                outcome: IntegrityTerminalOutcome::DeepCompleteClean,
                ..
            } => self.findings_seen == 0 && self.blocked_verifier_families.is_empty(),
            IntegrityJobState::Terminal {
                outcome: IntegrityTerminalOutcome::DeepCompleteWithFindings,
                ..
            } => self.findings_seen > 0 || !self.blocked_verifier_families.is_empty(),
            _ => true,
        }
    }

    fn checkpoint_is_well_formed(&self) -> bool {
        match &self.checkpoint {
            IntegrityCheckpoint::Rows(checkpoint) => row_checkpoint_is_well_formed(checkpoint),
            IntegrityCheckpoint::Index {
                ordinal,
                checkpoint,
            } => {
                usize::try_from(*ordinal).is_ok_and(|ordinal| {
                    ordinal < self.captured_proof_vector.index_generation_count()
                }) && index_checkpoint_is_well_formed(checkpoint)
            }
            IntegrityCheckpoint::ReverseRelation {
                ordinal,
                checkpoint,
            } => {
                usize::try_from(*ordinal).is_ok_and(|ordinal| {
                    ordinal < self.captured_proof_vector.relation_generation_count()
                }) && reverse_checkpoint_is_well_formed(checkpoint)
            }
            IntegrityCheckpoint::Journal {
                store_ordinal,
                checkpoint,
            } => usize::try_from(*store_ordinal)
                .ok()
                .and_then(|ordinal| self.captured_proof_vector.stores().get(ordinal))
                .is_some_and(|proof| {
                    let (fold_sequence, next_append_sequence) = proof.journal_interval();
                    journal_checkpoint_is_well_formed(
                        checkpoint,
                        fold_sequence,
                        next_append_sequence,
                    )
                }),
            IntegrityCheckpoint::QuickMetadata | IntegrityCheckpoint::FinalProof => true,
        }
    }
}

fn row_checkpoint_is_well_formed(checkpoint: &PhysicalUnitCheckpoint) -> bool {
    checkpoint.raw_data_key().is_ok()
        && !matches!(
            checkpoint,
            PhysicalUnitCheckpoint::Within {
                verifier_family: IntegrityVerifierFamily::IndexEntry
                    | IntegrityVerifierFamily::UniqueIndex
                    | IntegrityVerifierFamily::ReverseRelationEntry
                    | IntegrityVerifierFamily::JournalEnvelope
                    | IntegrityVerifierFamily::JournalBatchIdentity,
                ..
            }
        )
}

fn index_checkpoint_is_well_formed(checkpoint: &PhysicalUnitCheckpoint) -> bool {
    checkpoint.raw_index_key().is_ok()
        && !matches!(
            checkpoint,
            PhysicalUnitCheckpoint::Within {
                verifier_family: IntegrityVerifierFamily::DataKey
                    | IntegrityVerifierFamily::RowEnvelope
                    | IntegrityVerifierFamily::FieldValue
                    | IntegrityVerifierFamily::PrimaryKey
                    | IntegrityVerifierFamily::ValidatedConstraints
                    | IntegrityVerifierFamily::ForwardIndex
                    | IntegrityVerifierFamily::Relation
                    | IntegrityVerifierFamily::ReverseRelationEntry
                    | IntegrityVerifierFamily::JournalEnvelope
                    | IntegrityVerifierFamily::JournalBatchIdentity,
                ..
            }
        )
}

fn reverse_checkpoint_is_well_formed(checkpoint: &PhysicalUnitCheckpoint) -> bool {
    checkpoint.raw_index_key().is_ok()
        && !matches!(
            checkpoint,
            PhysicalUnitCheckpoint::Within {
                verifier_family: IntegrityVerifierFamily::DataKey
                    | IntegrityVerifierFamily::RowEnvelope
                    | IntegrityVerifierFamily::FieldValue
                    | IntegrityVerifierFamily::PrimaryKey
                    | IntegrityVerifierFamily::ValidatedConstraints
                    | IntegrityVerifierFamily::ForwardIndex
                    | IntegrityVerifierFamily::Relation
                    | IntegrityVerifierFamily::IndexEntry
                    | IntegrityVerifierFamily::UniqueIndex
                    | IntegrityVerifierFamily::JournalEnvelope
                    | IntegrityVerifierFamily::JournalBatchIdentity,
                ..
            }
        )
}

const fn journal_checkpoint_is_well_formed(
    checkpoint: &JournalInspectionCheckpoint,
    fold_sequence: u64,
    next_append_sequence: u64,
) -> bool {
    match checkpoint {
        JournalInspectionCheckpoint::BeforeFirst => true,
        JournalInspectionCheckpoint::BeforeBatch { sequence } => {
            *sequence > fold_sequence && *sequence < next_append_sequence
        }
        JournalInspectionCheckpoint::CheckingBatchIdentity {
            sequence,
            next_prior_sequence,
            ..
        } => {
            *sequence > fold_sequence
                && *sequence < next_append_sequence
                && *next_prior_sequence > fold_sequence
                && *next_prior_sequence < *sequence
        }
        JournalInspectionCheckpoint::AfterBatch { sequence } => {
            *sequence >= fold_sequence && *sequence < next_append_sequence
        }
    }
}

fn strictly_sorted_unique(values: &[IntegrityVerifierFamily]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

/// Typed Deep protocol or progress-record failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IntegrityJobError {
    InvalidOwner,
    InvalidSubmissionKey,
    SubmissionConflict,
    SubmissionAlreadyAdvanced,
    JobNotFound,
    JobOwnerMismatch,
    JobIncarnationMismatch,
    StaleAcknowledgement,
    JobBusy,
    CapacityExceeded,
    CorruptProgressHeader,
    CorruptProgressRecord,
    IncompatibleProgressFormat,
    CounterExhausted,
    Internal,
}

/// Deep protocol failures keep persisted-protocol and engine causes distinct.
#[derive(Debug)]
pub(in crate::db) enum IntegrityDeepError {
    /// Stable job/progress protocol rejected the request.
    Job(IntegrityJobError),
    /// Accepted authority or physical execution failed.
    Internal(crate::error::InternalError),
}

impl From<IntegrityJobError> for IntegrityDeepError {
    fn from(error: IntegrityJobError) -> Self {
        Self::Job(error)
    }
}

impl From<crate::error::InternalError> for IntegrityDeepError {
    fn from(error: crate::error::InternalError) -> Self {
        Self::Internal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_checkpoint_families_stay_phase_owned() {
        let journal_in_row = PhysicalUnitCheckpoint::Within {
            physical_key: vec![1],
            verifier_family: IntegrityVerifierFamily::JournalEnvelope,
            ordinal: 0,
        };
        let row_in_index = PhysicalUnitCheckpoint::Within {
            physical_key: vec![1],
            verifier_family: IntegrityVerifierFamily::FieldValue,
            ordinal: 0,
        };
        let reverse_in_reverse = PhysicalUnitCheckpoint::Within {
            physical_key: vec![1],
            verifier_family: IntegrityVerifierFamily::ReverseRelationEntry,
            ordinal: 0,
        };

        assert!(!row_checkpoint_is_well_formed(&journal_in_row));
        assert!(!index_checkpoint_is_well_formed(&row_in_index));
        assert!(reverse_checkpoint_is_well_formed(&reverse_in_reverse));
    }

    #[test]
    fn persisted_journal_checkpoint_cannot_skip_the_captured_tail_interval() {
        assert!(journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::BeforeFirst,
            4,
            8,
        ));
        assert!(journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::BeforeBatch { sequence: 7 },
            4,
            8,
        ));
        assert!(journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::AfterBatch { sequence: 4 },
            4,
            8,
        ));
        assert!(!journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::BeforeBatch { sequence: 4 },
            4,
            8,
        ));
        assert!(!journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::AfterBatch { sequence: 8 },
            4,
            8,
        ));
        assert!(!journal_checkpoint_is_well_formed(
            &JournalInspectionCheckpoint::CheckingBatchIdentity {
                sequence: 7,
                batch_id: [1; 16],
                next_prior_sequence: 4,
            },
            4,
            8,
        ));
    }
}
