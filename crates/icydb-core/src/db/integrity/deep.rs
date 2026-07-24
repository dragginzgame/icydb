//! Module: db::integrity::deep
//! Responsibility: authorize and advance one exact Deep phase under A/B proof equality.
//! Does not own: SQL syntax, public caller identity, physical codecs, or database recovery.
//! Boundary: accepted plan + progress store + bounded phase cores -> replayable job receipts.

use crate::{
    db::{
        Db,
        commit::database_incarnation_id,
        integrity::{
            DeepIntegrityPage, DeepIntegrityPageStatus, DerivedInspectionLimits,
            IntegrityAbortReceipt, IntegrityAbortStatus, IntegrityAuthorityDiagnostic,
            IntegrityCheckpoint, IntegrityDeepError, IntegrityEntityIdentity, IntegrityFinding,
            IntegrityFindingClass, IntegrityFindingKind, IntegrityJob, IntegrityJobError,
            IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt, IntegrityJobState,
            IntegrityPendingTerminal, IntegrityPhase, IntegrityReceiptEnvelope,
            IntegrityReceiptReplayKey, IntegrityResourceDiagnostic, IntegritySeverity,
            IntegritySubmissionKey, IntegrityTerminalOutcome, IntegrityVerifierFamily,
            MAX_INTEGRITY_IN_PROGRESS_PAGES, PhysicalUnitCheckpoint, QuickIntegrityStatus,
            RowInspectionLimits, capture_integrity_proof_vector, execute_index_integrity_page,
            execute_quick_integrity, execute_reverse_integrity_page, execute_row_integrity_page,
            progress_store::{InsertJobResult, with_progress_store},
        },
        journal::{JournalInspectionCheckpoint, JournalInspectionLimits, JournalIntegrityIssue},
        schema::AcceptedInspectionPlan,
    },
    error::InternalError,
    traits::CanisterKind,
};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

const INTEGRITY_JOB_ID_DOMAIN: &[u8] = b"icydb.integrity-job-id.v1";
const INTEGRITY_JOB_LEASE_NANOS: u64 = 15 * 60 * 1_000_000_000;
const INTEGRITY_TERMINAL_RETENTION_NANOS: u64 = 60 * 60 * 1_000_000_000;
const MAX_RETENTION_RECORDS_PER_PAGE: usize = 16;

/// Bounded retention-pass result over progress records only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IntegrityRetentionPage {
    next_checkpoint: Option<IntegrityJobId>,
    exhausted: bool,
    jobs_scanned: u32,
    jobs_expired: u32,
    jobs_deleted: u32,
    corrupt_jobs: Vec<IntegrityJobId>,
}

impl IntegrityRetentionPage {
    /// Return the next private retention scan checkpoint.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn next_checkpoint(&self) -> Option<IntegrityJobId> {
        self.next_checkpoint
    }

    /// Return whether the progress-record keyspace was exhausted.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn exhausted(&self) -> bool {
        self.exhausted
    }

    /// Return progress records visited by this bounded pass.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn jobs_scanned(&self) -> u32 {
        self.jobs_scanned
    }

    /// Return jobs frozen as expiry-pending by this pass.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn jobs_expired(&self) -> u32 {
        self.jobs_expired
    }

    /// Return acknowledged terminal jobs removed by this pass.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn jobs_deleted(&self) -> u32 {
        self.jobs_deleted
    }

    /// Borrow corrupt progress-record identities skipped by this pass.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn corrupt_jobs(&self) -> &[IntegrityJobId] {
        self.corrupt_jobs.as_slice()
    }
}

/// Heap-only fair-scan position for one stable progress allocation.
///
/// This is advisory scheduling state, not durable integrity authority. Losing
/// it on restart safely resumes scanning from the first progress-record key.
#[derive(Clone, Copy)]
struct IntegrityRetentionCursor {
    memory_id: u8,
    stable_key: &'static str,
    checkpoint: Option<IntegrityJobId>,
}

thread_local! {
    static INTEGRITY_RETENTION_CURSORS: RefCell<Vec<IntegrityRetentionCursor>> =
        const { RefCell::new(Vec::new()) };
}

/// Start one idempotent Deep job after an exact A/B capture handshake.
pub(in crate::db) fn start_deep_integrity_job<C: CanisterKind>(
    _db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    owner: IntegrityJobOwner,
    submission_key: IntegritySubmissionKey,
    proof_a: crate::db::integrity::IntegrityProofVector,
    proof_b: crate::db::integrity::IntegrityProofVector,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    owner.validate()?;
    submission_key.validate()?;
    if proof_a != proof_b {
        return Err(IntegrityJobError::StartInvalidated.into());
    }
    let incarnation = proof_b.database_incarnation_id();
    let id = integrity_job_id(incarnation, &owner, &submission_key)?;
    let identity = plan.identity();
    let checkpoint = IntegrityCheckpoint::QuickMetadata;
    let receipt = IntegrityJobReceipt::Page(DeepIntegrityPage {
        job_id: id,
        page_sequence: 0,
        phase: checkpoint.phase(),
        status: DeepIntegrityPageStatus::InProgress,
        pages_completed: 0,
        findings_seen: 0,
        findings: Vec::new(),
        blocked_verifier_families: Vec::new(),
    });
    let job = IntegrityJob {
        id,
        database_incarnation_id: incarnation,
        owner,
        submission_key,
        entity: IntegrityEntityIdentity::from_plan(plan),
        accepted_schema_version: identity.accepted_schema_version().get(),
        accepted_schema_fingerprint: identity.accepted_schema_fingerprint(),
        inspection_plan_fingerprint: plan.fingerprint().to_bytes(),
        checkpoint,
        captured_proof_vector: proof_b,
        state: IntegrityJobState::InProgress,
        lease_deadline_nanos: lease_deadline(now_nanos()?)?,
        findings_seen: 0,
        pages_completed: 0,
        blocked_verifier_families: Vec::new(),
        last_receipt: IntegrityReceiptEnvelope {
            replay_key: IntegrityReceiptReplayKey::Start,
            receipt: receipt.clone(),
        },
    };

    with_progress_store::<C, _>(|store| match store.insert_new(&job)? {
        InsertJobResult::Inserted => Ok(receipt),
        InsertJobResult::Occupied(existing) => replay_start(&job, *existing),
    })
    .map_err(IntegrityDeepError::from)
}

/// Continue or replay one authorized Deep job.
pub(in crate::db) fn continue_deep_integrity_job<C: CanisterKind>(
    db: &Db<C>,
    job_id: IntegrityJobId,
    owner: &IntegrityJobOwner,
    acknowledged_sequence: u64,
    load_plan: impl FnOnce(&str) -> Result<AcceptedInspectionPlan, InternalError>,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    let mut job = with_progress_store::<C, _>(|store| store.load(job_id))?;
    authorize_job(&job, owner)?;

    if job.last_receipt.replay_key
        == (IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence,
        })
    {
        return Ok(job.last_receipt.receipt.clone());
    }
    if matches!(job.state, IntegrityJobState::InProgress) && now_nanos()? > job.lease_deadline_nanos
    {
        job.state = IntegrityJobState::TerminalPending(IntegrityPendingTerminal::Expired);
        with_progress_store::<C, _>(|store| store.replace(&job))?;
    }

    match &mut job.state {
        IntegrityJobState::TerminalPending(reason)
            if acknowledged_sequence == job.last_receipt.receipt.page_sequence() =>
        {
            let reason = *reason;
            terminalize_pending::<C>(&mut job, reason, acknowledged_sequence)
        }
        IntegrityJobState::Terminal {
            receipt_acknowledged,
            ..
        } if acknowledged_sequence == job.last_receipt.receipt.page_sequence() => {
            if !*receipt_acknowledged {
                *receipt_acknowledged = true;
                with_progress_store::<C, _>(|store| store.replace(&job))?;
            }
            Ok(job.last_receipt.receipt.clone())
        }
        IntegrityJobState::InProgress
            if acknowledged_sequence == job.last_receipt.receipt.page_sequence() =>
        {
            let plan = match load_plan(job.entity.entity_path()) {
                Ok(plan) => plan,
                Err(error) => {
                    return terminalize::<C>(
                        &mut job,
                        IntegrityTerminalOutcome::Uninspectable(
                            IntegrityAuthorityDiagnostic::from_internal(&error),
                        ),
                        acknowledged_sequence,
                        Vec::new(),
                    );
                }
            };
            advance_job::<C>(db, &plan, job, acknowledged_sequence)
        }
        IntegrityJobState::TerminalPending(_)
        | IntegrityJobState::Terminal { .. }
        | IntegrityJobState::InProgress => Err(IntegrityJobError::StaleAcknowledgement.into()),
    }
}

/// Freeze an idle job for abort without overwriting its outstanding page.
pub(in crate::db) fn abort_deep_integrity_job<C: CanisterKind>(
    job_id: IntegrityJobId,
    owner: &IntegrityJobOwner,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    let mut job = with_progress_store::<C, _>(|store| store.load(job_id))?;
    authorize_job(&job, owner)?;

    match &job.state {
        IntegrityJobState::InProgress => {
            job.state = IntegrityJobState::TerminalPending(IntegrityPendingTerminal::Aborted);
            with_progress_store::<C, _>(|store| store.replace(&job))?;
            Ok(termination_pending_receipt(
                &job,
                IntegrityPendingTerminal::Aborted,
            ))
        }
        IntegrityJobState::TerminalPending(reason) => {
            Ok(termination_pending_receipt(&job, *reason))
        }
        IntegrityJobState::Terminal { .. } => Ok(job.last_receipt.receipt.clone()),
    }
}

/// Advance one fair bounded retention page for the current progress allocation.
pub(in crate::db) fn run_next_integrity_retention_page<C: CanisterKind>()
-> Result<(), IntegrityDeepError> {
    run_next_integrity_retention_page_at::<C>(now_nanos()?).map(|_| ())
}

fn run_next_integrity_retention_page_at<C: CanisterKind>(
    now: u64,
) -> Result<IntegrityRetentionPage, IntegrityDeepError> {
    let checkpoint = integrity_retention_checkpoint::<C>();
    let mut page = run_integrity_retention_page_at::<C>(checkpoint, now)?;

    // A reset progress store or another test can leave an advisory heap cursor
    // beyond every current key. Wrap immediately without decoding an extra
    // record so the maintained page still visits at most the frozen limit.
    if checkpoint.is_some() && page.exhausted && page.jobs_scanned == 0 {
        page = run_integrity_retention_page_at::<C>(None, now)?;
    }

    let next_checkpoint = if page.exhausted {
        None
    } else {
        page.next_checkpoint
    };
    set_integrity_retention_checkpoint::<C>(next_checkpoint);
    Ok(page)
}

fn run_integrity_retention_page_at<C: CanisterKind>(
    checkpoint: Option<IntegrityJobId>,
    now: u64,
) -> Result<IntegrityRetentionPage, IntegrityDeepError> {
    let scan = with_progress_store::<C, _>(|store| {
        store.scan_after(checkpoint, MAX_RETENTION_RECORDS_PER_PAGE)
    })?;
    let mut jobs_expired = 0_u32;
    let mut jobs_deleted = 0_u32;
    let mut corrupt_jobs = Vec::new();

    for job_id in &scan.job_ids {
        let loaded = with_progress_store::<C, _>(|store| store.load(*job_id));
        let mut job = match loaded {
            Ok(job) => job,
            Err(
                IntegrityJobError::CorruptProgressRecord
                | IntegrityJobError::IncompatibleProgressFormat,
            ) => {
                corrupt_jobs.push(*job_id);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match &job.state {
            IntegrityJobState::InProgress if now > job.lease_deadline_nanos => {
                job.state = IntegrityJobState::TerminalPending(IntegrityPendingTerminal::Expired);
                with_progress_store::<C, _>(|store| store.replace(&job))?;
                jobs_expired = jobs_expired
                    .checked_add(1)
                    .ok_or(IntegrityJobError::CounterExhausted)?;
            }
            IntegrityJobState::Terminal {
                receipt_acknowledged: true,
                ..
            } if now > job.lease_deadline_nanos => {
                with_progress_store::<C, _>(|store| store.remove(*job_id))?;
                jobs_deleted = jobs_deleted
                    .checked_add(1)
                    .ok_or(IntegrityJobError::CounterExhausted)?;
            }
            _ => {}
        }
    }

    Ok(IntegrityRetentionPage {
        next_checkpoint: scan.job_ids.last().copied(),
        exhausted: scan.exhausted,
        jobs_scanned: u32::try_from(scan.job_ids.len())
            .map_err(|_| IntegrityJobError::CounterExhausted)?,
        jobs_expired,
        jobs_deleted,
        corrupt_jobs,
    })
}

#[cfg(test)]
pub(in crate::db) fn run_integrity_retention_page_for_tests<C: CanisterKind>(
    checkpoint: Option<IntegrityJobId>,
    now: u64,
) -> Result<IntegrityRetentionPage, IntegrityDeepError> {
    run_integrity_retention_page_at::<C>(checkpoint, now)
}

#[cfg(test)]
pub(in crate::db) fn run_next_integrity_retention_page_for_tests<C: CanisterKind>(
    now: u64,
) -> Result<IntegrityRetentionPage, IntegrityDeepError> {
    run_next_integrity_retention_page_at::<C>(now)
}

#[cfg(test)]
pub(in crate::db) fn reset_integrity_retention_cursor_for_tests<C: CanisterKind>() {
    set_integrity_retention_checkpoint::<C>(None);
}

fn integrity_retention_checkpoint<C: CanisterKind>() -> Option<IntegrityJobId> {
    INTEGRITY_RETENTION_CURSORS.with(|cursors| {
        cursors
            .borrow()
            .iter()
            .find(|cursor| {
                cursor.memory_id == C::INTEGRITY_PROGRESS_MEMORY_ID
                    && cursor.stable_key == C::INTEGRITY_PROGRESS_STABLE_KEY
            })
            .and_then(|cursor| cursor.checkpoint)
    })
}

fn set_integrity_retention_checkpoint<C: CanisterKind>(checkpoint: Option<IntegrityJobId>) {
    INTEGRITY_RETENTION_CURSORS.with(|cursors| {
        let mut cursors = cursors.borrow_mut();
        if let Some(cursor) = cursors.iter_mut().find(|cursor| {
            cursor.memory_id == C::INTEGRITY_PROGRESS_MEMORY_ID
                && cursor.stable_key == C::INTEGRITY_PROGRESS_STABLE_KEY
        }) {
            cursor.checkpoint = checkpoint;
            return;
        }
        cursors.push(IntegrityRetentionCursor {
            memory_id: C::INTEGRITY_PROGRESS_MEMORY_ID,
            stable_key: C::INTEGRITY_PROGRESS_STABLE_KEY,
            checkpoint,
        });
    });
}

fn replay_start(
    candidate: &IntegrityJob,
    existing: IntegrityJob,
) -> Result<IntegrityJobReceipt, IntegrityJobError> {
    if candidate.database_incarnation_id != existing.database_incarnation_id
        || candidate.owner != existing.owner
        || candidate.submission_key != existing.submission_key
        || candidate.entity != existing.entity
        || candidate.accepted_schema_version != existing.accepted_schema_version
        || candidate.accepted_schema_fingerprint != existing.accepted_schema_fingerprint
        || candidate.inspection_plan_fingerprint != existing.inspection_plan_fingerprint
    {
        return Err(IntegrityJobError::SubmissionConflict);
    }
    if existing.last_receipt.replay_key != IntegrityReceiptReplayKey::Start
        || existing.last_receipt.receipt.page_sequence() != 0
    {
        return Err(IntegrityJobError::SubmissionAlreadyAdvanced);
    }
    Ok(existing.last_receipt.receipt)
}

fn authorize_job(job: &IntegrityJob, owner: &IntegrityJobOwner) -> Result<(), IntegrityDeepError> {
    if job.database_incarnation_id != database_incarnation_id()? {
        return Err(IntegrityJobError::JobIncarnationMismatch.into());
    }
    if job.owner != *owner {
        return Err(IntegrityJobError::JobOwnerMismatch.into());
    }
    Ok(())
}

fn advance_job<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    mut job: IntegrityJob,
    acknowledged_sequence: u64,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    if !plan_matches_job(plan, &job) {
        return terminalize::<C>(
            &mut job,
            IntegrityTerminalOutcome::Invalidated,
            acknowledged_sequence,
            Vec::new(),
        );
    }

    let proof_before = match capture_integrity_proof_vector(db, plan) {
        Ok(proof) => proof,
        Err(error) => {
            return terminalize::<C>(
                &mut job,
                IntegrityTerminalOutcome::Uninspectable(
                    IntegrityAuthorityDiagnostic::from_internal(&error),
                ),
                acknowledged_sequence,
                Vec::new(),
            );
        }
    };
    if proof_before != job.captured_proof_vector {
        return terminalize::<C>(
            &mut job,
            IntegrityTerminalOutcome::Invalidated,
            acknowledged_sequence,
            Vec::new(),
        );
    }

    let candidate = match execute_candidate_page(db, plan, &job) {
        Ok(candidate) => candidate,
        Err(error) => {
            let outcome = IntegrityTerminalOutcome::Uninspectable(
                IntegrityAuthorityDiagnostic::from_internal(&error),
            );
            return terminalize::<C>(&mut job, outcome, acknowledged_sequence, Vec::new());
        }
    };

    let proof_after = match capture_integrity_proof_vector(db, plan) {
        Ok(proof) => proof,
        Err(error) => {
            return terminalize::<C>(
                &mut job,
                IntegrityTerminalOutcome::Uninspectable(
                    IntegrityAuthorityDiagnostic::from_internal(&error),
                ),
                acknowledged_sequence,
                Vec::new(),
            );
        }
    };
    if proof_after != job.captured_proof_vector {
        return terminalize::<C>(
            &mut job,
            IntegrityTerminalOutcome::Invalidated,
            acknowledged_sequence,
            Vec::new(),
        );
    }

    persist_candidate::<C>(&mut job, candidate, acknowledged_sequence)
}

struct CandidatePage {
    checkpoint: IntegrityCheckpoint,
    findings: Vec<IntegrityFinding>,
    findings_increment: u64,
    blocked_verifier_families: Vec<IntegrityVerifierFamily>,
    terminal: Option<IntegrityTerminalOutcome>,
}

fn execute_candidate_page<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
) -> Result<CandidatePage, InternalError> {
    match &job.checkpoint {
        IntegrityCheckpoint::QuickMetadata => execute_quick_candidate(db, plan),
        IntegrityCheckpoint::Rows(checkpoint) => execute_row_candidate(db, plan, job, checkpoint),
        IntegrityCheckpoint::Index {
            ordinal,
            checkpoint,
        } => execute_index_candidate(db, plan, job, *ordinal, checkpoint),
        IntegrityCheckpoint::ReverseRelation {
            ordinal,
            checkpoint,
        } => execute_reverse_candidate(db, plan, job, *ordinal, checkpoint),
        IntegrityCheckpoint::Journal {
            store_ordinal,
            checkpoint,
        } => execute_journal_candidate(db, job, *store_ordinal, checkpoint),
        IntegrityCheckpoint::FinalProof => Ok(final_candidate(job)),
    }
}

fn execute_quick_candidate<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
) -> Result<CandidatePage, InternalError> {
    let quick = execute_quick_integrity(db, plan)?;
    let terminal = match quick.status() {
        QuickIntegrityStatus::CompleteClean | QuickIntegrityStatus::CompleteWithFindings => None,
        QuickIntegrityStatus::Uninspectable(diagnostic) => {
            Some(IntegrityTerminalOutcome::Uninspectable(diagnostic.clone()))
        }
        QuickIntegrityStatus::ResourceLimited(diagnostic) => Some(
            IntegrityTerminalOutcome::ResourceLimited(diagnostic.clone()),
        ),
    };
    Ok(CandidatePage {
        checkpoint: if terminal.is_some() {
            IntegrityCheckpoint::QuickMetadata
        } else {
            IntegrityCheckpoint::Rows(PhysicalUnitCheckpoint::BeforeFirst)
        },
        findings: quick.findings().to_vec(),
        findings_increment: quick.total_findings(),
        blocked_verifier_families: Vec::new(),
        terminal,
    })
}

fn execute_row_candidate<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
    checkpoint: &PhysicalUnitCheckpoint,
) -> Result<CandidatePage, InternalError> {
    let page = execute_row_integrity_page(
        db,
        plan,
        checkpoint.clone(),
        RowInspectionLimits::standard(),
    )?;
    let next = if page.exhausted() {
        checkpoint_after_rows(plan, job)
    } else {
        IntegrityCheckpoint::Rows(page.checkpoint().clone())
    };
    Ok(CandidatePage {
        checkpoint: next,
        findings_increment: finding_count(page.findings())?,
        findings: page.findings().to_vec(),
        blocked_verifier_families: page.blocked_verifier_families().to_vec(),
        terminal: None,
    })
}

fn execute_index_candidate<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
    ordinal: u32,
    checkpoint: &PhysicalUnitCheckpoint,
) -> Result<CandidatePage, InternalError> {
    let ordinal_usize = usize::try_from(ordinal).map_err(|_| InternalError::store_invariant())?;
    let page = execute_index_integrity_page(
        db,
        plan,
        ordinal_usize,
        checkpoint.clone(),
        DerivedInspectionLimits::standard(),
    )?;
    let next = if page.exhausted() {
        checkpoint_after_index(plan, job, ordinal_usize)?
    } else {
        IntegrityCheckpoint::Index {
            ordinal,
            checkpoint: page.checkpoint().clone(),
        }
    };
    derived_candidate(next, page.findings())
}

fn execute_reverse_candidate<C: CanisterKind>(
    db: &Db<C>,
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
    ordinal: u32,
    checkpoint: &PhysicalUnitCheckpoint,
) -> Result<CandidatePage, InternalError> {
    let ordinal_usize = usize::try_from(ordinal).map_err(|_| InternalError::store_invariant())?;
    let page = execute_reverse_integrity_page(
        db,
        plan,
        ordinal_usize,
        checkpoint.clone(),
        DerivedInspectionLimits::standard(),
    )?;
    let next = if page.exhausted() {
        checkpoint_after_reverse(plan, job, ordinal_usize)?
    } else {
        IntegrityCheckpoint::ReverseRelation {
            ordinal,
            checkpoint: page.checkpoint().clone(),
        }
    };
    derived_candidate(next, page.findings())
}

fn execute_journal_candidate<C: CanisterKind>(
    db: &Db<C>,
    job: &IntegrityJob,
    store_ordinal: u32,
    checkpoint: &JournalInspectionCheckpoint,
) -> Result<CandidatePage, InternalError> {
    let ordinal = usize::try_from(store_ordinal).map_err(|_| InternalError::store_invariant())?;
    let store_proof = job
        .captured_proof_vector
        .stores()
        .get(ordinal)
        .ok_or_else(InternalError::store_invariant)?;
    let handle = db.store_handle(store_proof.store_path())?;
    let journal = handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_unsupported)?;
    let page = journal.with_borrow(|journal| {
        journal.inspect_page(checkpoint.clone(), JournalInspectionLimits::standard())
    })?;
    let next = if page.exhausted() {
        checkpoint_after_journal(job, ordinal)?
    } else {
        IntegrityCheckpoint::Journal {
            store_ordinal,
            checkpoint: page.checkpoint().clone(),
        }
    };
    let findings = page
        .issue()
        .map(|issue| journal_integrity_finding(job, store_proof.store_path(), issue))
        .into_iter()
        .collect::<Vec<_>>();
    let blocked_verifier_families = page
        .batch_identity_blocked()
        .then_some(IntegrityVerifierFamily::JournalBatchIdentity)
        .into_iter()
        .collect();
    Ok(CandidatePage {
        checkpoint: next,
        findings_increment: finding_count(&findings)?,
        findings,
        blocked_verifier_families,
        terminal: None,
    })
}

fn journal_integrity_finding(
    job: &IntegrityJob,
    store_path: &str,
    issue: JournalIntegrityIssue,
) -> IntegrityFinding {
    let (diagnostic_code, class, kind, verifier_family, sequence) = match issue {
        JournalIntegrityIssue::MalformedBatch {
            sequence,
            diagnostic_code,
            incompatible_format,
        } => (
            diagnostic_code,
            if incompatible_format {
                IntegrityFindingClass::IncompatiblePersistedFormat
            } else {
                IntegrityFindingClass::Corruption
            },
            IntegrityFindingKind::MalformedJournalBatch,
            IntegrityVerifierFamily::JournalEnvelope,
            sequence,
        ),
        JournalIntegrityIssue::SequenceGap {
            expected_sequence, ..
        } => (
            icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
            IntegrityFindingClass::Corruption,
            IntegrityFindingKind::JournalSequenceGap,
            IntegrityVerifierFamily::JournalEnvelope,
            expected_sequence,
        ),
        JournalIntegrityIssue::DuplicateBatchIdentity { sequence, .. } => (
            icydb_diagnostic_code::ErrorCode::STORE_CORRUPTION.raw(),
            IntegrityFindingClass::Corruption,
            IntegrityFindingKind::DuplicateJournalBatchIdentity,
            IntegrityVerifierFamily::JournalBatchIdentity,
            sequence,
        ),
    };
    IntegrityFinding {
        diagnostic_code,
        class,
        severity: IntegritySeverity::Error,
        kind,
        entity: job.entity.clone(),
        store_path: store_path.to_owned(),
        phase: IntegrityPhase::JournalTails,
        verifier_family,
        physical_key: sequence.to_be_bytes().to_vec(),
        primary_key: None,
        field_paths: Vec::new(),
        constraint_id: None,
        constraint_name: None,
        schema_index_id: None,
        relation_id: None,
        expected: None,
        observed: None,
    }
}

fn derived_candidate(
    checkpoint: IntegrityCheckpoint,
    findings: &[IntegrityFinding],
) -> Result<CandidatePage, InternalError> {
    Ok(CandidatePage {
        checkpoint,
        findings_increment: finding_count(findings)?,
        findings: findings.to_vec(),
        blocked_verifier_families: Vec::new(),
        terminal: None,
    })
}

fn finding_count(findings: &[IntegrityFinding]) -> Result<u64, InternalError> {
    u64::try_from(findings.len()).map_err(|_| InternalError::store_invariant())
}

const fn final_candidate(job: &IntegrityJob) -> CandidatePage {
    let outcome = if job.findings_seen == 0 && job.blocked_verifier_families.is_empty() {
        IntegrityTerminalOutcome::DeepCompleteClean
    } else {
        IntegrityTerminalOutcome::DeepCompleteWithFindings
    };
    CandidatePage {
        checkpoint: IntegrityCheckpoint::FinalProof,
        findings: Vec::new(),
        findings_increment: 0,
        blocked_verifier_families: Vec::new(),
        terminal: Some(outcome),
    }
}

const fn checkpoint_after_rows(
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
) -> IntegrityCheckpoint {
    if plan.index_inspection().len() > 0 {
        return IntegrityCheckpoint::Index {
            ordinal: 0,
            checkpoint: PhysicalUnitCheckpoint::BeforeFirst,
        };
    }
    checkpoint_before_reverse_or_journal(plan, job)
}

fn checkpoint_after_index(
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
    ordinal: usize,
) -> Result<IntegrityCheckpoint, InternalError> {
    let next = ordinal
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    if next < plan.index_inspection().len() {
        return Ok(IntegrityCheckpoint::Index {
            ordinal: u32::try_from(next).map_err(|_| InternalError::store_invariant())?,
            checkpoint: PhysicalUnitCheckpoint::BeforeFirst,
        });
    }
    Ok(checkpoint_before_reverse_or_journal(plan, job))
}

const fn checkpoint_before_reverse_or_journal(
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
) -> IntegrityCheckpoint {
    if !plan.snapshot().persisted_snapshot().relations().is_empty() {
        return IntegrityCheckpoint::ReverseRelation {
            ordinal: 0,
            checkpoint: PhysicalUnitCheckpoint::BeforeFirst,
        };
    }
    checkpoint_before_journal(job)
}

fn checkpoint_after_reverse(
    plan: &AcceptedInspectionPlan,
    job: &IntegrityJob,
    ordinal: usize,
) -> Result<IntegrityCheckpoint, InternalError> {
    let next = ordinal
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    if next < plan.snapshot().persisted_snapshot().relations().len() {
        return Ok(IntegrityCheckpoint::ReverseRelation {
            ordinal: u32::try_from(next).map_err(|_| InternalError::store_invariant())?,
            checkpoint: PhysicalUnitCheckpoint::BeforeFirst,
        });
    }
    Ok(checkpoint_before_journal(job))
}

const fn checkpoint_before_journal(job: &IntegrityJob) -> IntegrityCheckpoint {
    if job.captured_proof_vector.stores().is_empty() {
        return IntegrityCheckpoint::FinalProof;
    }
    IntegrityCheckpoint::Journal {
        store_ordinal: 0,
        checkpoint: JournalInspectionCheckpoint::BeforeFirst,
    }
}

fn checkpoint_after_journal(
    job: &IntegrityJob,
    ordinal: usize,
) -> Result<IntegrityCheckpoint, InternalError> {
    let next = ordinal
        .checked_add(1)
        .ok_or_else(InternalError::store_invariant)?;
    if next < job.captured_proof_vector.stores().len() {
        return Ok(IntegrityCheckpoint::Journal {
            store_ordinal: u32::try_from(next).map_err(|_| InternalError::store_invariant())?,
            checkpoint: JournalInspectionCheckpoint::BeforeFirst,
        });
    }
    Ok(IntegrityCheckpoint::FinalProof)
}

fn persist_candidate<C: CanisterKind>(
    job: &mut IntegrityJob,
    candidate: CandidatePage,
    acknowledged_sequence: u64,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    if job.pages_completed >= MAX_INTEGRITY_IN_PROGRESS_PAGES {
        return terminalize::<C>(
            job,
            counter_exhausted_outcome(),
            acknowledged_sequence,
            Vec::new(),
        );
    }
    let next_sequence = next_page_sequence(job)?;
    let next_pages = job
        .pages_completed
        .checked_add(1)
        .ok_or(IntegrityJobError::CounterExhausted)?;
    let Some(findings_seen) = job.findings_seen.checked_add(candidate.findings_increment) else {
        return terminalize::<C>(
            job,
            counter_exhausted_outcome(),
            acknowledged_sequence,
            Vec::new(),
        );
    };
    merge_blocked_families(
        &mut job.blocked_verifier_families,
        &candidate.blocked_verifier_families,
    );
    job.checkpoint = candidate.checkpoint;
    job.pages_completed = next_pages;
    job.findings_seen = findings_seen;
    job.lease_deadline_nanos = lease_deadline(now_nanos()?)?;

    if let Some(outcome) = candidate.terminal {
        return terminalize_at_sequence::<C>(
            job,
            outcome,
            acknowledged_sequence,
            next_sequence,
            candidate.findings,
        );
    }

    let receipt = IntegrityJobReceipt::Page(DeepIntegrityPage {
        job_id: job.id,
        page_sequence: next_sequence,
        phase: job.checkpoint.phase(),
        status: DeepIntegrityPageStatus::InProgress,
        pages_completed: next_pages,
        findings_seen,
        findings: candidate.findings,
        blocked_verifier_families: job.blocked_verifier_families.clone(),
    });
    job.last_receipt = IntegrityReceiptEnvelope {
        replay_key: IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence,
        },
        receipt: receipt.clone(),
    };
    with_progress_store::<C, _>(|store| store.replace(job))?;
    Ok(receipt)
}

fn terminalize<C: CanisterKind>(
    job: &mut IntegrityJob,
    outcome: IntegrityTerminalOutcome,
    acknowledged_sequence: u64,
    findings: Vec<IntegrityFinding>,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    let sequence = next_page_sequence(job)?;
    job.pages_completed = job
        .pages_completed
        .checked_add(1)
        .ok_or(IntegrityJobError::CounterExhausted)?;
    terminalize_at_sequence::<C>(job, outcome, acknowledged_sequence, sequence, findings)
}

fn terminalize_at_sequence<C: CanisterKind>(
    job: &mut IntegrityJob,
    outcome: IntegrityTerminalOutcome,
    acknowledged_sequence: u64,
    sequence: u64,
    findings: Vec<IntegrityFinding>,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    let receipt = IntegrityJobReceipt::Page(DeepIntegrityPage {
        job_id: job.id,
        page_sequence: sequence,
        phase: job.checkpoint.phase(),
        status: DeepIntegrityPageStatus::Terminal(outcome.clone()),
        pages_completed: job.pages_completed,
        findings_seen: job.findings_seen,
        findings,
        blocked_verifier_families: job.blocked_verifier_families.clone(),
    });
    job.state = IntegrityJobState::Terminal {
        outcome,
        receipt_acknowledged: false,
    };
    job.lease_deadline_nanos = terminal_retention_deadline(now_nanos()?)?;
    job.last_receipt = IntegrityReceiptEnvelope {
        replay_key: IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence,
        },
        receipt: receipt.clone(),
    };
    with_progress_store::<C, _>(|store| store.replace(job))?;
    Ok(receipt)
}

fn terminalize_pending<C: CanisterKind>(
    job: &mut IntegrityJob,
    pending: IntegrityPendingTerminal,
    acknowledged_sequence: u64,
) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
    let outcome = match pending {
        IntegrityPendingTerminal::Expired => IntegrityTerminalOutcome::Expired,
        IntegrityPendingTerminal::Aborted => IntegrityTerminalOutcome::Aborted,
    };
    let sequence = next_page_sequence(job)?;
    job.pages_completed = job
        .pages_completed
        .checked_add(1)
        .ok_or(IntegrityJobError::CounterExhausted)?;
    let receipt = IntegrityJobReceipt::Abort(IntegrityAbortReceipt {
        job_id: job.id,
        page_sequence: sequence,
        status: IntegrityAbortStatus::Terminal(outcome.clone()),
    });
    job.state = IntegrityJobState::Terminal {
        outcome,
        receipt_acknowledged: false,
    };
    job.lease_deadline_nanos = terminal_retention_deadline(now_nanos()?)?;
    job.last_receipt = IntegrityReceiptEnvelope {
        replay_key: IntegrityReceiptReplayKey::Continue {
            acknowledged_sequence,
        },
        receipt: receipt.clone(),
    };
    with_progress_store::<C, _>(|store| store.replace(job))?;
    Ok(receipt)
}

const fn termination_pending_receipt(
    job: &IntegrityJob,
    reason: IntegrityPendingTerminal,
) -> IntegrityJobReceipt {
    IntegrityJobReceipt::Abort(IntegrityAbortReceipt {
        job_id: job.id,
        page_sequence: job.last_receipt.receipt.page_sequence(),
        status: IntegrityAbortStatus::TerminationPending(reason),
    })
}

fn next_page_sequence(job: &IntegrityJob) -> Result<u64, IntegrityDeepError> {
    job.last_receipt
        .receipt
        .page_sequence()
        .checked_add(1)
        .ok_or_else(|| IntegrityJobError::CounterExhausted.into())
}

const fn counter_exhausted_outcome() -> IntegrityTerminalOutcome {
    IntegrityTerminalOutcome::ResourceLimited(IntegrityResourceDiagnostic {
        diagnostic_code: icydb_diagnostic_code::ErrorCode::RUNTIME_INTERNAL.raw(),
    })
}

fn merge_blocked_families(
    accumulated: &mut Vec<IntegrityVerifierFamily>,
    page: &[IntegrityVerifierFamily],
) {
    accumulated.extend_from_slice(page);
    accumulated.sort();
    accumulated.dedup();
}

fn plan_matches_job(plan: &AcceptedInspectionPlan, job: &IntegrityJob) -> bool {
    let identity = plan.identity();
    let checkpoint_matches = match &job.checkpoint {
        IntegrityCheckpoint::QuickMetadata
        | IntegrityCheckpoint::Rows(_)
        | IntegrityCheckpoint::FinalProof => true,
        IntegrityCheckpoint::Index { ordinal, .. } => {
            usize::try_from(*ordinal).is_ok_and(|ordinal| ordinal < plan.index_inspection().len())
        }
        IntegrityCheckpoint::ReverseRelation { ordinal, .. } => usize::try_from(*ordinal)
            .is_ok_and(|ordinal| ordinal < plan.snapshot().persisted_snapshot().relations().len()),
        IntegrityCheckpoint::Journal { store_ordinal, .. } => usize::try_from(*store_ordinal)
            .is_ok_and(|ordinal| ordinal < job.captured_proof_vector.stores().len()),
    };

    checkpoint_matches
        && identity.entity_tag().value() == job.entity.entity_tag()
        && identity.entity_path() == job.entity.entity_path()
        && identity.store_path() == job.entity.store_path()
        && identity.accepted_schema_version().get() == job.accepted_schema_version
        && identity.accepted_schema_fingerprint() == job.accepted_schema_fingerprint
        && plan.fingerprint().to_bytes() == job.inspection_plan_fingerprint
}

fn integrity_job_id(
    incarnation: crate::db::integrity::DatabaseIncarnationId,
    owner: &IntegrityJobOwner,
    submission_key: &IntegritySubmissionKey,
) -> Result<IntegrityJobId, IntegrityJobError> {
    let mut hasher = Sha256::new();
    hasher.update(INTEGRITY_JOB_ID_DOMAIN);
    hasher.update(incarnation.to_bytes());
    write_len_prefixed(&mut hasher, owner.as_str())?;
    write_len_prefixed(&mut hasher, submission_key.as_str())?;
    IntegrityJobId::try_from_bytes(hasher.finalize().into())
}

fn write_len_prefixed(hasher: &mut Sha256, value: &str) -> Result<(), IntegrityJobError> {
    let len = u32::try_from(value.len()).map_err(|_| IntegrityJobError::Internal)?;
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn lease_deadline(now: u64) -> Result<u64, IntegrityJobError> {
    now.checked_add(INTEGRITY_JOB_LEASE_NANOS)
        .ok_or(IntegrityJobError::CounterExhausted)
}

fn terminal_retention_deadline(now: u64) -> Result<u64, IntegrityJobError> {
    now.checked_add(INTEGRITY_TERMINAL_RETENTION_NANOS)
        .ok_or(IntegrityJobError::CounterExhausted)
}

#[cfg(target_arch = "wasm32")]
fn now_nanos() -> Result<u64, IntegrityJobError> {
    Ok(ic_cdk::api::time())
}

#[cfg(not(target_arch = "wasm32"))]
fn now_nanos() -> Result<u64, IntegrityJobError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IntegrityJobError::Internal)?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| IntegrityJobError::CounterExhausted)
}
