//! Module: session::resumable_job
//! Responsibility: idempotent proof-checked application progress advancement.
//! Does not own: application authorization, accumulator meaning, or page selection.
//! Boundary: one synchronous operation closure -> excluded durable progress record.

use crate::{
    db::{
        CompareProofAndAdvanceError, DbSession, ExhaustiveReadError, ReadSetRevisionProof,
        ResumableJobAdvance, ResumableJobAdvanceReceipt, ResumableJobAdvanceRequest,
        ResumableJobError, ResumableJobId, ResumableJobRecord, ResumableJobState,
        ResumableJobStatus,
        executor::budget::{ExecutionBudgetExceeded, HardExecutionContext},
        integrity::with_resumable_progress_store,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

const RESUMABLE_JOB_START_SHAPE: u64 = 0x7265_7375_6d65_0101;
const RESUMABLE_JOB_LOAD_SHAPE: u64 = 0x7265_7375_6d65_0102;
const RESUMABLE_JOB_ADVANCE_SHAPE: u64 = 0x7265_7375_6d65_0103;
const RESUMABLE_JOB_ACKNOWLEDGE_SHAPE: u64 = 0x7265_7375_6d65_0104;

impl<C: CanisterKind> DbSession<C> {
    /// Create one durable application-owned job in IcyDB's excluded progress
    /// domain. Every protected source store must be journaled.
    pub fn start_resumable_job(
        &self,
        job_id: ResumableJobId,
        proof: ReadSetRevisionProof,
        initial_application_state: Vec<u8>,
    ) -> Result<ResumableJobState, ResumableJobError> {
        self.charge_resumable_operation(
            DiagnosticExecutionLane::Mutation,
            RESUMABLE_JOB_START_SHAPE,
        )?;
        self.verify_durable_read_set_revision_proof(&proof)
            .map_err(map_exhaustive_error)?;
        let record = ResumableJobRecord::new(job_id, proof, initial_application_state)?;
        with_resumable_progress_store::<C, _>(|store| store.insert_resumable(&record))?;
        Ok(record.state().clone())
    }

    /// Load the bounded current application state for one retained job.
    pub fn resumable_job_state(
        &self,
        job_id: ResumableJobId,
    ) -> Result<ResumableJobState, ResumableJobError> {
        self.charge_resumable_operation(
            DiagnosticExecutionLane::TrustedRead,
            RESUMABLE_JOB_LOAD_SHAPE,
        )?;
        with_resumable_progress_store::<C, _>(|store| store.load_resumable(job_id))
            .map(|record| record.state().clone())
    }

    /// Remove one completed or invalidated job after the application has
    /// durably consumed its terminal result.
    ///
    /// The expected sequence prevents acknowledgement of a replaced state.
    /// Repeating an acknowledgement after a lost reply succeeds when the job
    /// is already absent. Active jobs with remaining continuation fail closed.
    pub fn acknowledge_resumable_job(
        &self,
        job_id: ResumableJobId,
        expected_sequence: u64,
    ) -> Result<(), ResumableJobError> {
        self.charge_resumable_operation(
            DiagnosticExecutionLane::Mutation,
            RESUMABLE_JOB_ACKNOWLEDGE_SHAPE,
        )?;
        with_resumable_progress_store::<C, _>(|store| {
            let record = match store.load_resumable(job_id) {
                Ok(record) => record,
                Err(ResumableJobError::NotFound) => return Ok(()),
                Err(error) => return Err(error),
            };
            if record.state().sequence != expected_sequence {
                return Err(ResumableJobError::StaleSequence {
                    expected: expected_sequence,
                    actual: record.state().sequence,
                });
            }
            let terminal = record.state().sequence > 0
                && matches!(
                    record.state().status,
                    ResumableJobStatus::Completed | ResumableJobStatus::Invalidated
                );
            if !terminal {
                return Err(ResumableJobError::NotTerminal);
            }
            store.remove_resumable(job_id)
        })
    }

    /// Execute at most one application page and atomically retain its next
    /// state after rechecking the complete source proof.
    ///
    /// Replaying the same sequence and idempotency key returns the persisted
    /// receipt without invoking `operation` again. The closure is synchronous;
    /// no `.await`, timer, or external call can split proof comparison from the
    /// final progress write.
    pub fn compare_proof_and_advance<E>(
        &self,
        request: &ResumableJobAdvanceRequest,
        operation: impl FnOnce(&ResumableJobState) -> Result<ResumableJobAdvance, E>,
    ) -> Result<ResumableJobAdvanceReceipt, CompareProofAndAdvanceError<E>> {
        self.charge_resumable_operation(
            DiagnosticExecutionLane::Mutation,
            RESUMABLE_JOB_ADVANCE_SHAPE,
        )?;
        request.idempotency_key.validate()?;
        if request.job_id.to_bytes() == [0; 32] {
            return Err(ResumableJobError::InvalidJobId.into());
        }

        let record =
            with_resumable_progress_store::<C, _>(|store| store.load_resumable(request.job_id))?;
        if let Some(receipt) = exact_replay(&record, request) {
            return Ok(receipt.clone());
        }
        ensure_request_can_advance(&record, request)?;

        if let Err(error) = self.verify_read_set_revision_proof(&record.state().proof) {
            return Self::persist_or_report_pre_page_invalidation(record, request, error);
        }

        let advance = operation(record.state()).map_err(CompareProofAndAdvanceError::Operation)?;
        advance.validate()?;

        if let Err(error) = self.verify_read_set_revision_proof(&record.state().proof) {
            return Self::persist_or_report_post_page_invalidation(record, request, error);
        }

        let (candidate, receipt) = record.apply_advance(request, advance)?;
        Self::replace_resumable_job_if_current(request, &candidate)?;
        Ok(receipt)
    }

    fn persist_or_report_pre_page_invalidation<E>(
        record: ResumableJobRecord,
        request: &ResumableJobAdvanceRequest,
        error: ExhaustiveReadError,
    ) -> Result<ResumableJobAdvanceReceipt, CompareProofAndAdvanceError<E>> {
        Self::persist_or_report_invalidation(record, request, error)
    }

    fn persist_or_report_post_page_invalidation<E>(
        record: ResumableJobRecord,
        request: &ResumableJobAdvanceRequest,
        error: ExhaustiveReadError,
    ) -> Result<ResumableJobAdvanceReceipt, CompareProofAndAdvanceError<E>> {
        Self::persist_or_report_invalidation(record, request, error)
    }

    fn persist_or_report_invalidation<E>(
        record: ResumableJobRecord,
        request: &ResumableJobAdvanceRequest,
        error: ExhaustiveReadError,
    ) -> Result<ResumableJobAdvanceReceipt, CompareProofAndAdvanceError<E>> {
        match error {
            ExhaustiveReadError::Revision(error) if error.is_source_change() => {
                let (invalidated, receipt) = record.invalidate(request)?;
                Self::replace_resumable_job_if_current(request, &invalidated)?;
                Ok(receipt)
            }
            other => Err(map_exhaustive_error(other).into()),
        }
    }

    fn replace_resumable_job_if_current<E>(
        request: &ResumableJobAdvanceRequest,
        candidate: &ResumableJobRecord,
    ) -> Result<(), CompareProofAndAdvanceError<E>> {
        with_resumable_progress_store::<C, _>(|store| {
            let current = store.load_resumable(request.job_id)?;
            if exact_replay(&current, request).is_some() {
                return Err(ResumableJobError::StaleSequence {
                    expected: request.expected_sequence,
                    actual: current.state().sequence,
                });
            }
            ensure_request_can_advance(&current, request)?;
            store.replace_resumable(candidate)
        })?;
        Ok(())
    }

    fn charge_resumable_operation(
        &self,
        lane: DiagnosticExecutionLane,
        shape: u64,
    ) -> Result<(), ResumableJobError> {
        self.db
            .request_execution_scope()
            .charge(
                HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, shape),
                DiagnosticExecutionBudgetResource::QueryExecutions,
                1,
            )
            .map_err(resumable_execution_budget_error)
    }
}

fn exact_replay<'a>(
    record: &'a ResumableJobRecord,
    request: &ResumableJobAdvanceRequest,
) -> Option<&'a ResumableJobAdvanceReceipt> {
    record.last_receipt().filter(|receipt| {
        receipt.request_sequence == request.expected_sequence
            && receipt.idempotency_key() == &request.idempotency_key
    })
}

fn ensure_request_can_advance(
    record: &ResumableJobRecord,
    request: &ResumableJobAdvanceRequest,
) -> Result<(), ResumableJobError> {
    if record.state().status == ResumableJobStatus::Invalidated {
        return Err(ResumableJobError::Invalidated);
    }
    if record.state().status == ResumableJobStatus::Completed {
        return Err(ResumableJobError::Completed);
    }
    if record.state().sequence != request.expected_sequence {
        return Err(ResumableJobError::StaleSequence {
            expected: request.expected_sequence,
            actual: record.state().sequence,
        });
    }
    Ok(())
}

fn map_exhaustive_error(error: ExhaustiveReadError) -> ResumableJobError {
    match error {
        ExhaustiveReadError::Revision(error) => ResumableJobError::SourceProof(error),
        ExhaustiveReadError::Query(_) => ResumableJobError::Internal,
    }
}

const fn resumable_execution_budget_error(error: ExecutionBudgetExceeded) -> ResumableJobError {
    ResumableJobError::ExecutionBudgetExceeded {
        resource: error.resource().raw(),
        limit: error.limit(),
        observed: error.observed(),
        scope: error.scope().raw(),
        lane: error.lane().raw(),
        normalized_shape_fingerprint_prefix: error.normalized_shape_fingerprint_prefix(),
    }
}
