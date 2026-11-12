//! Module: session::mutation_job
//! Responsibility: charged mutation-job start, state load, phase dispatch, and terminal acknowledgement.
//! Does not own: SQL lowering, Forward/Verify execution, or authorization.
//! Boundary: trusted session API -> excluded mutation progress record.

use crate::{
    db::{
        DbSession, MutationJobError, MutationJobId, MutationJobState, ProgressJobInventory,
        ProgressRetirementRecord,
        integrity::with_mutation_progress_store,
        resource::budget::{ExecutionBudgetExceeded, HardExecutionContext},
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
};

#[cfg(feature = "sql")]
use crate::db::{
    MutationJobAdvanceReceipt, MutationJobAdvanceRequest,
    executor::mutation::job::{
        advance_mutation_job, validate_current_initial_mutation_job_continuation,
    },
    integrity::InsertMutationJobResult,
    mutation_job::{CanonicalMutationIntent, MutationJobRecord},
    resource::budget::with_mutation_execution_budget,
};

#[cfg(feature = "sql")]
const MUTATION_JOB_START_SHAPE: u64 = 0x6d75_7461_7465_0100;
const MUTATION_JOB_LOAD_SHAPE: u64 = 0x6d75_7461_7465_0101;
const MUTATION_JOB_ACKNOWLEDGE_SHAPE: u64 = 0x6d75_7461_7465_0102;
#[cfg(feature = "sql")]
const MUTATION_JOB_ADVANCE_SHAPE: u64 = 0x6d75_7461_7465_0103;
#[cfg(feature = "sql")]
const MUTATION_JOB_CANCEL_UNADVANCED_SHAPE: u64 = 0x6d75_7461_7465_0104;
const PROGRESS_JOB_INVENTORY_SHAPE: u64 = 0x6d75_7461_7465_0105;

impl<C: CanisterKind> DbSession<C> {
    /// Start one durable trusted fixed SQL mutation job.
    ///
    /// SQL is parsed and admitted exactly once for a new identity. The
    /// catalog-native intent and initial engine checkpoint are durably retained
    /// before this method returns, and no target row is read or mutated.
    /// Repeating the same canonical request returns the retained state without
    /// replacing its statement timestamp or resetting progress.
    #[cfg(feature = "sql")]
    pub fn start_trusted_sql_mutation_job(
        &self,
        job_id: MutationJobId,
        sql: &str,
    ) -> Result<MutationJobState, MutationJobError> {
        job_id.validate()?;
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_START_SHAPE,
        )?;
        let prepared = self.prepare_mutation_job_start(job_id, sql)?;
        let submitted_intent = CanonicalMutationIntent::decode(&prepared.canonical_intent)?;
        let submitted = MutationJobRecord::new(
            job_id,
            prepared.canonical_intent,
            prepared.engine_continuation,
        )?;
        with_mutation_progress_store::<C, _>(|store| match store.insert_mutation(&submitted)? {
            InsertMutationJobResult::Inserted => Ok(submitted.state().clone()),
            InsertMutationJobResult::Occupied(retained) => {
                resolve_occupied_mutation_job_start(&retained, &submitted_intent)
            }
        })
    }

    /// Load the bounded public state for one retained mutation job.
    pub fn mutation_job_state(
        &self,
        job_id: MutationJobId,
    ) -> Result<MutationJobState, MutationJobError> {
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::TrustedRead,
            MUTATION_JOB_LOAD_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| store.load_mutation(job_id))
            .map(|record| record.state().clone())
    }

    /// Advance one durable mutation job through one bounded engine-owned step.
    ///
    /// The request carries only job identity, expected sequence, and a replay
    /// key. SQL and continuation bytes remain private IcyDB custody.
    /// Catalog admission or inspection failure returns
    /// [`MutationJobError::TargetQueryFailed`] without changing retained progress;
    /// retry the same request after the obstruction is resolved. Successfully
    /// inspected missing or changed schema authority requires a terminal restart.
    /// Execution-budget policy exhaustion retains its terminal restart reason.
    #[cfg(feature = "sql")]
    pub fn advance_trusted_mutation_job(
        &self,
        request: &MutationJobAdvanceRequest,
    ) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
        let retained =
            with_mutation_progress_store::<C, _>(|store| store.load_mutation(request.job_id))?;
        if let Some(receipt) = retained.exact_replay(request)? {
            return Ok(receipt.clone());
        }
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_ADVANCE_SHAPE,
        )?;
        retained.ensure_can_advance(request)?;
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_ADVANCE_SHAPE,
        );
        with_mutation_execution_budget(
            context,
            || {
                advance_mutation_job(&self.db, &retained, request, |entity_source| {
                    self.find_accepted_schema_catalog_context_for_entity_source_key(entity_source)
                })
            },
            |error| MutationJobError::from_internal_error(&error),
        )
    }

    /// Remove one terminal mutation job after its result has been consumed.
    ///
    /// Repeating acknowledgement after a lost response succeeds when the job
    /// is already absent. Active jobs and stale terminal sequences fail closed.
    /// Recovery admission must succeed before retained progress can be removed.
    pub fn acknowledge_mutation_job(
        &self,
        job_id: MutationJobId,
        expected_terminal_sequence: u64,
    ) -> Result<(), MutationJobError> {
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_ACKNOWLEDGE_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| {
            store.acknowledge_mutation(job_id, expected_terminal_sequence)
        })
    }

    /// Idempotently remove one exact initial mutation-job record.
    ///
    /// Cancellation is available only before any page or receipt exists. A
    /// logical restart must use a fresh [`MutationJobId`]; absent-record
    /// success never makes an old identity reusable.
    /// Recovery admission must succeed before retained progress can be removed.
    #[cfg(feature = "sql")]
    pub fn cancel_unadvanced_mutation_job(
        &self,
        job_id: MutationJobId,
        expected_sequence: u64,
    ) -> Result<(), MutationJobError> {
        job_id.validate()?;
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            MUTATION_JOB_CANCEL_UNADVANCED_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| {
            store.cancel_unadvanced_mutation(
                job_id,
                expected_sequence,
                validate_current_initial_mutation_job_continuation,
            )
        })
    }

    /// Return one complete fail-closed inventory of shared retained progress.
    ///
    /// Callers remain responsible for authorization. The result exposes only
    /// family, job identity, bounded lifecycle, sequence, and capacity facts.
    pub fn progress_job_inventory(&self) -> Result<ProgressJobInventory, MutationJobError> {
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::TrustedRead,
            PROGRESS_JOB_INVENTORY_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| store.inventory())
    }

    /// Inspect bounded raw progress records for explicit administrative retirement.
    ///
    /// Applications must enforce controller or equivalent administration authority.
    /// Unlike the validated job inventory this also identifies corrupt records,
    /// without exposing payloads. It never advances or deletes retained work.
    pub fn admin_progress_retirement_inventory(
        &self,
    ) -> Result<Vec<ProgressRetirementRecord>, MutationJobError> {
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::TrustedRead,
            PROGRESS_JOB_INVENTORY_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| store.retirement_inventory())
    }

    /// Explicitly abandon one exact retained progress record of any family.
    ///
    /// Applications must enforce controller or equivalent administration authority.
    /// This permanently discards progress, outstanding receipts and replay evidence;
    /// it does not undo committed mutations or application effects. Allocate fresh
    /// job identities afterward. Normal owner acknowledgement remains preferable.
    ///
    /// # Errors
    ///
    /// Recovery must already be complete. Returns `IdentityConflict` if inspection
    /// evidence is stale, or `InvalidJobId` for the protected header. Absence is
    /// idempotent and returns false. Corrupt payloads can be explicitly retired.
    pub fn admin_retire_progress_record(
        &self,
        record: &ProgressRetirementRecord,
    ) -> Result<bool, MutationJobError> {
        self.admit_mutation_job_operation(
            DiagnosticExecutionLane::Mutation,
            PROGRESS_JOB_INVENTORY_SHAPE,
        )?;
        with_mutation_progress_store::<C, _>(|store| store.retire_exact(record))
    }

    fn admit_mutation_job_operation(
        &self,
        lane: DiagnosticExecutionLane,
        shape: u64,
    ) -> Result<(), MutationJobError> {
        self.db
            .request_execution_scope()
            .charge(
                HardExecutionContext::new(DiagnosticExecutionBudgetScope::Execution, lane, shape),
                DiagnosticExecutionBudgetResource::QueryExecutions,
                1,
            )
            .map_err(mutation_job_execution_budget_error)?;
        // Every progress writer shares control admission. Observation and exact
        // replay remain state-only; this check never drives pending recovery.
        if lane == DiagnosticExecutionLane::Mutation {
            self.db
                .ensure_recovered_control_state()
                .map_err(|_| MutationJobError::TargetQueryFailed)?;
        }
        Ok(())
    }
}

#[cfg(feature = "sql")]
fn resolve_occupied_mutation_job_start(
    retained: &MutationJobRecord,
    submitted_intent: &CanonicalMutationIntent,
) -> Result<MutationJobState, MutationJobError> {
    let retained_intent = CanonicalMutationIntent::decode(retained.canonical_intent())?;
    if retained_intent.same_start_request(submitted_intent) {
        return Ok(retained.state().clone());
    }
    if !retained_intent.same_authority(submitted_intent) {
        return Err(MutationJobError::AuthorityMismatch);
    }
    Err(MutationJobError::IdentityConflict)
}

const fn mutation_job_execution_budget_error(error: ExecutionBudgetExceeded) -> MutationJobError {
    MutationJobError::ExecutionBudgetExceeded {
        resource: error.resource().raw(),
        limit: error.limit(),
        observed: error.observed(),
        scope: error.scope().raw(),
        lane: error.lane().raw(),
        normalized_shape_fingerprint_prefix: error.normalized_shape_fingerprint_prefix(),
    }
}

#[cfg(test)]
mod tests;
