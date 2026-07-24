//! Module: db::session::integrity
//! Responsibility: session routing into accepted-native integrity inspection.
//! Does not own: inspection semantics, accepted schema construction, or recovery.
//! Boundary: authorized entity path -> runtime hook -> accepted inspection plan.

#[cfg(test)]
use crate::{
    db::integrity::{
        DerivedInspectionLimits, DerivedIntegrityPage, IntegrityProofVector,
        PhysicalUnitCheckpoint, RowInspectionLimits, RowIntegrityPage,
        execute_index_integrity_page, execute_reverse_integrity_page, execute_row_integrity_page,
    },
    error::InternalError,
};
use crate::{
    db::{
        DbSession, QuickIntegrityResult,
        commit::database_incarnation_id,
        integrity::{
            IntegrityAuthorityDiagnostic, IntegrityCheckRequest, IntegrityCheckResult,
            IntegrityDeepError, IntegrityEntityIdentity, IntegrityJobError, IntegrityJobId,
            IntegrityJobOwner, IntegrityJobReceipt, IntegritySubmissionKey,
            abort_deep_integrity_job, capture_integrity_proof_vector, continue_deep_integrity_job,
            execute_quick_integrity, run_next_integrity_retention_page, start_deep_integrity_job,
            uninspectable_quick_integrity,
        },
        runtime_hooks::EntityRuntimeHooks,
        schema::AcceptedInspectionPlan,
        session::accepted_schema::AcceptedInspectionPlanLoadError,
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one trusted typed integrity request.
    ///
    /// The caller must enforce controller or equivalent integrity-specific
    /// authorization before accepting caller-controlled requests. The owner
    /// must be a stable identity for that already-authorized caller or
    /// capability; possession of a job ID is never authorization.
    ///
    /// # Errors
    ///
    /// Returns a typed protocol error for invalid requests, authorization
    /// ownership mismatches, and stale acknowledgements, or an internal error
    /// when accepted authority or physical inspection cannot be read safely.
    pub fn execute_admin_integrity(
        &self,
        request: IntegrityCheckRequest,
        owner: IntegrityJobOwner,
    ) -> Result<IntegrityCheckResult, IntegrityDeepError> {
        owner.validate()?;
        let result = match request {
            IntegrityCheckRequest::Quick { entity } => self
                .execute_quick_integrity_for_identity(&entity)
                .map(IntegrityCheckResult::Quick),
            IntegrityCheckRequest::DeepStart {
                entity,
                submission_key,
            } => self
                .start_deep_integrity_for_identity(&entity, owner, submission_key)
                .map(IntegrityCheckResult::Deep),
            IntegrityCheckRequest::DeepContinue {
                job_id,
                acknowledged_sequence,
            } => {
                job_id.validate()?;
                self.continue_deep_integrity(job_id, &owner, acknowledged_sequence)
                    .map(IntegrityCheckResult::Deep)
            }
            IntegrityCheckRequest::DeepAbort { job_id } => {
                job_id.validate()?;
                Self::abort_deep_integrity(job_id, &owner).map(IntegrityCheckResult::Deep)
            }
        };
        run_next_integrity_retention_page::<C>()?;
        result
    }

    fn execute_quick_integrity_for_identity(
        &self,
        entity: &IntegrityEntityIdentity,
    ) -> Result<QuickIntegrityResult, IntegrityDeepError> {
        let (hooks, store) = self.integrity_target(entity)?;
        let incarnation = database_incarnation_id()?;
        match self.accepted_inspection_plan_for_runtime_hook(hooks, store) {
            Ok(plan) => {
                Self::validate_integrity_plan_identity(entity, &plan)?;
                execute_quick_integrity(&self.db, &plan).map_err(IntegrityDeepError::from)
            }
            Err(AcceptedInspectionPlanLoadError::Selected { identity, error }) => {
                let accepted = IntegrityEntityIdentity::from_accepted_identity(identity);
                if entity != &accepted {
                    return Err(IntegrityJobError::EntityIdentityMismatch.into());
                }
                Ok(uninspectable_quick_integrity(identity, incarnation, &error))
            }
            Err(AcceptedInspectionPlanLoadError::Unselected(error)) => {
                Err(IntegrityDeepError::from(error))
            }
        }
    }

    fn integrity_target(
        &self,
        entity: &IntegrityEntityIdentity,
    ) -> Result<(&EntityRuntimeHooks<C>, crate::db::registry::StoreHandle), IntegrityDeepError>
    {
        entity.validate()?;
        let hooks = self.db.runtime_hook_for_entity_path(entity.entity_path())?;
        if hooks.entity_tag.value() != entity.entity_tag()
            || hooks.store_path != entity.store_path()
        {
            return Err(IntegrityJobError::EntityIdentityMismatch.into());
        }
        let store = self.db.recovered_store(hooks.store_path)?;

        Ok((hooks, store))
    }

    fn validate_integrity_plan_identity(
        entity: &IntegrityEntityIdentity,
        plan: &AcceptedInspectionPlan,
    ) -> Result<(), IntegrityDeepError> {
        if entity != &IntegrityEntityIdentity::from_accepted_identity(plan.identity()) {
            return Err(IntegrityJobError::EntityIdentityMismatch.into());
        }
        Ok(())
    }

    /// Execute one bounded internal row-inspection page.
    ///
    /// The checkpoint and limits remain inside the database boundary. Patch 5
    /// binds this exact core to authorized durable Deep jobs; no application
    /// caller can author or resume physical progress directly.
    #[cfg(test)]
    pub(in crate::db) fn execute_integrity_row_page_for_entity(
        &self,
        entity_path: &str,
        checkpoint: PhysicalUnitCheckpoint,
        limits: RowInspectionLimits,
    ) -> Result<RowIntegrityPage, InternalError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        execute_row_integrity_page(&self.db, &plan, checkpoint, limits)
    }

    /// Execute one bounded internal active forward-index page.
    ///
    /// The index ordinal is accepted-plan order, not public or physical
    /// identity. Patch 5 owns phase sequencing through authorized jobs.
    #[cfg(test)]
    pub(in crate::db) fn execute_integrity_index_page_for_entity(
        &self,
        entity_path: &str,
        index_ordinal: usize,
        checkpoint: PhysicalUnitCheckpoint,
        limits: DerivedInspectionLimits,
    ) -> Result<DerivedIntegrityPage, InternalError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        execute_index_integrity_page(&self.db, &plan, index_ordinal, checkpoint, limits)
    }

    /// Execute one bounded internal active source-owned reverse-relation page.
    ///
    /// The relation ordinal is accepted-plan order. Pending and retired
    /// generations remain outside this traversal by construction.
    #[cfg(test)]
    pub(in crate::db) fn execute_integrity_reverse_page_for_entity(
        &self,
        entity_path: &str,
        relation_ordinal: usize,
        checkpoint: PhysicalUnitCheckpoint,
        limits: DerivedInspectionLimits,
    ) -> Result<DerivedIntegrityPage, InternalError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        execute_reverse_integrity_page(&self.db, &plan, relation_ordinal, checkpoint, limits)
    }

    /// Capture the complete pre/post advancement proof for one entity.
    #[cfg(test)]
    pub(in crate::db) fn capture_integrity_proof_for_entity(
        &self,
        entity_path: &str,
    ) -> Result<IntegrityProofVector, InternalError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        capture_integrity_proof_vector(&self.db, &plan)
    }

    /// Start one authorized Deep job with an A/B proof handshake.
    fn start_deep_integrity_for_identity(
        &self,
        entity: &IntegrityEntityIdentity,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        self.start_deep_integrity_for_identity_with_plan_loader(
            entity,
            owner,
            submission_key,
            |hooks, store| self.accepted_inspection_plan_for_runtime_hook(hooks, store),
        )
    }

    fn start_deep_integrity_for_identity_with_plan_loader(
        &self,
        entity: &IntegrityEntityIdentity,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
        mut load_plan: impl FnMut(
            &EntityRuntimeHooks<C>,
            crate::db::registry::StoreHandle,
        )
            -> Result<AcceptedInspectionPlan, AcceptedInspectionPlanLoadError>,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        submission_key.validate()?;
        let (hooks, store) = self.integrity_target(entity)?;
        let first_plan = load_plan(hooks, store)
            .map_err(|error| Self::deep_start_plan_load_error(entity, error))?;
        Self::validate_integrity_plan_identity(entity, &first_plan)?;
        let proof_a = capture_integrity_proof_vector(&self.db, &first_plan)?;

        let store = self.db.recovered_store(hooks.store_path)?;
        let second_plan = load_plan(hooks, store)
            .map_err(|error| Self::deep_start_plan_load_error(entity, error))?;
        Self::validate_integrity_plan_identity(entity, &second_plan)?;
        let proof_b = capture_integrity_proof_vector(&self.db, &second_plan)?;
        start_deep_integrity_job(
            &self.db,
            &second_plan,
            owner,
            submission_key,
            proof_a,
            proof_b,
        )
    }

    fn deep_start_plan_load_error(
        entity: &IntegrityEntityIdentity,
        error: AcceptedInspectionPlanLoadError,
    ) -> IntegrityDeepError {
        match error {
            AcceptedInspectionPlanLoadError::Selected { identity, error } => {
                if entity != &IntegrityEntityIdentity::from_accepted_identity(identity) {
                    return IntegrityJobError::EntityIdentityMismatch.into();
                }
                IntegrityDeepError::Uninspectable(IntegrityAuthorityDiagnostic::from_internal(
                    &error,
                ))
            }
            AcceptedInspectionPlanLoadError::Unselected(error) => {
                IntegrityDeepError::Uninspectable(IntegrityAuthorityDiagnostic::from_internal(
                    &error,
                ))
            }
        }
    }

    /// Continue or replay one authorized Deep job.
    fn continue_deep_integrity(
        &self,
        job_id: IntegrityJobId,
        owner: &IntegrityJobOwner,
        acknowledged_sequence: u64,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        continue_deep_integrity_job(
            &self.db,
            job_id,
            owner,
            acknowledged_sequence,
            |entity_path| {
                let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
                let store = self.db.recovered_store(hooks.store_path)?;
                self.accepted_inspection_plan_for_runtime_hook(hooks, store)
                    .map_err(AcceptedInspectionPlanLoadError::into_internal)
            },
        )
    }

    /// Freeze one authorized Deep job for abort.
    fn abort_deep_integrity(
        job_id: IntegrityJobId,
        owner: &IntegrityJobOwner,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        abort_deep_integrity_job::<C>(job_id, owner)
    }

    #[cfg(test)]
    pub(in crate::db) fn start_deep_integrity_for_entity(
        &self,
        entity_path: &str,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        let entity = IntegrityEntityIdentity::from_accepted_identity(plan.identity());

        self.start_deep_integrity_for_identity(&entity, owner, submission_key)
    }

    #[cfg(test)]
    pub(in crate::db) fn continue_deep_integrity_for_tests(
        &self,
        job_id: IntegrityJobId,
        owner: &IntegrityJobOwner,
        acknowledged_sequence: u64,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        self.continue_deep_integrity(job_id, owner, acknowledged_sequence)
    }

    #[cfg(test)]
    pub(in crate::db) fn start_deep_integrity_with_plan_load_failure_for_tests(
        &self,
        entity_path: &str,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        let entity = IntegrityEntityIdentity::from_accepted_identity(plan.identity());

        self.start_deep_integrity_for_identity_with_plan_loader(
            &entity,
            owner,
            submission_key,
            |_, _| {
                Err(AcceptedInspectionPlanLoadError::Unselected(
                    InternalError::store_corruption(),
                ))
            },
        )
    }

    #[cfg(test)]
    pub(in crate::db) fn start_deep_integrity_with_proofs_for_tests(
        &self,
        entity_path: &str,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
        proof_a: IntegrityProofVector,
        proof_b: IntegrityProofVector,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;

        start_deep_integrity_job(&self.db, &plan, owner, submission_key, proof_a, proof_b)
    }

    #[cfg(test)]
    pub(in crate::db) fn continue_deep_integrity_with_plan_load_failure_for_tests(
        &self,
        job_id: IntegrityJobId,
        owner: &IntegrityJobOwner,
        acknowledged_sequence: u64,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        continue_deep_integrity_job(&self.db, job_id, owner, acknowledged_sequence, |_| {
            Err(InternalError::store_corruption())
        })
    }
}
