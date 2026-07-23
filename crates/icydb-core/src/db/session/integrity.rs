//! Module: db::session::integrity
//! Responsibility: session routing into accepted-native integrity inspection.
//! Does not own: inspection semantics, accepted schema construction, or recovery.
//! Boundary: authorized entity path -> runtime hook -> accepted inspection plan.

#[cfg(test)]
use crate::db::integrity::{
    DerivedInspectionLimits, DerivedIntegrityPage, IntegrityProofVector, PhysicalUnitCheckpoint,
    RowInspectionLimits, RowIntegrityPage, execute_index_integrity_page,
    execute_reverse_integrity_page, execute_row_integrity_page,
};
use crate::{
    db::{
        DbSession, QuickIntegrityResult,
        commit::database_incarnation_id,
        integrity::{
            IntegrityDeepError, IntegrityJobId, IntegrityJobOwner, IntegrityJobReceipt,
            IntegrityRetentionPage, IntegritySubmissionKey, abort_deep_integrity_job,
            capture_integrity_proof_vector, continue_deep_integrity_job, execute_quick_integrity,
            run_integrity_retention_page, start_deep_integrity_job, uninspectable_quick_integrity,
        },
        session::accepted_schema::AcceptedInspectionPlanLoadError,
    },
    error::InternalError,
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one bounded Quick inspection for a registered accepted entity.
    ///
    /// This hidden core entry is the final semantic route used by the public
    /// typed and SQL frontends once their authorization and request lowering
    /// land. It is not an application-facing compatibility surface.
    #[doc(hidden)]
    pub fn __icydb_execute_quick_integrity_for_entity(
        &self,
        entity_path: &str,
    ) -> Result<QuickIntegrityResult, InternalError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let incarnation = database_incarnation_id()?;
        match self.accepted_inspection_plan_for_runtime_hook(hooks, store) {
            Ok(plan) => execute_quick_integrity(&self.db, &plan),
            Err(AcceptedInspectionPlanLoadError::Selected { identity, error }) => {
                Ok(uninspectable_quick_integrity(identity, incarnation, &error))
            }
            Err(AcceptedInspectionPlanLoadError::Unselected(error)) => Err(error),
        }
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

    /// Start one internal authorized Deep job with an A/B proof handshake.
    #[allow(
        dead_code,
        reason = "0.212 Patch 7 exposes the internal Deep lifecycle through authorized frontends"
    )]
    pub(in crate::db) fn start_deep_integrity_for_entity(
        &self,
        entity_path: &str,
        owner: IntegrityJobOwner,
        submission_key: IntegritySubmissionKey,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        let hooks = self.db.runtime_hook_for_entity_path(entity_path)?;
        let store = self.db.recovered_store(hooks.store_path)?;
        let first_plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
        let proof_a = capture_integrity_proof_vector(&self.db, &first_plan)?;

        let store = self.db.recovered_store(hooks.store_path)?;
        let second_plan = self
            .accepted_inspection_plan_for_runtime_hook(hooks, store)
            .map_err(AcceptedInspectionPlanLoadError::into_internal)?;
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

    /// Continue or replay one internal authorized Deep job.
    #[allow(
        dead_code,
        reason = "0.212 Patch 7 exposes the internal Deep lifecycle through authorized frontends"
    )]
    pub(in crate::db) fn continue_deep_integrity(
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

    /// Freeze one internal authorized Deep job for abort.
    #[allow(
        dead_code,
        reason = "0.212 Patch 7 exposes the internal Deep lifecycle through authorized frontends"
    )]
    pub(in crate::db) fn abort_deep_integrity(
        job_id: IntegrityJobId,
        owner: &IntegrityJobOwner,
    ) -> Result<IntegrityJobReceipt, IntegrityDeepError> {
        abort_deep_integrity_job::<C>(job_id, owner)
    }

    /// Run one bounded progress-only expiry and retention page.
    #[allow(
        dead_code,
        reason = "0.212 Patch 7 wires bounded retention into the authorized frontend lifecycle"
    )]
    pub(in crate::db) fn run_integrity_retention(
        checkpoint: Option<IntegrityJobId>,
    ) -> Result<IntegrityRetentionPage, IntegrityDeepError> {
        run_integrity_retention_page::<C>(checkpoint)
    }
}
