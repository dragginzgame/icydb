use super::*;

///
/// SchemaMutationRuntimeInvalidationSink
///
/// Sink for runtime schema invalidation after staged physical index work has
/// validated. This is intentionally narrower than cache/planner internals so
/// runner code can record the invalidation boundary before publication wiring
/// exists.
///

pub(in crate::db::schema) trait SchemaMutationRuntimeInvalidationSink {
    fn invalidate_runtime_schema(
        &mut self,
        store: &str,
        before: &SchemaMutationRuntimeEpoch,
        after: &SchemaMutationRuntimeEpoch,
    );
}
/// SchemaFieldPathIndexRuntimeInvalidationPlan
///
/// Runtime invalidation plan for one validated staged field-path index store.
/// It binds physical validation to the accepted before/after schema epochs
/// while keeping store visibility staged-only.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationPlan {
    store: String,
    entry_count: usize,
    publication_identity: SchemaMutationPublicationIdentity,
    #[cfg(test)]
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaFieldPathIndexRuntimeInvalidationPlan {
    pub(in crate::db::schema) fn from_isolated_index_store_validation(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
        input: &SchemaMutationRunnerInput<'_>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            store: validation.store().to_string(),
            entry_count: validation.entry_count(),
            publication_identity: SchemaMutationPublicationIdentity::from_input(
                input,
                validation.store_visibility(),
            )?,
            #[cfg(test)]
            store_visibility: validation.store_visibility(),
            runner_report: validation.runner_report().clone(),
        })
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) fn requires_invalidation(&self) -> bool {
        self.publication_identity.changes_epoch()
    }

    #[must_use]
    pub(in crate::db::schema) fn invalidate_runtime_state(
        &self,
        sink: &mut impl SchemaMutationRuntimeInvalidationSink,
    ) -> SchemaFieldPathIndexRuntimeInvalidationReport {
        #[cfg(test)]
        let invalidated_epochs = usize::from(self.requires_invalidation());
        if self.requires_invalidation() {
            sink.invalidate_runtime_schema(
                &self.store,
                self.publication_identity.before_epoch(),
                self.publication_identity.after_epoch(),
            );
        }

        SchemaFieldPathIndexRuntimeInvalidationReport {
            store: self.store.clone(),
            entry_count: self.entry_count,
            #[cfg(test)]
            publication_identity: self.publication_identity.clone(),
            #[cfg(test)]
            invalidated_epochs,
            #[cfg(test)]
            store_visibility: self.store_visibility,
            runner_report: self.runner_report.with_runtime_state_invalidated(),
        }
    }
}
/// SchemaFieldPathIndexRuntimeInvalidationReport
///
/// Positive report after runtime invalidation has accepted one validated staged
/// field-path index store. This advances runner diagnostics through
/// `InvalidateRuntimeState` but remains non-publishable while store visibility
/// is staged-only and snapshot publication has not occurred.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationReport {
    store: String,
    entry_count: usize,
    #[cfg(test)]
    publication_identity: SchemaMutationPublicationIdentity,
    #[cfg(test)]
    invalidated_epochs: usize,
    #[cfg(test)]
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaFieldPathIndexRuntimeInvalidationReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn invalidated_epochs(&self) -> usize {
        self.invalidated_epochs
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_runtime_invalidation_report(self)
    }
}

///
/// SchemaMutationAcceptedSnapshotPublicationSink
///
/// Sink for publishing the accepted-after schema snapshot after staged physical
/// work has validated and runtime state has been invalidated. This keeps the
/// runner publication handoff mockable until real schema-store publication is
/// wired.
///

pub(in crate::db::schema) trait SchemaMutationAcceptedSnapshotPublicationSink {
    fn publish_accepted_schema(
        &mut self,
        store: &str,
        accepted_after: &PersistedSchemaSnapshot,
        before: &SchemaMutationRuntimeEpoch,
        after: &SchemaMutationRuntimeEpoch,
    );
}

///
/// SchemaFieldPathIndexSnapshotPublicationPlanError
///
/// Fail-closed reasons for constructing a staged field-path index snapshot
/// publication plan after runtime invalidation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexSnapshotPublicationPlanError {
    RuntimeStateNotInvalidated,
    AcceptedSnapshotIdentity,
}

///
/// SchemaFieldPathIndexSnapshotPublicationPlan
///
/// Publication handoff for one validated and invalidated staged field-path
/// index store. The plan publishes through a sink and reports the final runner
/// publication phase without directly mutating the schema store.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationPlan {
    store: String,
    entry_count: usize,
    accepted_after: PersistedSchemaSnapshot,
    publication_identity: SchemaMutationPublicationIdentity,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaFieldPathIndexSnapshotPublicationPlan {
    pub(in crate::db::schema) fn from_runtime_invalidation_report(
        report: &SchemaFieldPathIndexRuntimeInvalidationReport,
        input: &SchemaMutationRunnerInput<'_>,
    ) -> Result<Self, SchemaFieldPathIndexSnapshotPublicationPlanError> {
        if !report
            .runner_report()
            .has_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState)
        {
            return Err(
                SchemaFieldPathIndexSnapshotPublicationPlanError::RuntimeStateNotInvalidated,
            );
        }

        let publication_identity = SchemaMutationPublicationIdentity::from_input(
            input,
            SchemaMutationStoreVisibility::Published,
        )
        .map_err(|_| SchemaFieldPathIndexSnapshotPublicationPlanError::AcceptedSnapshotIdentity)?;

        Ok(Self {
            store: report.store().to_string(),
            entry_count: report.entry_count(),
            accepted_after: input.accepted_after().clone(),
            publication_identity,
            runner_report: report.runner_report().clone(),
        })
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) fn publish_snapshot(
        &self,
        sink: &mut impl SchemaMutationAcceptedSnapshotPublicationSink,
    ) -> SchemaFieldPathIndexSnapshotPublicationReport {
        sink.publish_accepted_schema(
            &self.store,
            &self.accepted_after,
            self.publication_identity.before_epoch(),
            self.publication_identity.after_epoch(),
        );

        SchemaFieldPathIndexSnapshotPublicationReport {
            store: self.store.clone(),
            entry_count: self.entry_count,
            #[cfg(test)]
            accepted_after: self.accepted_after.clone(),
            #[cfg(test)]
            publication_identity: self.publication_identity.clone(),
            #[cfg(test)]
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
            runner_report: self.runner_report.with_snapshot_published(),
        }
    }
}

///
/// SchemaFieldPathIndexSnapshotPublicationReport
///
/// Positive report after the accepted-after snapshot publication handoff has
/// been accepted by a sink. Physical store visibility remains staged until the
/// validated `IndexStore` is promoted through `SchemaFieldPathIndexPublishedStorePlan`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationReport {
    store: String,
    entry_count: usize,
    #[cfg(test)]
    accepted_after: PersistedSchemaSnapshot,
    #[cfg(test)]
    publication_identity: SchemaMutationPublicationIdentity,
    #[cfg(test)]
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaFieldPathIndexSnapshotPublicationReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_snapshot_publication_report(self)
    }
}

///
/// SchemaFieldPathIndexPublishedStoreError
///
/// Fail-closed reasons for promoting a validated staged field-path index store
/// to published `IndexStore` visibility.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexPublishedStoreError {
    StoreMismatch,
    PhysicalStateNotValidated,
    SnapshotNotPublished,
    StoreNotBuilding,
    IndexKeyDecode,
    EntryCountMismatch,
}

///
/// SchemaFieldPathIndexPublishedStorePlan
///
/// Final physical publication plan for one validated field-path `IndexStore`.
/// It is constructible only after isolated physical validation and accepted
/// snapshot publication agree on the same accepted store.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStorePlan {
    store: String,
    entry_count: usize,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
}

impl SchemaFieldPathIndexPublishedStorePlan {
    pub(in crate::db::schema) fn from_validated_publication(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
        publication_report: &SchemaFieldPathIndexSnapshotPublicationReport,
    ) -> Result<Self, SchemaFieldPathIndexPublishedStoreError> {
        if validation.store() != publication_report.store() {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreMismatch);
        }
        if !validation
            .runner_report()
            .has_completed_phase(SchemaMutationRunnerPhase::ValidatePhysicalState)
        {
            return Err(SchemaFieldPathIndexPublishedStoreError::PhysicalStateNotValidated);
        }
        if !publication_report
            .runner_report()
            .has_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot)
        {
            return Err(SchemaFieldPathIndexPublishedStoreError::SnapshotNotPublished);
        }
        if validation.index_state() != IndexState::Building {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreNotBuilding);
        }
        if validation.entry_count() != publication_report.entry_count() {
            return Err(SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch);
        }

        Ok(Self {
            store: validation.store().to_string(),
            entry_count: validation.entry_count(),
            publication_report: publication_report.clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::db::schema) fn publish_index_store(
        &self,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexPublishedStoreReport, SchemaFieldPathIndexPublishedStoreError>
    {
        self.publish_index_store_with_scope(index_store, None)
    }

    pub(in crate::db::schema) fn publish_index_store_for_target_index(
        &self,
        target_index_id: &IndexId,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexPublishedStoreReport, SchemaFieldPathIndexPublishedStoreError>
    {
        self.publish_index_store_with_scope(index_store, Some(target_index_id))
    }

    fn publish_index_store_with_scope(
        &self,
        index_store: &mut IndexStore,
        target_index_id: Option<&IndexId>,
    ) -> Result<SchemaFieldPathIndexPublishedStoreReport, SchemaFieldPathIndexPublishedStoreError>
    {
        if index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreNotBuilding);
        }

        let entry_count = match target_index_id {
            Some(target_index_id) => target_index_entry_count(index_store, target_index_id)?,
            None => usize::try_from(index_store.len())
                .map_err(|_| SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch)?,
        };
        if entry_count != self.entry_count {
            return Err(SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch);
        }

        index_store.mark_ready();
        let runner_report = self
            .publication_report
            .runner_report()
            .with_physical_store_published();

        Ok(SchemaFieldPathIndexPublishedStoreReport {
            #[cfg(test)]
            store: self.store.clone(),
            #[cfg(test)]
            entry_count,
            #[cfg(test)]
            index_state: index_store.state(),
            #[cfg(test)]
            store_visibility: SchemaMutationStoreVisibility::Published,
            runner_report,
        })
    }
}

fn target_index_entry_count(
    index_store: &IndexStore,
    target_index_id: &IndexId,
) -> Result<usize, SchemaFieldPathIndexPublishedStoreError> {
    let mut entry_count = 0usize;
    let result: Result<(), SchemaFieldPathIndexPublishedStoreError> =
        index_store.visit_entries(|raw_key, _| {
            let index_key = IndexKey::try_from_raw(raw_key)
                .map_err(|_| SchemaFieldPathIndexPublishedStoreError::IndexKeyDecode)?;
            if index_key.index_id() == target_index_id {
                entry_count = entry_count.saturating_add(1);
            }
            Ok(IndexStoreVisit::Continue)
        });
    result?;

    Ok(entry_count)
}

///
/// SchemaFieldPathIndexPublishedStoreReport
///
/// Positive report after a validated isolated field-path `IndexStore` has been
/// promoted to ready, planner-visible physical state.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStoreReport {
    #[cfg(test)]
    store: String,
    #[cfg(test)]
    entry_count: usize,
    #[cfg(test)]
    index_state: IndexState,
    #[cfg(test)]
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

impl SchemaFieldPathIndexPublishedStoreReport {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn publication_readiness(
        &self,
    ) -> SchemaFieldPathIndexStagedStorePublicationReadiness {
        SchemaFieldPathIndexStagedStorePublicationReadiness::from_published_store_report(self)
    }
}

///
/// SchemaFieldPathIndexStagedStorePublicationBlocker
///
/// Remaining publication barriers after a staged field-path index overlay has
/// been validated. These remain explicit so overlay validation cannot be
/// mistaken for accepted snapshot publication.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedStorePublicationBlocker {
    StoreStillStaged,
    PhysicalStateNotValidated,
    RuntimeStateNotInvalidated,
    SnapshotNotPublished,
    PhysicalStoreNotPublished,
}

///
/// SchemaFieldPathIndexStagedStorePublicationReadiness
///
/// Fail-closed publication readiness for one validated staged field-path index
/// overlay. A readiness report with blockers is diagnostic only; publication
/// remains disallowed until the store is published and all runner phases have
/// completed.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStorePublicationReadiness {
    store: String,
    entry_count: usize,
    blockers: Vec<SchemaFieldPathIndexStagedStorePublicationBlocker>,
    runner_report: SchemaMutationRunnerReport,
}

#[cfg(test)]
impl SchemaFieldPathIndexStagedStorePublicationReadiness {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) fn from_overlay_validation(
        validation: &SchemaFieldPathIndexStagedStoreOverlayValidation,
    ) -> Self {
        Self::from_validation_inputs(
            validation.store(),
            validation.entry_count(),
            validation.store_visibility(),
            validation.runner_report(),
        )
    }

    #[must_use]
    pub(in crate::db::schema) fn from_isolated_index_store_validation(
        validation: &SchemaFieldPathIndexIsolatedIndexStoreValidation,
    ) -> Self {
        Self::from_validation_inputs(
            validation.store(),
            validation.entry_count(),
            validation.store_visibility(),
            validation.runner_report(),
        )
    }

    #[must_use]
    fn from_runtime_invalidation_report(
        report: &SchemaFieldPathIndexRuntimeInvalidationReport,
    ) -> Self {
        Self::from_validation_inputs(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_snapshot_publication_report(
        report: &SchemaFieldPathIndexSnapshotPublicationReport,
    ) -> Self {
        Self::from_validation_inputs(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_published_store_report(report: &SchemaFieldPathIndexPublishedStoreReport) -> Self {
        Self::from_validation_inputs(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_validation_inputs(
        store: &str,
        entry_count: usize,
        store_visibility: SchemaMutationStoreVisibility,
        runner_report: &SchemaMutationRunnerReport,
    ) -> Self {
        let mut blockers = Vec::new();

        if store_visibility != SchemaMutationStoreVisibility::Published {
            blockers.push(SchemaFieldPathIndexStagedStorePublicationBlocker::StoreStillStaged);
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::ValidatePhysicalState) {
            blockers
                .push(SchemaFieldPathIndexStagedStorePublicationBlocker::PhysicalStateNotValidated);
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState) {
            blockers.push(
                SchemaFieldPathIndexStagedStorePublicationBlocker::RuntimeStateNotInvalidated,
            );
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot) {
            blockers.push(SchemaFieldPathIndexStagedStorePublicationBlocker::SnapshotNotPublished);
        }
        if !runner_report.has_completed_phase(SchemaMutationRunnerPhase::PublishPhysicalStore) {
            blockers
                .push(SchemaFieldPathIndexStagedStorePublicationBlocker::PhysicalStoreNotPublished);
        }

        Self {
            store: store.to_string(),
            entry_count,
            blockers,
            runner_report: runner_report.clone(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn blockers(
        &self,
    ) -> &[SchemaFieldPathIndexStagedStorePublicationBlocker] {
        self.blockers.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
    pub(in crate::db::schema) fn allows_publication(&self) -> bool {
        self.blockers.is_empty() && self.runner_report.physical_work_allows_publication()
    }
}
