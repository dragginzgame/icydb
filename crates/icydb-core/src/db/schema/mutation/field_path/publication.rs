use super::*;

///
/// SchemaMutationRuntimeInvalidationSink
///
/// Sink for runtime schema invalidation after staged physical index work has
/// validated. This is intentionally narrower than cache/planner internals so
/// runner code can record the invalidation boundary before publication wiring
/// exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
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

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationPlan {
    store: String,
    entry_count: usize,
    publication_identity: SchemaMutationPublicationIdentity,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
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
            store_visibility: validation.store_visibility(),
            runner_report: validation.runner_report().clone(),
        })
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
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
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
            publication_identity: self.publication_identity.clone(),
            invalidated_epochs,
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

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRuntimeInvalidationReport {
    store: String,
    entry_count: usize,
    publication_identity: SchemaMutationPublicationIdentity,
    invalidated_epochs: usize,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime invalidation before publication exists"
)]
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
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn invalidated_epochs(&self) -> usize {
        self.invalidated_epochs
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
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

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
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

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
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

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationPlan {
    store: String,
    entry_count: usize,
    accepted_after: PersistedSchemaSnapshot,
    publication_identity: SchemaMutationPublicationIdentity,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
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
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
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
            accepted_after: self.accepted_after.clone(),
            publication_identity: self.publication_identity.clone(),
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

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexSnapshotPublicationReport {
    store: String,
    entry_count: usize,
    accepted_after: PersistedSchemaSnapshot,
    publication_identity: SchemaMutationPublicationIdentity,
    store_visibility: SchemaMutationStoreVisibility,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages accepted snapshot publication before schema-store writes are wired"
)]
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
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        &self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_identity(
        &self,
    ) -> &SchemaMutationPublicationIdentity {
        &self.publication_identity
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
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

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexPublishedStoreError {
    StoreMismatch,
    PhysicalStateNotValidated,
    SnapshotNotPublished,
    StoreNotBuilding,
    EntryCountMismatch,
}

///
/// SchemaFieldPathIndexPublishedStorePlan
///
/// Final physical publication plan for one validated field-path `IndexStore`.
/// It is constructible only after isolated physical validation and accepted
/// snapshot publication agree on the same accepted store.
///

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStorePlan {
    store: String,
    entry_count: usize,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
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

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    pub(in crate::db::schema) fn publish_index_store(
        &self,
        index_store: &mut IndexStore,
    ) -> Result<SchemaFieldPathIndexPublishedStoreReport, SchemaFieldPathIndexPublishedStoreError>
    {
        if index_store.state() != IndexState::Building {
            return Err(SchemaFieldPathIndexPublishedStoreError::StoreNotBuilding);
        }

        let entry_count = usize::try_from(index_store.len())
            .map_err(|_| SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch)?;
        if entry_count != self.entry_count {
            return Err(SchemaFieldPathIndexPublishedStoreError::EntryCountMismatch);
        }

        let generation_before = index_store.generation();
        index_store.mark_ready();
        let runner_report = self
            .publication_report
            .runner_report()
            .with_physical_store_published();

        Ok(SchemaFieldPathIndexPublishedStoreReport {
            store: self.store.clone(),
            entry_count,
            generation_before,
            generation_after: index_store.generation(),
            index_state: index_store.state(),
            store_visibility: SchemaMutationStoreVisibility::Published,
            publication_report: self.publication_report.clone(),
            runner_report,
        })
    }
}

///
/// SchemaFieldPathIndexPublishedStoreReport
///
/// Positive report after a validated isolated field-path `IndexStore` has been
/// promoted to ready, planner-visible physical state.
///

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexPublishedStoreReport {
    store: String,
    entry_count: usize,
    generation_before: u64,
    generation_after: u64,
    index_state: IndexState,
    store_visibility: SchemaMutationStoreVisibility,
    publication_report: SchemaFieldPathIndexSnapshotPublicationReport,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages physical index-store publication before DDL wiring consumes it"
)]
impl SchemaFieldPathIndexPublishedStoreReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_before(&self) -> u64 {
        self.generation_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn generation_after(&self) -> u64 {
        self.generation_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_state(&self) -> IndexState {
        self.index_state
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_report(
        &self,
    ) -> &SchemaFieldPathIndexSnapshotPublicationReport {
        &self.publication_report
    }

    #[must_use]
    pub(in crate::db::schema) const fn runner_report(&self) -> &SchemaMutationRunnerReport {
        &self.runner_report
    }

    #[must_use]
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
/// been validated. 0.153 keeps these explicit so overlay validation cannot be
/// mistaken for accepted snapshot publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages publication blockers before staged stores can be published"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[allow(
    dead_code,
    reason = "0.153 stages publication readiness before staged stores can be published"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStorePublicationReadiness {
    store: String,
    entry_count: usize,
    blockers: Vec<SchemaFieldPathIndexStagedStorePublicationBlocker>,
    runner_report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages publication readiness before staged stores can be published"
)]
impl SchemaFieldPathIndexStagedStorePublicationReadiness {
    #[must_use]
    pub(in crate::db::schema) fn from_overlay_validation(
        validation: &SchemaFieldPathIndexStagedStoreOverlayValidation,
    ) -> Self {
        Self::from_validated_parts(
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
        Self::from_validated_parts(
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
        Self::from_validated_parts(
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
        Self::from_validated_parts(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_published_store_report(report: &SchemaFieldPathIndexPublishedStoreReport) -> Self {
        Self::from_validated_parts(
            report.store(),
            report.entry_count(),
            report.store_visibility(),
            report.runner_report(),
        )
    }

    #[must_use]
    fn from_validated_parts(
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
