use super::{MutationPlan, SchemaFieldPathIndexStagedValidation};
use crate::db::schema::PersistedSchemaSnapshot;

///
/// SchemaMutationRunnerPhase
///
/// Ordered phase boundary used to prove that physical mutation work completed
/// before publication becomes visible.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerPhase {
    Preflight,
    StageStores,
    BuildPhysicalState,
    ValidatePhysicalState,
    InvalidateRuntimeState,
    PublishSnapshot,
    PublishPhysicalStore,
}

///
/// SchemaFieldPathIndexMutationMetrics
///
/// Metrics emitted by the completed field-path index mutation path. Physical
/// publication readiness remains on `SchemaFieldPathIndexMutationProgress`; this type
/// carries only the facts consumed after successful publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexMutationMetrics {
    entity_path: &'static str,
    rows_scanned: usize,
    index_keys_written: usize,
}

impl SchemaFieldPathIndexMutationMetrics {
    #[must_use]
    pub(in crate::db::schema) const fn new(
        entity_path: &'static str,
        rows_scanned: usize,
        index_keys_written: usize,
    ) -> Self {
        Self {
            entity_path,
            rows_scanned,
            index_keys_written,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db::schema) const fn index_keys_written(&self) -> usize {
        self.index_keys_written
    }
}

///
/// SchemaMutationStoreVisibility
///
/// Visibility state for schema mutation physical stores. Rebuilt or
/// cleanup-affected state must remain staged-only until publication.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationStoreVisibility {
    StagedOnly,
    Published,
}

///
/// SchemaFieldPathIndexMutationProgress
///
/// Positive runner diagnostic report sourced from the phases the current
/// physical mutation path actually completed.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexMutationProgress {
    completed_phases: Vec<SchemaMutationRunnerPhase>,
    store_visibility: Option<SchemaMutationStoreVisibility>,
    rows_scanned: usize,
    #[cfg(test)]
    rows_skipped: usize,
    index_keys_written: usize,
}

impl SchemaFieldPathIndexMutationProgress {
    #[must_use]
    pub(super) fn field_path_index_staged(
        validation: SchemaFieldPathIndexStagedValidation,
    ) -> Self {
        Self {
            completed_phases: vec![
                SchemaMutationRunnerPhase::Preflight,
                SchemaMutationRunnerPhase::StageStores,
                SchemaMutationRunnerPhase::BuildPhysicalState,
                SchemaMutationRunnerPhase::ValidatePhysicalState,
            ],
            store_visibility: Some(validation.store_visibility()),
            rows_scanned: validation.source_rows(),
            #[cfg(test)]
            rows_skipped: validation.skipped_rows(),
            index_keys_written: validation.entry_count(),
        }
    }

    #[must_use]
    fn with_completed_phase(&self, phase: SchemaMutationRunnerPhase) -> Self {
        let mut next = self.clone();
        if !next.completed_phases.contains(&phase) {
            next.completed_phases.push(phase);
        }
        next
    }

    #[must_use]
    pub(in crate::db::schema) fn with_runtime_state_invalidated(&self) -> Self {
        self.with_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState)
    }

    #[must_use]
    pub(in crate::db::schema) fn with_snapshot_published(&self) -> Self {
        self.with_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot)
    }

    #[must_use]
    pub(in crate::db::schema) fn with_physical_store_published(&self) -> Self {
        let mut next = self.with_completed_phase(SchemaMutationRunnerPhase::PublishPhysicalStore);
        next.store_visibility = Some(SchemaMutationStoreVisibility::Published);
        next
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn completed_phases(&self) -> &[SchemaMutationRunnerPhase] {
        self.completed_phases.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) fn has_completed_phase(
        &self,
        phase: SchemaMutationRunnerPhase,
    ) -> bool {
        self.completed_phases.contains(&phase)
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn store_visibility(
        &self,
    ) -> Option<SchemaMutationStoreVisibility> {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::schema) const fn rows_skipped(&self) -> usize {
        self.rows_skipped
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_keys_written(&self) -> usize {
        self.index_keys_written
    }

    #[must_use]
    pub(in crate::db::schema) fn physical_work_allows_publication(&self) -> bool {
        self.store_visibility == Some(SchemaMutationStoreVisibility::Published)
            && self.has_completed_phase(SchemaMutationRunnerPhase::ValidatePhysicalState)
            && self.has_completed_phase(SchemaMutationRunnerPhase::InvalidateRuntimeState)
            && self.has_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot)
            && self.has_completed_phase(SchemaMutationRunnerPhase::PublishPhysicalStore)
    }
}

///
/// SchemaMutationRunnerInputError
///
/// Fail-closed input construction error before a physical runner can see a
/// schema mutation. These are catalog identity errors, not runner failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerInputError {
    EntityPath,
    EntityName,
    PrimaryKeyField,
}

///
/// SchemaMutationRunnerInput
///
/// Accepted-schema-native input for physical mutation runners. It binds the
/// before snapshot, after snapshot, and schema-owned mutation plan together so
/// runner code never reconstructs mutation semantics from generated metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerInput<'a> {
    accepted_before: &'a PersistedSchemaSnapshot,
    accepted_after: &'a PersistedSchemaSnapshot,
    mutation_plan: MutationPlan,
}

impl<'a> SchemaMutationRunnerInput<'a> {
    pub(in crate::db::schema) fn new(
        accepted_before: &'a PersistedSchemaSnapshot,
        accepted_after: &'a PersistedSchemaSnapshot,
        mutation_plan: MutationPlan,
    ) -> Result<Self, SchemaMutationRunnerInputError> {
        if accepted_before.entity_path() != accepted_after.entity_path() {
            return Err(SchemaMutationRunnerInputError::EntityPath);
        }

        if accepted_before.entity_name() != accepted_after.entity_name() {
            return Err(SchemaMutationRunnerInputError::EntityName);
        }

        if accepted_before.primary_key_field_ids() != accepted_after.primary_key_field_ids() {
            return Err(SchemaMutationRunnerInputError::PrimaryKeyField);
        }

        Ok(Self {
            accepted_before,
            accepted_after,
            mutation_plan,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_before(&self) -> &PersistedSchemaSnapshot {
        self.accepted_before
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_after(&self) -> &PersistedSchemaSnapshot {
        self.accepted_after
    }

    #[must_use]
    pub(in crate::db::schema) const fn mutation_plan(&self) -> &MutationPlan {
        &self.mutation_plan
    }
}
