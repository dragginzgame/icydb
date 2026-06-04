use super::{
    RebuildRequirement, SchemaFieldPathIndexRebuildTarget, SchemaFieldPathIndexStagedValidation,
    SchemaMutationExecutionPlan, SchemaMutationRunnerCapability, SchemaMutationRunnerContract,
    expression::SchemaExpressionIndexStagedValidation,
};
use crate::db::schema::PersistedSchemaSnapshot;

///
/// SchemaMutationRunnerPhase
///
/// Named phase boundary for physical schema mutation runners. 0.153 starts by
/// making these diagnostics explicit before any runner can mutate storage.
///

#[allow(
    dead_code,
    reason = "0.153 stages runner outcome diagnostics before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerPhase {
    Preflight,
    StageStores,
    BuildPhysicalState,
    ValidatePhysicalState,
    InvalidateRuntimeState,
    PublishSnapshot,
    PublishPhysicalStore,
    Diagnostics,
}

impl SchemaMutationRunnerPhase {
    #[must_use]
    pub(in crate::db::schema) const fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::StageStores => "stage_stores",
            Self::BuildPhysicalState => "build_physical_state",
            Self::ValidatePhysicalState => "validate_physical_state",
            Self::InvalidateRuntimeState => "invalidate_runtime_state",
            Self::PublishSnapshot => "publish_snapshot",
            Self::PublishPhysicalStore => "publish_physical_store",
            Self::Diagnostics => "diagnostics",
        }
    }
}

///
/// SchemaMutationDeveloperKind
///
/// Stable developer-facing mutation kind for startup schema mutation reports.
/// This stays narrower than the internal mutation vocabulary so unsupported
/// shapes cannot appear as partially supported diagnostics.
///

#[allow(
    dead_code,
    reason = "0.154 exposes startup schema mutation diagnostics before SQL DDL consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationDeveloperKind {
    AddFieldPathIndex,
}

impl SchemaMutationDeveloperKind {
    #[must_use]
    pub(in crate::db::schema) const fn as_str(self) -> &'static str {
        match self {
            Self::AddFieldPathIndex => "add_field_path_index",
        }
    }
}

///
/// SchemaMutationValidationStatus
///
/// Stable validation outcome bucket for developer-visible schema mutation
/// diagnostics. Detailed internal errors stay typed on the runner failure.
///

#[allow(
    dead_code,
    reason = "0.154 exposes startup schema mutation diagnostics before SQL DDL consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationValidationStatus {
    NotStarted,
    Passed,
    Failed,
}

impl SchemaMutationValidationStatus {
    #[must_use]
    pub(in crate::db::schema) const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

///
/// SchemaMutationPublishStatus
///
/// Stable publication outcome bucket for developer-visible schema mutation
/// diagnostics. This is intentionally separate from validation status because
/// startup can validate physical work and still fail closed before publication.
///

#[allow(
    dead_code,
    reason = "0.154 exposes startup schema mutation diagnostics before SQL DDL consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationPublishStatus {
    NotStarted,
    Published,
    Failed,
}

impl SchemaMutationPublishStatus {
    #[must_use]
    pub(in crate::db::schema) const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

///
/// SchemaMutationDeveloperReport
///
/// Compact startup report for the one supported developer schema mutation
/// path. It exposes the mutation shape, target, row/key counts, and validation
/// and publication status without making SQL DDL the authority.
///

#[allow(
    dead_code,
    reason = "0.154 exposes startup schema mutation diagnostics before SQL DDL consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationDeveloperReport {
    phase: SchemaMutationRunnerPhase,
    mutation_kind: SchemaMutationDeveloperKind,
    entity_path: &'static str,
    target_index: String,
    target_store: String,
    target_fields: Vec<String>,
    rows_scanned: usize,
    index_keys_written: usize,
    validation_status: SchemaMutationValidationStatus,
    publish_status: SchemaMutationPublishStatus,
}

#[allow(
    dead_code,
    reason = "0.154 exposes startup schema mutation diagnostics before SQL DDL consumes them"
)]
impl SchemaMutationDeveloperReport {
    #[must_use]
    pub(in crate::db::schema) fn field_path_index_addition(
        phase: SchemaMutationRunnerPhase,
        entity_path: &'static str,
        target: &SchemaFieldPathIndexRebuildTarget,
        rows_scanned: usize,
        index_keys_written: usize,
        validation_status: SchemaMutationValidationStatus,
        publish_status: SchemaMutationPublishStatus,
    ) -> Self {
        Self {
            phase,
            mutation_kind: SchemaMutationDeveloperKind::AddFieldPathIndex,
            entity_path,
            target_index: target.name().to_string(),
            target_store: target.store().to_string(),
            target_fields: target_field_paths(target),
            rows_scanned,
            index_keys_written,
            validation_status,
            publish_status,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn phase(&self) -> SchemaMutationRunnerPhase {
        self.phase
    }

    #[must_use]
    pub(in crate::db::schema) const fn mutation_kind(&self) -> SchemaMutationDeveloperKind {
        self.mutation_kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn entity_path(&self) -> &'static str {
        self.entity_path
    }

    #[must_use]
    pub(in crate::db::schema) const fn target_index(&self) -> &str {
        self.target_index.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn target_store(&self) -> &str {
        self.target_store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn target_fields(&self) -> &[String] {
        self.target_fields.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn rows_scanned(&self) -> usize {
        self.rows_scanned
    }

    #[must_use]
    pub(in crate::db::schema) const fn index_keys_written(&self) -> usize {
        self.index_keys_written
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation_status(&self) -> SchemaMutationValidationStatus {
        self.validation_status
    }

    #[must_use]
    pub(in crate::db::schema) const fn publish_status(&self) -> SchemaMutationPublishStatus {
        self.publish_status
    }

    #[must_use]
    pub(in crate::db::schema) fn summary(&self) -> String {
        format!(
            "phase={} mutation_kind={} entity='{}' target_index='{}' target_store='{}' target_fields='{}' rows_scanned={} index_keys_written={} validation_status={} publish_status={}",
            self.phase.as_str(),
            self.mutation_kind.as_str(),
            self.entity_path,
            self.target_index,
            self.target_store,
            self.target_fields.join(","),
            self.rows_scanned,
            self.index_keys_written,
            self.validation_status.as_str(),
            self.publish_status.as_str(),
        )
    }
}

fn target_field_paths(target: &SchemaFieldPathIndexRebuildTarget) -> Vec<String> {
    target
        .key_paths()
        .iter()
        .map(|key| key.path().join("."))
        .collect()
}

///
/// SchemaMutationStoreVisibility
///
/// Visibility state for schema mutation physical stores. Rebuilt or
/// cleanup-affected state must remain staged-only until publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages staged-store visibility contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationStoreVisibility {
    StagedOnly,
    Published,
}

///
/// SchemaMutationRunnerRejectionKind
///
/// Classified runner rejection category. These categories keep preflight and
/// future physical failures distinguishable without matching error strings.
///

#[allow(
    dead_code,
    reason = "0.153 stages runner rejection diagnostics before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerRejectionKind {
    MissingCapabilities,
    UnsupportedRequirement,
    UnsupportedExecutionStep,
    ValidationFailed,
    RollbackFailed,
    PublicationFailed,
}

///
/// SchemaMutationRunnerRejection
///
/// Structured rejection from a runner phase. Preflight uses it immediately;
/// later slices can report validation, rollback, and publication failures
/// through the same contract.
///

#[allow(
    dead_code,
    reason = "0.153 stages runner rejection diagnostics before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerRejection {
    phase: SchemaMutationRunnerPhase,
    kind: SchemaMutationRunnerRejectionKind,
    requirement: Option<RebuildRequirement>,
    missing_capabilities: Vec<SchemaMutationRunnerCapability>,
}

#[allow(
    dead_code,
    reason = "0.153 stages runner rejection diagnostics before physical runners consume them"
)]
impl SchemaMutationRunnerRejection {
    #[must_use]
    pub(super) const fn unsupported_requirement(requirement: RebuildRequirement) -> Self {
        Self {
            phase: SchemaMutationRunnerPhase::Preflight,
            kind: SchemaMutationRunnerRejectionKind::UnsupportedRequirement,
            requirement: Some(requirement),
            missing_capabilities: Vec::new(),
        }
    }

    #[must_use]
    pub(super) const fn missing_runner_capabilities(
        requirement: Option<RebuildRequirement>,
        missing_capabilities: Vec<SchemaMutationRunnerCapability>,
    ) -> Self {
        Self {
            phase: SchemaMutationRunnerPhase::Preflight,
            kind: SchemaMutationRunnerRejectionKind::MissingCapabilities,
            requirement,
            missing_capabilities,
        }
    }

    #[must_use]
    pub(super) const fn validation_failed(requirement: RebuildRequirement) -> Self {
        Self {
            phase: SchemaMutationRunnerPhase::ValidatePhysicalState,
            kind: SchemaMutationRunnerRejectionKind::ValidationFailed,
            requirement: Some(requirement),
            missing_capabilities: Vec::new(),
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn phase(&self) -> SchemaMutationRunnerPhase {
        self.phase
    }

    #[must_use]
    pub(in crate::db::schema) const fn kind(&self) -> SchemaMutationRunnerRejectionKind {
        self.kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn requirement(&self) -> Option<RebuildRequirement> {
        self.requirement
    }

    #[must_use]
    pub(in crate::db::schema) const fn missing_capabilities(
        &self,
    ) -> &[SchemaMutationRunnerCapability] {
        self.missing_capabilities.as_slice()
    }
}

///
/// SchemaMutationRunnerReport
///
/// Positive runner diagnostic report. The first 0.153 slice records only
/// preflight facts; later physical phases should extend this report instead of
/// inventing a second diagnostics lane.
///

#[allow(
    dead_code,
    reason = "0.153 stages runner diagnostics before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerReport {
    step_count: usize,
    required_capabilities: Vec<SchemaMutationRunnerCapability>,
    completed_phases: Vec<SchemaMutationRunnerPhase>,
    store_visibility: Option<SchemaMutationStoreVisibility>,
    rows_scanned: usize,
    rows_skipped: usize,
    index_keys_written: usize,
}

#[allow(
    dead_code,
    reason = "0.153 stages runner diagnostics before physical runners consume them"
)]
impl SchemaMutationRunnerReport {
    #[must_use]
    pub(super) fn preflight_ready(
        step_count: usize,
        required_capabilities: Vec<SchemaMutationRunnerCapability>,
        store_visibility: Option<SchemaMutationStoreVisibility>,
    ) -> Self {
        Self {
            step_count,
            required_capabilities,
            completed_phases: vec![SchemaMutationRunnerPhase::Preflight],
            store_visibility,
            rows_scanned: 0,
            rows_skipped: 0,
            index_keys_written: 0,
        }
    }

    #[must_use]
    pub(super) fn field_path_index_staged(
        step_count: usize,
        required_capabilities: Vec<SchemaMutationRunnerCapability>,
        validation: SchemaFieldPathIndexStagedValidation,
    ) -> Self {
        Self {
            step_count,
            required_capabilities,
            completed_phases: vec![
                SchemaMutationRunnerPhase::Preflight,
                SchemaMutationRunnerPhase::StageStores,
                SchemaMutationRunnerPhase::BuildPhysicalState,
                SchemaMutationRunnerPhase::ValidatePhysicalState,
            ],
            store_visibility: Some(validation.store_visibility()),
            rows_scanned: validation.source_rows(),
            rows_skipped: validation.skipped_rows(),
            index_keys_written: validation.entry_count(),
        }
    }

    #[must_use]
    pub(super) fn expression_index_staged(
        step_count: usize,
        required_capabilities: Vec<SchemaMutationRunnerCapability>,
        validation: SchemaExpressionIndexStagedValidation,
    ) -> Self {
        Self {
            step_count,
            required_capabilities,
            completed_phases: vec![
                SchemaMutationRunnerPhase::Preflight,
                SchemaMutationRunnerPhase::StageStores,
                SchemaMutationRunnerPhase::BuildPhysicalState,
                SchemaMutationRunnerPhase::ValidatePhysicalState,
            ],
            store_visibility: Some(validation.store_visibility()),
            rows_scanned: validation.source_rows(),
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
    pub(in crate::db::schema) const fn step_count(&self) -> usize {
        self.step_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn required_capabilities(
        &self,
    ) -> &[SchemaMutationRunnerCapability] {
        self.required_capabilities.as_slice()
    }

    #[must_use]
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
/// SchemaMutationRunnerOutcome
///
/// Runner-facing outcome after evaluating one execution plan against the
/// advertised runner contract. `ReadyForPhysicalWork` is not publication; it is
/// the point where a later runner may start staged physical work.
///

#[allow(
    dead_code,
    reason = "0.153 stages runner outcomes before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerOutcome {
    NoPhysicalWork(SchemaMutationRunnerReport),
    ReadyForPhysicalWork(SchemaMutationRunnerReport),
    Rejected(SchemaMutationRunnerRejection),
}

///
/// SchemaMutationRunnerInputError
///
/// Fail-closed input construction error before a physical runner can see a
/// schema mutation. These are catalog identity errors, not runner failures.
///

#[allow(
    dead_code,
    reason = "0.153 stages checked runner inputs before physical runners consume them"
)]
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
/// before snapshot, after snapshot, and schema-owned execution plan together so
/// runner code never reconstructs mutation semantics from generated metadata.
///

#[allow(
    dead_code,
    reason = "0.153 stages checked runner inputs before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerInput<'a> {
    accepted_before: &'a PersistedSchemaSnapshot,
    accepted_after: &'a PersistedSchemaSnapshot,
    execution_plan: SchemaMutationExecutionPlan,
}

#[allow(
    dead_code,
    reason = "0.153 stages checked runner inputs before physical runners consume them"
)]
impl<'a> SchemaMutationRunnerInput<'a> {
    pub(in crate::db::schema) fn new(
        accepted_before: &'a PersistedSchemaSnapshot,
        accepted_after: &'a PersistedSchemaSnapshot,
        execution_plan: SchemaMutationExecutionPlan,
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
            execution_plan,
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
    pub(in crate::db::schema) const fn execution_plan(&self) -> &SchemaMutationExecutionPlan {
        &self.execution_plan
    }

    #[must_use]
    pub(in crate::db::schema) fn outcome(
        &self,
        runner: &SchemaMutationRunnerContract,
    ) -> SchemaMutationRunnerOutcome {
        runner.outcome(&self.execution_plan)
    }
}

///
/// SchemaMutationNoopRunner
///
/// No-op schema mutation runner adapter. It consumes checked runner inputs and
/// reports the preflight outcome for an empty capability contract, so metadata-
/// only inputs can pass through while physical-work inputs fail closed until a
/// real staged runner exists.
///

#[allow(
    dead_code,
    reason = "0.153 stages the no-op runner adapter before physical runners consume inputs"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationNoopRunner {
    contract: SchemaMutationRunnerContract,
}

#[allow(
    dead_code,
    reason = "0.153 stages the no-op runner adapter before physical runners consume inputs"
)]
impl SchemaMutationNoopRunner {
    #[must_use]
    pub(in crate::db::schema) fn new() -> Self {
        Self {
            contract: SchemaMutationRunnerContract::new(&[]),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn run(
        &self,
        input: &SchemaMutationRunnerInput<'_>,
    ) -> SchemaMutationRunnerOutcome {
        input.outcome(&self.contract)
    }
}

impl Default for SchemaMutationNoopRunner {
    fn default() -> Self {
        Self::new()
    }
}
