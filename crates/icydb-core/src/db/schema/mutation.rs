//! Module: db::schema::mutation
//! Responsibility: catalog-native schema mutation contracts.
//! Does not own: SQL DDL parsing, physical rebuild execution, or schema-store writes.
//! Boundary: describes accepted snapshot changes before reconciliation persists them.

use crate::db::{
    codec::{
        finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_tag_u8,
        write_hash_u32,
    },
    data::{CanonicalSlotReader, StorageKey},
    index::{IndexEntry, IndexKey, RawIndexEntry, RawIndexKey},
    schema::{
        FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaVersion,
        encode_persisted_schema_snapshot,
    },
};
use crate::error::InternalError;
use crate::types::EntityTag;
use sha2::Digest;

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
const SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-plan:v1";

#[allow(
    dead_code,
    reason = "0.153 stages runtime epoch identity before physical runners publish snapshots"
)]
const SCHEMA_MUTATION_RUNTIME_EPOCH_PROFILE_TAG: &[u8] = b"icydb:schema-mutation-runtime-epoch:v1";

///
/// SchemaMutation
///
/// SchemaMutation is the schema-owned description of one accepted catalog
/// change. It is intentionally independent of SQL syntax so parser frontends
/// must lower into this contract instead of becoming the migration authority.
///

#[allow(
    dead_code,
    reason = "0.152 defines the first mutation vocabulary before every operation is executable"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutation {
    AddNullableField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    AddDefaultedField {
        field_id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
    },
    AddNonUniqueFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    AlterNullability {
        field_id: FieldId,
    },
}

///
/// SchemaMutationRequest
///
/// Internal request vocabulary that lowers catalog-level mutation intent into
/// a deterministic `MutationPlan`. SQL DDL and generated proposal comparison
/// must route through this type instead of constructing plans ad hoc.
///

#[allow(
    dead_code,
    reason = "0.152 stages the internal mutation request API before every request has a live caller"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRequest<'a> {
    ExactMatch,
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    AddNonUniqueFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    AddExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropNonRequiredSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    AlterNullability {
        field_id: FieldId,
    },
    Incompatible,
}

///
/// AcceptedSchemaMutationError
///
/// Fail-closed reason produced while lowering accepted schema metadata into a
/// mutation request. These errors mean the mutation framework cannot describe
/// a safe catalog operation yet; callers must not compensate with generated
/// metadata.
///

#[allow(
    dead_code,
    reason = "0.152 stages fail-closed mutation lowering before DDL diagnostics expose it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum AcceptedSchemaMutationError {
    UniqueIndexRequiresDedicatedValidation,
    UnsupportedIndexKeyShape,
    EmptyIndexKey,
    ExpressionIndexRequiresExpressionKey,
}

///
/// SchemaFieldPathIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a field-path index. It carries the
/// persisted index store identity and key-slot contract that a later physical
/// rebuild runner must consume before the index can become runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldPathIndexRebuildTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
    key_paths: Vec<SchemaFieldPathIndexRebuildKey>,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaFieldPathIndexRebuildTarget {
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn key_paths(&self) -> &[SchemaFieldPathIndexRebuildKey] {
        self.key_paths.as_slice()
    }
}

///
/// SchemaFieldPathIndexRebuildKey
///
/// One accepted field-path key component required to rebuild a secondary index
/// from accepted row-layout slots.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldPathIndexRebuildKey {
    field_id: FieldId,
    slot: SchemaFieldSlot,
    path: Vec<String>,
    kind: PersistedFieldKind,
    nullable: bool,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaFieldPathIndexRebuildKey {
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn field_name(&self) -> &str {
        self.path.first().map_or("", String::as_str)
    }

    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }
}

///
/// SchemaExpressionIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a deterministic expression index.
/// It preserves accepted key order across field-path and expression components
/// so a later physical rebuild runner does not need generated `IndexModel`
/// metadata to derive key shape.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexRebuildTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
    key_items: Vec<SchemaExpressionIndexRebuildKey>,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaExpressionIndexRebuildTarget {
    #[must_use]
    pub(in crate::db::schema) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db::schema) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
    pub(in crate::db::schema) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn key_items(&self) -> &[SchemaExpressionIndexRebuildKey] {
        self.key_items.as_slice()
    }
}

///
/// SchemaExpressionIndexRebuildKey
///
/// Accepted key component required to rebuild deterministic expression indexes.
/// Mixed indexes retain their exact accepted item order.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaExpressionIndexRebuildKey {
    FieldPath(SchemaFieldPathIndexRebuildKey),
    Expression(Box<SchemaExpressionIndexRebuildExpression>),
}

///
/// SchemaExpressionIndexRebuildExpression
///
/// One accepted deterministic expression key component.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaExpressionIndexRebuildExpression {
    op: PersistedIndexExpressionOp,
    source: SchemaFieldPathIndexRebuildKey,
    input_kind: PersistedFieldKind,
    output_kind: PersistedFieldKind,
    canonical_text: String,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild target contracts before a physical runner consumes them"
)]
impl SchemaExpressionIndexRebuildExpression {
    #[must_use]
    pub(in crate::db::schema) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    #[must_use]
    pub(in crate::db::schema) const fn source(&self) -> &SchemaFieldPathIndexRebuildKey {
        &self.source
    }

    #[must_use]
    pub(in crate::db::schema) const fn input_kind(&self) -> &PersistedFieldKind {
        &self.input_kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn output_kind(&self) -> &PersistedFieldKind {
        &self.output_kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn canonical_text(&self) -> &str {
        self.canonical_text.as_str()
    }
}

///
/// SchemaSecondaryIndexDropCleanupTarget
///
/// Accepted schema-owned cleanup target for dropping a secondary index. It
/// carries the persisted store identity that must be cleaned before a later
/// mutation can publish a snapshot without the index.
///

#[allow(
    dead_code,
    reason = "0.152 stages cleanup target contracts before a physical runner consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaSecondaryIndexDropCleanupTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
}

#[allow(
    dead_code,
    reason = "0.152 stages cleanup target contracts before a physical runner consumes them"
)]
impl SchemaSecondaryIndexDropCleanupTarget {
    #[must_use]
    pub(in crate::db::schema) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db::schema) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
    pub(in crate::db::schema) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }
}

///
/// MutationCompatibility
///
/// Stable high-level compatibility bucket for one mutation plan. This is kept
/// small so unsupported schema changes fail closed instead of leaking through
/// as ad hoc snapshot rewrites.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild and unsupported buckets before every bucket has a live caller"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationCompatibility {
    MetadataOnlySafe,
    RequiresRebuild,
    UnsupportedPreOne,
    Incompatible,
}

///
/// RebuildRequirement
///
/// Physical work required before a mutation can be considered runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.152 exposes future rebuild buckets before orchestration consumes them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum RebuildRequirement {
    NoRebuildRequired,
    IndexRebuildRequired,
    FullDataRewriteRequired,
    Unsupported,
}

///
/// SchemaRebuildAction
///
/// One physical rebuild action implied by a catalog mutation plan. These
/// actions are planning facts only; 0.152 still blocks publication until an
/// executor owns the physical work and validation boundary.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaRebuildAction {
    BuildFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    BuildExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    RewriteAllRows,
    Unsupported {
        reason: &'static str,
    },
}

///
/// SchemaRebuildPlan
///
/// Schema-owned physical work classification derived from a mutation plan.
/// Reconciliation asks publication status before exposing a new accepted
/// snapshot; rebuild plans are the audit/debug shape that will later feed the
/// physical rebuild runner.
///

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaRebuildPlan {
    requirement: RebuildRequirement,
    actions: Vec<SchemaRebuildAction>,
}

#[allow(
    dead_code,
    reason = "0.152 stages rebuild orchestration contracts before execution consumes them"
)]
impl SchemaRebuildPlan {
    const fn no_rebuild() -> Self {
        Self {
            requirement: RebuildRequirement::NoRebuildRequired,
            actions: Vec::new(),
        }
    }

    const fn new(requirement: RebuildRequirement, actions: Vec<SchemaRebuildAction>) -> Self {
        Self {
            requirement,
            actions,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn requirement(&self) -> RebuildRequirement {
        self.requirement
    }

    #[must_use]
    pub(in crate::db::schema) const fn actions(&self) -> &[SchemaRebuildAction] {
        self.actions.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn requires_physical_work(&self) -> bool {
        !matches!(self.requirement, RebuildRequirement::NoRebuildRequired)
    }

    #[must_use]
    const fn publication_blocker(&self) -> Option<MutationPublicationBlocker> {
        if self.requires_physical_work() {
            return Some(MutationPublicationBlocker::RebuildRequired(
                self.requirement,
            ));
        }

        None
    }
}

///
/// SchemaMutationExecutionReadiness
///
/// Schema-owned execution readiness for one mutation plan. This names whether
/// reconciliation can publish immediately, whether a future physical runner
/// must execute index work first, or whether the mutation remains unsupported.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionReadiness {
    PublishableNow,
    RequiresPhysicalRunner(RebuildRequirement),
    Unsupported(RebuildRequirement),
}

///
/// SchemaMutationExecutionStep
///
/// Ordered physical execution step implied by one mutation plan. These are
/// contracts for a later runner, not live rebuild behavior.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionStep {
    BuildFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    BuildExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    ValidatePhysicalWork,
    InvalidateRuntimeState,
    RewriteAllRows,
    Unsupported {
        reason: &'static str,
    },
}

///
/// SchemaMutationRunnerCapability
///
/// Coarse physical capability required by one execution plan. Capabilities are
/// derived from accepted execution steps and give future runner wiring a small
/// fail-closed surface before any physical mutation is attempted.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner capability contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerCapability {
    BuildFieldPathIndex,
    BuildExpressionIndex,
    DropSecondaryIndex,
    ValidatePhysicalWork,
    InvalidateRuntimeState,
    RewriteAllRows,
}

///
/// SchemaMutationExecutionAdmission
///
/// Fail-closed admission result for one execution plan against a future
/// runner's advertised capabilities.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner admission contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionAdmission {
    PublishableNow,
    RunnerReady {
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingRunnerCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

///
/// SchemaMutationRunnerPreflight
///
/// Runner-facing preflight result for one execution plan. This is the last
/// schema-owned check before a future physical runner is allowed to start
/// rebuild or cleanup work.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerPreflight {
    NoPhysicalWork,
    Ready {
        step_count: usize,
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

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
    Diagnostics,
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
    const fn unsupported_requirement(requirement: RebuildRequirement) -> Self {
        Self {
            phase: SchemaMutationRunnerPhase::Preflight,
            kind: SchemaMutationRunnerRejectionKind::UnsupportedRequirement,
            requirement: Some(requirement),
            missing_capabilities: Vec::new(),
        }
    }

    #[must_use]
    const fn missing_runner_capabilities(
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
    const fn validation_failed(requirement: RebuildRequirement) -> Self {
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
    fn preflight_ready(
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
    fn field_path_index_staged(
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

        if accepted_before.primary_key_field_id() != accepted_after.primary_key_field_id() {
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

///
/// SchemaMutationRuntimeEpoch
///
/// Runtime schema identity derived from one accepted persisted snapshot. Future
/// runners use this as the publication/invalidation token; staged physical work
/// must not advance visible runtime identity.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime epoch identity before physical runners publish snapshots"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRuntimeEpoch {
    entity_path: String,
    schema_version: SchemaVersion,
    snapshot_fingerprint: [u8; 16],
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime epoch identity before physical runners publish snapshots"
)]
impl SchemaMutationRuntimeEpoch {
    pub(in crate::db::schema) fn from_snapshot(
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            entity_path: snapshot.entity_path().to_string(),
            schema_version: snapshot.version(),
            snapshot_fingerprint: runtime_epoch_fingerprint(snapshot)?,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
    pub(in crate::db::schema) const fn snapshot_fingerprint(&self) -> [u8; 16] {
        self.snapshot_fingerprint
    }
}

///
/// SchemaMutationPublicationIdentity
///
/// Publication identity for one checked runner input. `StagedOnly` keeps the
/// previous epoch visible; only `Published` exposes the accepted-after epoch to
/// runtime caches and planners.
///

#[allow(
    dead_code,
    reason = "0.153 stages runtime publication identity before physical runners publish snapshots"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationPublicationIdentity {
    before: SchemaMutationRuntimeEpoch,
    after: SchemaMutationRuntimeEpoch,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime publication identity before physical runners publish snapshots"
)]
impl SchemaMutationPublicationIdentity {
    pub(in crate::db::schema) fn from_input(
        input: &SchemaMutationRunnerInput<'_>,
        store_visibility: SchemaMutationStoreVisibility,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            before: SchemaMutationRuntimeEpoch::from_snapshot(input.accepted_before())?,
            after: SchemaMutationRuntimeEpoch::from_snapshot(input.accepted_after())?,
            store_visibility,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn before_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        &self.before
    }

    #[must_use]
    pub(in crate::db::schema) const fn after_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        &self.after
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    #[must_use]
    pub(in crate::db::schema) const fn visible_epoch(&self) -> &SchemaMutationRuntimeEpoch {
        match self.store_visibility {
            SchemaMutationStoreVisibility::StagedOnly => &self.before,
            SchemaMutationStoreVisibility::Published => &self.after,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn published_epoch(
        &self,
    ) -> Option<&SchemaMutationRuntimeEpoch> {
        match self.store_visibility {
            SchemaMutationStoreVisibility::StagedOnly => None,
            SchemaMutationStoreVisibility::Published => Some(&self.after),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn changes_epoch(&self) -> bool {
        self.before != self.after
    }
}

///
/// SchemaFieldPathIndexRebuildRow
///
/// One authoritative row exposed to the field-path rebuild staging primitive.
/// The row is already decoded behind a canonical slot-reader contract; staging
/// must only derive accepted index keys from these row slots.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild row inputs before physical runners own row iteration"
)]
#[derive(Clone, Copy)]
pub(in crate::db::schema) struct SchemaFieldPathIndexRebuildRow<'a> {
    storage_key: StorageKey,
    slots: &'a dyn CanonicalSlotReader,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild row inputs before physical runners own row iteration"
)]
impl<'a> SchemaFieldPathIndexRebuildRow<'a> {
    #[must_use]
    pub(in crate::db::schema) const fn new(
        storage_key: StorageKey,
        slots: &'a dyn CanonicalSlotReader,
    ) -> Self {
        Self { storage_key, slots }
    }

    #[must_use]
    pub(in crate::db::schema) const fn storage_key(self) -> StorageKey {
        self.storage_key
    }

    #[must_use]
    pub(in crate::db::schema) const fn slots(self) -> &'a dyn CanonicalSlotReader {
        self.slots
    }
}

///
/// SchemaFieldPathIndexStagedEntry
///
/// One raw index-store entry produced during staged field-path rebuild work.
/// It remains in memory until later runner phases validate and publish it.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory rebuild entries before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedEntry {
    key: RawIndexKey,
    entry: RawIndexEntry,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory rebuild entries before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedEntry {
    #[must_use]
    pub(in crate::db::schema) const fn key(&self) -> &RawIndexKey {
        &self.key
    }

    #[must_use]
    pub(in crate::db::schema) const fn entry(&self) -> &RawIndexEntry {
        &self.entry
    }
}

///
/// SchemaFieldPathIndexStagedRebuild
///
/// In-memory staged field-path index state. This is not a published store and
/// must not be made planner-visible until validation and publication complete.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory field-path rebuild output before physical runners publish stores"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedRebuild {
    target: SchemaFieldPathIndexRebuildTarget,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory field-path rebuild output before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedRebuild {
    pub(in crate::db::schema) fn from_rows<'a>(
        entity_path: &str,
        entity_tag: EntityTag,
        target: SchemaFieldPathIndexRebuildTarget,
        rows: impl IntoIterator<Item = SchemaFieldPathIndexRebuildRow<'a>>,
    ) -> Result<Self, InternalError> {
        let mut entries = Vec::new();
        let mut source_rows = 0usize;
        let mut skipped_rows = 0usize;

        for row in rows {
            source_rows = source_rows.saturating_add(1);
            let Some(key) = IndexKey::new_from_slots_with_field_path_rebuild_target(
                entity_tag,
                row.storage_key(),
                &target,
                row.slots(),
            )?
            else {
                skipped_rows = skipped_rows.saturating_add(1);
                continue;
            };
            let entry = IndexEntry::new(row.storage_key());
            let raw_entry = RawIndexEntry::try_from(&entry)
                .map_err(|err| err.into_commit_internal_error(entity_path, target.name()))?;

            entries.push(SchemaFieldPathIndexStagedEntry {
                key: key.to_raw(),
                entry: raw_entry,
            });
        }

        entries.sort_by(|left, right| left.key.cmp(&right.key));

        Ok(Self {
            target,
            entries,
            source_rows,
            skipped_rows,
            store_visibility: SchemaMutationStoreVisibility::StagedOnly,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn target(&self) -> &SchemaFieldPathIndexRebuildTarget {
        &self.target
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn source_rows(&self) -> usize {
        self.source_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn skipped_rows(&self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }

    pub(in crate::db::schema) fn validate(
        &self,
    ) -> Result<SchemaFieldPathIndexStagedValidation, SchemaFieldPathIndexStagedValidationError>
    {
        if self.store_visibility != SchemaMutationStoreVisibility::StagedOnly {
            return Err(SchemaFieldPathIndexStagedValidationError::PublishedVisibility);
        }

        let expected_entries = self
            .source_rows
            .checked_sub(self.skipped_rows)
            .ok_or(SchemaFieldPathIndexStagedValidationError::SkippedRowsExceedSourceRows)?;
        if expected_entries != self.entries.len() {
            return Err(SchemaFieldPathIndexStagedValidationError::EntryCountMismatch);
        }

        if !self
            .entries
            .windows(2)
            .all(|pair| pair[0].key < pair[1].key)
        {
            return Err(SchemaFieldPathIndexStagedValidationError::UnsortedOrDuplicateEntries);
        }

        Ok(SchemaFieldPathIndexStagedValidation {
            entry_count: self.entries.len(),
            source_rows: self.source_rows,
            skipped_rows: self.skipped_rows,
            store_visibility: self.store_visibility,
        })
    }

    pub(in crate::db::schema) fn validated_runner_report(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> Result<SchemaMutationRunnerReport, SchemaMutationRunnerRejection> {
        let step_count = match execution_plan.execution_gate() {
            SchemaMutationExecutionGate::AwaitingPhysicalWork {
                requirement: RebuildRequirement::IndexRebuildRequired,
                step_count,
            } => step_count,
            SchemaMutationExecutionGate::AwaitingPhysicalWork { requirement, .. }
            | SchemaMutationExecutionGate::Rejected { requirement } => {
                return Err(SchemaMutationRunnerRejection::unsupported_requirement(
                    requirement,
                ));
            }
            SchemaMutationExecutionGate::ReadyToPublish => {
                return Err(SchemaMutationRunnerRejection::unsupported_requirement(
                    RebuildRequirement::NoRebuildRequired,
                ));
            }
        };

        let validation = self.validate().map_err(|_| {
            SchemaMutationRunnerRejection::validation_failed(
                RebuildRequirement::IndexRebuildRequired,
            )
        })?;

        Ok(SchemaMutationRunnerReport::field_path_index_staged(
            step_count,
            execution_plan.runner_capabilities(),
            validation,
        ))
    }
}

///
/// SchemaFieldPathIndexStagedValidationError
///
/// Fail-closed validation result for staged field-path rebuild output. Later
/// runner phases must validate staged output before any store publication.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaFieldPathIndexStagedValidationError {
    PublishedVisibility,
    SkippedRowsExceedSourceRows,
    EntryCountMismatch,
    UnsortedOrDuplicateEntries,
}

///
/// SchemaFieldPathIndexStagedValidation
///
/// Positive validation report for one in-memory field-path rebuild. The report
/// intentionally stays small until physical runner diagnostics consume it.
///

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedValidation {
    entry_count: usize,
    source_rows: usize,
    skipped_rows: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages field-path rebuild validation before physical runners publish stores"
)]
impl SchemaFieldPathIndexStagedValidation {
    #[must_use]
    pub(in crate::db::schema) const fn entry_count(self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(in crate::db::schema) const fn source_rows(self) -> usize {
        self.source_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn skipped_rows(self) -> usize {
        self.skipped_rows
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }
}

///
/// SchemaFieldPathIndexStagedStore
///
/// In-memory staged store image for one field-path index rebuild. This is the
/// first writer-facing shape after staged rebuild validation: it binds raw
/// index entries to the accepted store identity without mutating `IndexStore`
/// or making rebuilt state runtime-visible.
///

#[allow(
    dead_code,
    reason = "0.153 stages in-memory index-store writes before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedStore {
    store: String,
    entries: Vec<SchemaFieldPathIndexStagedEntry>,
    validation: SchemaFieldPathIndexStagedValidation,
    report: SchemaMutationRunnerReport,
}

#[allow(
    dead_code,
    reason = "0.153 stages in-memory index-store writes before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedStore {
    pub(in crate::db::schema) fn from_rebuild(
        rebuild: &SchemaFieldPathIndexStagedRebuild,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> Result<Self, SchemaMutationRunnerRejection> {
        let validation = rebuild.validate().map_err(|_| {
            SchemaMutationRunnerRejection::validation_failed(
                RebuildRequirement::IndexRebuildRequired,
            )
        })?;
        let report = rebuild.validated_runner_report(execution_plan)?;

        Ok(Self {
            store: rebuild.target().store().to_string(),
            entries: rebuild.entries().to_vec(),
            validation,
            report,
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(&self) -> &[SchemaFieldPathIndexStagedEntry] {
        self.entries.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn validation(&self) -> SchemaFieldPathIndexStagedValidation {
        self.validation
    }

    #[must_use]
    pub(in crate::db::schema) const fn report(&self) -> &SchemaMutationRunnerReport {
        &self.report
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.validation.store_visibility()
    }

    #[must_use]
    pub(in crate::db::schema) fn physical_work_allows_publication(&self) -> bool {
        self.report.physical_work_allows_publication()
    }

    #[must_use]
    pub(in crate::db::schema) fn discard(self) -> SchemaFieldPathIndexStagedDiscardReport {
        let store_visibility = self.store_visibility();
        let discarded_entries = self.entries.len();

        SchemaFieldPathIndexStagedDiscardReport {
            store: self.store,
            discarded_entries,
            store_visibility,
        }
    }
}

///
/// SchemaFieldPathIndexStagedDiscardReport
///
/// Typed cleanup report for discarding one in-memory staged field-path rebuild
/// buffer. Physical rollback will later use a store-backed version of this
/// boundary; this shape keeps staged cleanup explicit before stores are mutated.
///

#[allow(
    dead_code,
    reason = "0.153 stages rebuild rollback diagnostics before physical stores are mutated"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaFieldPathIndexStagedDiscardReport {
    store: String,
    discarded_entries: usize,
    store_visibility: SchemaMutationStoreVisibility,
}

#[allow(
    dead_code,
    reason = "0.153 stages rebuild rollback diagnostics before physical stores are mutated"
)]
impl SchemaFieldPathIndexStagedDiscardReport {
    #[must_use]
    pub(in crate::db::schema) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn discarded_entries(&self) -> usize {
        self.discarded_entries
    }

    #[must_use]
    pub(in crate::db::schema) const fn store_visibility(&self) -> SchemaMutationStoreVisibility {
        self.store_visibility
    }
}

///
/// MutationPublicationPreflight
///
/// Publication-boundary decision after consulting runner preflight. It keeps
/// metadata-only publication separate from physical-work readiness so a future
/// runner cannot accidentally make rebuild-required plans publishable before
/// physical execution and validation exist.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight publication checks before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationPreflight {
    PublishableNow,
    PhysicalWorkReady {
        step_count: usize,
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingRunnerCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
    Blocked(MutationPublicationBlocker),
}

///
/// SchemaMutationRunnerContract
///
/// Capability advertisement for a future physical mutation runner. It owns no
/// execution behavior yet; it only lets schema mutation plans fail closed before
/// publication policy can be widened.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerContract {
    capabilities: Vec<SchemaMutationRunnerCapability>,
}

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
impl SchemaMutationRunnerContract {
    #[must_use]
    pub(in crate::db::schema) fn new(capabilities: &[SchemaMutationRunnerCapability]) -> Self {
        let mut deduped = Vec::new();

        for capability in capabilities {
            push_runner_capability_once(&mut deduped, *capability);
        }

        Self {
            capabilities: deduped,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn capabilities(&self) -> &[SchemaMutationRunnerCapability] {
        self.capabilities.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) fn preflight(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> SchemaMutationRunnerPreflight {
        match execution_plan.admit_runner_capabilities(self.capabilities()) {
            SchemaMutationExecutionAdmission::PublishableNow => {
                SchemaMutationRunnerPreflight::NoPhysicalWork
            }
            SchemaMutationExecutionAdmission::RunnerReady { required } => {
                SchemaMutationRunnerPreflight::Ready {
                    step_count: execution_plan.steps().len(),
                    required,
                }
            }
            SchemaMutationExecutionAdmission::MissingRunnerCapabilities { missing } => {
                SchemaMutationRunnerPreflight::MissingCapabilities { missing }
            }
            SchemaMutationExecutionAdmission::Rejected { requirement } => {
                SchemaMutationRunnerPreflight::Rejected { requirement }
            }
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn outcome(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> SchemaMutationRunnerOutcome {
        match self.preflight(execution_plan) {
            SchemaMutationRunnerPreflight::NoPhysicalWork => {
                SchemaMutationRunnerOutcome::NoPhysicalWork(
                    SchemaMutationRunnerReport::preflight_ready(0, Vec::new(), None),
                )
            }
            SchemaMutationRunnerPreflight::Ready {
                step_count,
                required,
            } => SchemaMutationRunnerOutcome::ReadyForPhysicalWork(
                SchemaMutationRunnerReport::preflight_ready(
                    step_count,
                    required,
                    Some(SchemaMutationStoreVisibility::StagedOnly),
                ),
            ),
            SchemaMutationRunnerPreflight::MissingCapabilities { missing } => {
                SchemaMutationRunnerOutcome::Rejected(
                    SchemaMutationRunnerRejection::missing_runner_capabilities(
                        execution_plan.physical_requirement(),
                        missing,
                    ),
                )
            }
            SchemaMutationRunnerPreflight::Rejected { requirement } => {
                SchemaMutationRunnerOutcome::Rejected(
                    SchemaMutationRunnerRejection::unsupported_requirement(requirement),
                )
            }
        }
    }
}

///
/// SchemaMutationExecutionGate
///
/// Schema-owned publish gate derived from an execution plan. It is the narrow
/// answer future callers need before deciding whether to publish, invoke a
/// physical runner, or reject the mutation.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionGate {
    ReadyToPublish,
    AwaitingPhysicalWork {
        requirement: RebuildRequirement,
        step_count: usize,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

///
/// SchemaMutationExecutionPlan
///
/// Execution-facing form of a mutation plan. It keeps the future physical
/// runner contract separate from rebuild classification and publication
/// policy, so adding execution cannot silently widen startup reconciliation.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationExecutionPlan {
    readiness: SchemaMutationExecutionReadiness,
    steps: Vec<SchemaMutationExecutionStep>,
}

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
impl SchemaMutationExecutionPlan {
    const fn publishable_now() -> Self {
        Self {
            readiness: SchemaMutationExecutionReadiness::PublishableNow,
            steps: Vec::new(),
        }
    }

    fn from_rebuild_plan(rebuild_plan: SchemaRebuildPlan) -> Self {
        if !rebuild_plan.requires_physical_work() {
            return Self::publishable_now();
        }

        let readiness = match rebuild_plan.requirement() {
            RebuildRequirement::NoRebuildRequired => {
                SchemaMutationExecutionReadiness::PublishableNow
            }
            RebuildRequirement::IndexRebuildRequired => {
                SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
                    RebuildRequirement::IndexRebuildRequired,
                )
            }
            RebuildRequirement::FullDataRewriteRequired | RebuildRequirement::Unsupported => {
                SchemaMutationExecutionReadiness::Unsupported(rebuild_plan.requirement())
            }
        };

        let mut steps = rebuild_plan
            .actions()
            .iter()
            .map(|action| match action {
                SchemaRebuildAction::BuildFieldPathIndex { target } => {
                    SchemaMutationExecutionStep::BuildFieldPathIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::BuildExpressionIndex { target } => {
                    SchemaMutationExecutionStep::BuildExpressionIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::DropSecondaryIndex { target } => {
                    SchemaMutationExecutionStep::DropSecondaryIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::RewriteAllRows => SchemaMutationExecutionStep::RewriteAllRows,
                SchemaRebuildAction::Unsupported { reason } => {
                    SchemaMutationExecutionStep::Unsupported { reason }
                }
            })
            .collect::<Vec<_>>();

        if matches!(
            readiness,
            SchemaMutationExecutionReadiness::RequiresPhysicalRunner(_)
        ) {
            steps.push(SchemaMutationExecutionStep::ValidatePhysicalWork);
            steps.push(SchemaMutationExecutionStep::InvalidateRuntimeState);
        }

        Self { readiness, steps }
    }

    #[must_use]
    pub(in crate::db::schema) const fn readiness(&self) -> SchemaMutationExecutionReadiness {
        self.readiness
    }

    #[must_use]
    pub(in crate::db::schema) const fn steps(&self) -> &[SchemaMutationExecutionStep] {
        self.steps.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn execution_gate(&self) -> SchemaMutationExecutionGate {
        match self.readiness {
            SchemaMutationExecutionReadiness::PublishableNow => {
                SchemaMutationExecutionGate::ReadyToPublish
            }
            SchemaMutationExecutionReadiness::RequiresPhysicalRunner(requirement) => {
                SchemaMutationExecutionGate::AwaitingPhysicalWork {
                    requirement,
                    step_count: self.steps.len(),
                }
            }
            SchemaMutationExecutionReadiness::Unsupported(requirement) => {
                SchemaMutationExecutionGate::Rejected { requirement }
            }
        }
    }

    #[must_use]
    const fn physical_requirement(&self) -> Option<RebuildRequirement> {
        match self.execution_gate() {
            SchemaMutationExecutionGate::ReadyToPublish => None,
            SchemaMutationExecutionGate::AwaitingPhysicalWork { requirement, .. }
            | SchemaMutationExecutionGate::Rejected { requirement } => Some(requirement),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn runner_capabilities(&self) -> Vec<SchemaMutationRunnerCapability> {
        let mut capabilities = Vec::new();

        for step in &self.steps {
            let capability = match step {
                SchemaMutationExecutionStep::BuildFieldPathIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::BuildFieldPathIndex)
                }
                SchemaMutationExecutionStep::BuildExpressionIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::BuildExpressionIndex)
                }
                SchemaMutationExecutionStep::DropSecondaryIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::DropSecondaryIndex)
                }
                SchemaMutationExecutionStep::ValidatePhysicalWork => {
                    Some(SchemaMutationRunnerCapability::ValidatePhysicalWork)
                }
                SchemaMutationExecutionStep::InvalidateRuntimeState => {
                    Some(SchemaMutationRunnerCapability::InvalidateRuntimeState)
                }
                SchemaMutationExecutionStep::RewriteAllRows => {
                    Some(SchemaMutationRunnerCapability::RewriteAllRows)
                }
                SchemaMutationExecutionStep::Unsupported { .. } => None,
            };

            if let Some(capability) = capability {
                push_runner_capability_once(&mut capabilities, capability);
            }
        }

        capabilities
    }

    #[must_use]
    pub(in crate::db::schema) fn admit_runner_capabilities(
        &self,
        available: &[SchemaMutationRunnerCapability],
    ) -> SchemaMutationExecutionAdmission {
        match self.execution_gate() {
            SchemaMutationExecutionGate::ReadyToPublish => {
                SchemaMutationExecutionAdmission::PublishableNow
            }
            SchemaMutationExecutionGate::Rejected { requirement } => {
                SchemaMutationExecutionAdmission::Rejected { requirement }
            }
            SchemaMutationExecutionGate::AwaitingPhysicalWork { .. } => {
                let required = self.runner_capabilities();
                let missing = required
                    .iter()
                    .copied()
                    .filter(|capability| !available.contains(capability))
                    .collect::<Vec<_>>();

                if missing.is_empty() {
                    SchemaMutationExecutionAdmission::RunnerReady { required }
                } else {
                    SchemaMutationExecutionAdmission::MissingRunnerCapabilities { missing }
                }
            }
        }
    }
}

///
/// MutationPublicationBlocker
///
/// Fail-closed reason preventing one mutation plan from becoming accepted
/// runtime schema. This is intentionally separate from the compatibility and
/// rebuild enums so reconciliation asks one schema-owned publication gate
/// instead of reimplementing publishability rules locally.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationBlocker {
    NotMetadataSafe(MutationCompatibility),
    RebuildRequired(RebuildRequirement),
}

///
/// MutationPublicationStatus
///
/// Runtime publication decision for one mutation plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationStatus {
    Publishable,
    Blocked(MutationPublicationBlocker),
}

///
/// SchemaMutationDelta
///
/// Snapshot-delta classification between two accepted catalog snapshots. This
/// keeps structural mutation detection inside the mutation layer while the
/// transition layer remains responsible for validation and diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationDelta<'a> {
    AppendOnlyFields(&'a [PersistedFieldSnapshot]),
    ExactMatch,
    Incompatible,
}

/// Classify the structural mutation shape between an accepted snapshot and a
/// proposed replacement. This does not decide whether the mutation is safe; it
/// only names the catalog delta shape for policy code.
pub(in crate::db::schema) fn classify_schema_mutation_delta<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationDelta<'a> {
    if actual == expected {
        return SchemaMutationDelta::ExactMatch;
    }

    append_only_additive_fields(actual, expected).map_or(
        SchemaMutationDelta::Incompatible,
        SchemaMutationDelta::AppendOnlyFields,
    )
}

/// Build one mutation request from the structural delta between two accepted
/// snapshots. Policy validation remains in transition; this function only
/// classifies the catalog operation to keep lowering centralized.
pub(in crate::db::schema) fn schema_mutation_request_for_snapshots<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> SchemaMutationRequest<'a> {
    SchemaMutationRequest::from(classify_schema_mutation_delta(actual, expected))
}

///
/// MutationPlan
///
/// Deterministic schema-owned plan for moving one accepted snapshot to the
/// next. Startup reconciliation can currently execute only no-rebuild plans;
/// future DDL/rebuild work should extend this type before widening behavior.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct MutationPlan {
    mutations: Vec<SchemaMutation>,
    compatibility: MutationCompatibility,
    rebuild: RebuildRequirement,
}

impl MutationPlan {
    /// Build the no-op plan for equal accepted snapshots.
    pub(in crate::db::schema) const fn exact_match() -> Self {
        Self {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::NoRebuildRequired,
        }
    }

    /// Build the currently executable append-only field plan. The caller owns
    /// validating nullable/default absence semantics before publishing it.
    pub(in crate::db::schema) fn append_only_fields(fields: &[PersistedFieldSnapshot]) -> Self {
        let mutations = fields
            .iter()
            .map(|field| {
                if field.default().is_none() {
                    SchemaMutation::AddNullableField {
                        field_id: field.id(),
                        name: field.name().to_string(),
                        slot: field.slot(),
                    }
                } else {
                    SchemaMutation::AddDefaultedField {
                        field_id: field.id(),
                        name: field.name().to_string(),
                        slot: field.slot(),
                    }
                }
            })
            .collect();

        Self {
            mutations,
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::NoRebuildRequired,
        }
    }

    /// Stage a non-unique field-path index addition from accepted index
    /// metadata. This is a planning artifact only until rebuild orchestration
    /// can construct the physical index safely.
    fn non_unique_field_path_index_addition(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddNonUniqueFieldPathIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage an accepted deterministic expression index addition. This shares
    /// the same rebuild bucket as field-path indexes but remains a separate
    /// mutation so canonical expression metadata can be audited independently.
    fn expression_index_addition(target: SchemaExpressionIndexRebuildTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::AddExpressionIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a supported index drop. Runtime execution is deferred until store
    /// cleanup and planner invalidation are wired through the mutation engine.
    fn secondary_index_drop(target: SchemaSecondaryIndexDropCleanupTarget) -> Self {
        Self {
            mutations: vec![SchemaMutation::DropNonRequiredSecondaryIndex { target }],
            compatibility: MutationCompatibility::RequiresRebuild,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        }
    }

    /// Stage a nullability alteration. Pre-1.0 this remains fail-closed because
    /// existing data must be proven or rewritten before accepting it.
    fn nullability_alteration(field_id: FieldId) -> Self {
        Self {
            mutations: vec![SchemaMutation::AlterNullability { field_id }],
            compatibility: MutationCompatibility::UnsupportedPreOne,
            rebuild: RebuildRequirement::Unsupported,
        }
    }

    /// Build the generic incompatible plan used by guard tests and future
    /// diagnostics for rejected snapshot changes.
    const fn incompatible() -> Self {
        Self {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::Incompatible,
            rebuild: RebuildRequirement::FullDataRewriteRequired,
        }
    }

    /// Borrow the ordered mutation list.
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
    )]
    #[must_use]
    pub(in crate::db::schema) const fn mutations(&self) -> &[SchemaMutation] {
        self.mutations.as_slice()
    }

    /// Return the stable compatibility bucket.
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
    )]
    #[must_use]
    pub(in crate::db::schema) const fn compatibility(&self) -> MutationCompatibility {
        self.compatibility
    }

    /// Return the physical rebuild requirement.
    #[allow(
        dead_code,
        reason = "mutation diagnostics and DDL lowering will consume this in the next 0.152 slice"
    )]
    #[must_use]
    pub(in crate::db::schema) const fn rebuild_requirement(&self) -> RebuildRequirement {
        self.rebuild
    }

    /// Decide whether this mutation plan can be published as accepted runtime
    /// schema without additional physical rebuild work.
    #[must_use]
    pub(in crate::db::schema) fn publication_status(&self) -> MutationPublicationStatus {
        if !matches!(self.compatibility, MutationCompatibility::MetadataOnlySafe) {
            return MutationPublicationStatus::Blocked(
                MutationPublicationBlocker::NotMetadataSafe(self.compatibility),
            );
        }

        if let Some(blocker) = self.rebuild_plan().publication_blocker() {
            return MutationPublicationStatus::Blocked(blocker);
        }

        MutationPublicationStatus::Publishable
    }

    /// Consult runner preflight before deciding whether publication can proceed.
    /// `PhysicalWorkReady` is still not publishable in 0.152; it only means a
    /// future runner advertises the capabilities required before execution can
    /// start.
    #[allow(
        dead_code,
        reason = "0.152 stages runner preflight publication checks before physical runners consume them"
    )]
    #[must_use]
    pub(in crate::db::schema) fn publication_preflight(
        &self,
        runner: &SchemaMutationRunnerContract,
    ) -> MutationPublicationPreflight {
        match runner.preflight(&self.execution_plan()) {
            SchemaMutationRunnerPreflight::NoPhysicalWork => match self.publication_status() {
                MutationPublicationStatus::Publishable => {
                    MutationPublicationPreflight::PublishableNow
                }
                MutationPublicationStatus::Blocked(blocker) => {
                    MutationPublicationPreflight::Blocked(blocker)
                }
            },
            SchemaMutationRunnerPreflight::Ready {
                step_count,
                required,
            } => MutationPublicationPreflight::PhysicalWorkReady {
                step_count,
                required,
            },
            SchemaMutationRunnerPreflight::MissingCapabilities { missing } => {
                MutationPublicationPreflight::MissingRunnerCapabilities { missing }
            }
            SchemaMutationRunnerPreflight::Rejected { requirement } => {
                MutationPublicationPreflight::Rejected { requirement }
            }
        }
    }

    /// Derive the physical rebuild plan required before this catalog mutation
    /// can safely become accepted runtime schema.
    #[must_use]
    pub(in crate::db::schema) fn rebuild_plan(&self) -> SchemaRebuildPlan {
        if matches!(self.rebuild, RebuildRequirement::NoRebuildRequired) {
            return SchemaRebuildPlan::no_rebuild();
        }

        let mut actions = Vec::new();
        for mutation in &self.mutations {
            match mutation {
                SchemaMutation::AddNullableField { .. }
                | SchemaMutation::AddDefaultedField { .. } => {}
                SchemaMutation::AddNonUniqueFieldPathIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildFieldPathIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::AddExpressionIndex { target } => {
                    actions.push(SchemaRebuildAction::BuildExpressionIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::DropNonRequiredSecondaryIndex { target } => {
                    actions.push(SchemaRebuildAction::DropSecondaryIndex {
                        target: target.clone(),
                    });
                }
                SchemaMutation::AlterNullability { .. } => {
                    actions.push(SchemaRebuildAction::Unsupported {
                        reason: "alter nullability requires data proof or rewrite",
                    });
                }
            }
        }

        if actions.is_empty() {
            actions.push(match self.rebuild {
                RebuildRequirement::FullDataRewriteRequired => SchemaRebuildAction::RewriteAllRows,
                RebuildRequirement::Unsupported => SchemaRebuildAction::Unsupported {
                    reason: "unsupported schema mutation",
                },
                RebuildRequirement::IndexRebuildRequired => SchemaRebuildAction::Unsupported {
                    reason: "index rebuild mutation lacks an index target",
                },
                RebuildRequirement::NoRebuildRequired => {
                    unreachable!("no-rebuild plans returned before rebuild action derivation",)
                }
            });
        }

        SchemaRebuildPlan::new(self.rebuild, actions)
    }

    /// Derive the future physical execution contract for this mutation plan.
    /// Startup reconciliation still uses `publication_status` and remains
    /// fail-closed for every plan that requires physical work.
    #[allow(
        dead_code,
        reason = "0.152 stages execution-boundary contracts before physical runners consume them"
    )]
    #[must_use]
    pub(in crate::db::schema) fn execution_plan(&self) -> SchemaMutationExecutionPlan {
        SchemaMutationExecutionPlan::from_rebuild_plan(self.rebuild_plan())
    }

    /// Return how many appended fields are represented by this plan.
    #[cfg(test)]
    pub(in crate::db::schema) fn added_field_count(&self) -> usize {
        self.mutations
            .iter()
            .filter(|mutation| {
                matches!(
                    mutation,
                    SchemaMutation::AddNullableField { .. }
                        | SchemaMutation::AddDefaultedField { .. }
                )
            })
            .count()
    }

    /// Compute a deterministic plan fingerprint. This is not a cache key yet;
    /// it is a stable audit identity for mutation semantics.
    #[allow(
        dead_code,
        reason = "0.152 stages mutation audit identity before diagnostics expose it"
    )]
    pub(in crate::db::schema) fn fingerprint(&self) -> [u8; 16] {
        let mut hasher = new_hash_sha256_prefixed(SCHEMA_MUTATION_FINGERPRINT_PROFILE_TAG);
        write_hash_tag_u8(&mut hasher, self.compatibility.tag());
        write_hash_tag_u8(&mut hasher, self.rebuild.tag());
        write_hash_u32(
            &mut hasher,
            u32::try_from(self.mutations.len()).unwrap_or(u32::MAX),
        );

        for mutation in &self.mutations {
            mutation.hash_into(&mut hasher);
        }

        let digest = finalize_hash_sha256(hasher);
        let mut fingerprint = [0u8; 16];
        fingerprint.copy_from_slice(&digest[..16]);
        fingerprint
    }
}

impl SchemaMutationRequest<'_> {
    /// Lower one accepted non-unique field-path index snapshot into a mutation
    /// request. Unique and expression/mixed indexes fail closed until their
    /// rebuild validators exist.
    #[allow(
        dead_code,
        reason = "0.152 stages accepted index mutation lowering before DDL/rebuild callers use it"
    )]
    pub(in crate::db::schema) fn from_accepted_non_unique_field_path_index(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        if index.unique() {
            return Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation);
        }

        let PersistedIndexKeySnapshot::FieldPath(paths) = index.key() else {
            return Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape);
        };

        if paths.is_empty() {
            return Err(AcceptedSchemaMutationError::EmptyIndexKey);
        }

        let key_paths = paths.iter().map(field_path_rebuild_key).collect();

        Ok(Self::AddNonUniqueFieldPathIndex {
            target: SchemaFieldPathIndexRebuildTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
                key_paths,
            },
        })
    }

    /// Lower one accepted deterministic expression index snapshot into a
    /// mutation request. Unique indexes, field-path-only keys, and empty keys
    /// fail closed until their validators and rebuild semantics exist.
    #[allow(
        dead_code,
        reason = "0.152 stages accepted expression-index mutation lowering before DDL/rebuild callers use it"
    )]
    pub(in crate::db::schema) fn from_accepted_expression_index(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        if index.unique() {
            return Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation);
        }

        let PersistedIndexKeySnapshot::Items(items) = index.key() else {
            return Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape);
        };

        if items.is_empty() {
            return Err(AcceptedSchemaMutationError::EmptyIndexKey);
        }

        let mut has_expression = false;
        let key_items = items
            .iter()
            .map(|item| match item {
                PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                    SchemaExpressionIndexRebuildKey::FieldPath(field_path_rebuild_key(path))
                }
                PersistedIndexKeyItemSnapshot::Expression(expression) => {
                    has_expression = true;
                    SchemaExpressionIndexRebuildKey::Expression(Box::new(
                        SchemaExpressionIndexRebuildExpression {
                            op: expression.op(),
                            source: field_path_rebuild_key(expression.source()),
                            input_kind: expression.input_kind().clone(),
                            output_kind: expression.output_kind().clone(),
                            canonical_text: expression.canonical_text().to_string(),
                        },
                    ))
                }
            })
            .collect();

        if !has_expression {
            return Err(AcceptedSchemaMutationError::ExpressionIndexRequiresExpressionKey);
        }

        Ok(Self::AddExpressionIndex {
            target: SchemaExpressionIndexRebuildTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
                key_items,
            },
        })
    }

    /// Lower one accepted non-unique secondary index snapshot into a cleanup
    /// request. Unique indexes are constraints and stay fail-closed until drop
    /// policy can prove constraint removal explicitly.
    #[allow(
        dead_code,
        reason = "0.152 stages accepted index cleanup lowering before DDL/rebuild callers use it"
    )]
    pub(in crate::db::schema) fn from_accepted_non_unique_secondary_index_drop(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        if index.unique() {
            return Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation);
        }

        Ok(Self::DropNonRequiredSecondaryIndex {
            target: SchemaSecondaryIndexDropCleanupTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
            },
        })
    }

    /// Lower this request into the deterministic mutation plan consumed by
    /// transition, publication, and future rebuild orchestration.
    #[must_use]
    pub(in crate::db::schema) fn lower_to_plan(self) -> MutationPlan {
        match self {
            Self::ExactMatch => MutationPlan::exact_match(),
            Self::AppendOnlyFields(fields) => MutationPlan::append_only_fields(fields),
            Self::AddNonUniqueFieldPathIndex { target } => {
                MutationPlan::non_unique_field_path_index_addition(target)
            }
            Self::AddExpressionIndex { target } => MutationPlan::expression_index_addition(target),
            Self::DropNonRequiredSecondaryIndex { target } => {
                MutationPlan::secondary_index_drop(target)
            }
            Self::AlterNullability { field_id } => MutationPlan::nullability_alteration(field_id),
            Self::Incompatible => MutationPlan::incompatible(),
        }
    }
}

impl<'a> From<SchemaMutationDelta<'a>> for SchemaMutationRequest<'a> {
    fn from(delta: SchemaMutationDelta<'a>) -> Self {
        match delta {
            SchemaMutationDelta::AppendOnlyFields(fields) => Self::AppendOnlyFields(fields),
            SchemaMutationDelta::ExactMatch => Self::ExactMatch,
            SchemaMutationDelta::Incompatible => Self::Incompatible,
        }
    }
}

impl SchemaMutation {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            Self::AddNullableField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 1);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddDefaultedField {
                field_id,
                name,
                slot,
            } => {
                write_hash_tag_u8(hasher, 2);
                hash_field_identity(hasher, *field_id, name, *slot);
            }
            Self::AddNonUniqueFieldPathIndex { target } => {
                write_hash_tag_u8(hasher, 3);
                target.hash_into(hasher);
            }
            Self::AddExpressionIndex { target } => {
                write_hash_tag_u8(hasher, 4);
                target.hash_into(hasher);
            }
            Self::DropNonRequiredSecondaryIndex { target } => {
                write_hash_tag_u8(hasher, 5);
                target.hash_into(hasher);
            }
            Self::AlterNullability { field_id } => {
                write_hash_tag_u8(hasher, 6);
                write_hash_u32(hasher, field_id.get());
            }
        }
    }
}

impl MutationCompatibility {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    const fn tag(self) -> u8 {
        match self {
            Self::MetadataOnlySafe => 1,
            Self::RequiresRebuild => 2,
            Self::UnsupportedPreOne => 3,
            Self::Incompatible => 4,
        }
    }
}

impl RebuildRequirement {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    const fn tag(self) -> u8 {
        match self {
            Self::NoRebuildRequired => 1,
            Self::IndexRebuildRequired => 2,
            Self::FullDataRewriteRequired => 3,
            Self::Unsupported => 4,
        }
    }
}

impl SchemaFieldPathIndexRebuildTarget {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
        write_hash_u32(
            hasher,
            u32::try_from(self.key_paths.len()).unwrap_or(u32::MAX),
        );
        for key_path in &self.key_paths {
            key_path.hash_into(hasher);
        }
    }
}

impl SchemaFieldPathIndexRebuildKey {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, self.field_id.get());
        write_hash_u32(hasher, u32::from(self.slot.get()));
        write_hash_u32(hasher, u32::try_from(self.path.len()).unwrap_or(u32::MAX));
        for segment in &self.path {
            write_hash_str_u32(hasher, segment);
        }
        write_hash_str_u32(hasher, &format!("{:?}", self.kind));
        write_hash_bool(hasher, self.nullable);
    }
}

impl SchemaExpressionIndexRebuildTarget {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
        write_hash_u32(
            hasher,
            u32::try_from(self.key_items.len()).unwrap_or(u32::MAX),
        );
        for key_item in &self.key_items {
            key_item.hash_into(hasher);
        }
    }
}

impl SchemaExpressionIndexRebuildKey {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            Self::FieldPath(path) => {
                write_hash_tag_u8(hasher, 1);
                path.hash_into(hasher);
            }
            Self::Expression(expression) => {
                write_hash_tag_u8(hasher, 2);
                expression.hash_into(hasher);
            }
        }
    }
}

impl SchemaExpressionIndexRebuildExpression {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, self.op as u32);
        self.source.hash_into(hasher);
        write_hash_str_u32(hasher, &format!("{:?}", self.input_kind));
        write_hash_str_u32(hasher, &format!("{:?}", self.output_kind));
        write_hash_str_u32(hasher, &self.canonical_text);
    }
}

impl SchemaSecondaryIndexDropCleanupTarget {
    #[allow(
        dead_code,
        reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
    )]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
    }
}

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
fn hash_field_identity(
    hasher: &mut sha2::Sha256,
    field_id: FieldId,
    name: &str,
    slot: SchemaFieldSlot,
) {
    write_hash_u32(hasher, field_id.get());
    write_hash_str_u32(hasher, name);
    write_hash_u32(hasher, u32::from(slot.get()));
}

#[allow(
    dead_code,
    reason = "used by mutation fingerprint tests until audit identity is surfaced in diagnostics"
)]
fn write_hash_bool(hasher: &mut sha2::Sha256, value: bool) {
    write_hash_tag_u8(hasher, u8::from(value));
}

fn field_path_rebuild_key(
    path: &PersistedIndexFieldPathSnapshot,
) -> SchemaFieldPathIndexRebuildKey {
    SchemaFieldPathIndexRebuildKey {
        field_id: path.field_id(),
        slot: path.slot(),
        path: path.path().to_vec(),
        kind: path.kind().clone(),
        nullable: path.nullable(),
    }
}

#[allow(
    dead_code,
    reason = "0.152 stages runner capability contracts before physical runners consume them"
)]
fn push_runner_capability_once(
    capabilities: &mut Vec<SchemaMutationRunnerCapability>,
    capability: SchemaMutationRunnerCapability,
) {
    if !capabilities.contains(&capability) {
        capabilities.push(capability);
    }
}

#[allow(
    dead_code,
    reason = "0.153 stages runtime epoch identity before physical runners publish snapshots"
)]
fn runtime_epoch_fingerprint(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<[u8; 16], InternalError> {
    let encoded_snapshot = encode_persisted_schema_snapshot(snapshot)?;
    let mut hasher = new_hash_sha256_prefixed(SCHEMA_MUTATION_RUNTIME_EPOCH_PROFILE_TAG);
    write_hash_str_u32(&mut hasher, snapshot.entity_path());
    write_hash_u32(&mut hasher, snapshot.version().get());
    write_hash_u32(
        &mut hasher,
        u32::try_from(encoded_snapshot.len()).unwrap_or(u32::MAX),
    );
    hasher.update(encoded_snapshot);
    let digest = finalize_hash_sha256(hasher);
    let mut fingerprint = [0u8; 16];
    fingerprint.copy_from_slice(&digest[..16]);

    Ok(fingerprint)
}

// Return generated fields for the additive shape that can become an accepted
// mutation plan: stored fields and row-layout entries must be exact prefixes of
// the generated proposal. Absence/default policy is validated by transition.
fn append_only_additive_fields<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    if actual.fields().len() >= expected.fields().len()
        || actual.row_layout().field_to_slot().len() >= expected.row_layout().field_to_slot().len()
    {
        return None;
    }

    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return None;
    }

    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return None;
    }

    Some(&expected.fields()[actual.fields().len()..])
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            data::{
                CanonicalSlotReader, ScalarSlotValueRef, SlotReader, StorageKey,
                StructuralFieldDecodeContract,
            },
            index::{IndexId, IndexKey},
            schema::{
                AcceptedSchemaMutationError, FieldId, MutationCompatibility, MutationPlan,
                PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexExpressionOp,
                PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
                PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
                PersistedSchemaSnapshot, RebuildRequirement, SchemaFieldDefault, SchemaFieldSlot,
                SchemaMutation, SchemaMutationDelta, SchemaMutationRequest, SchemaRebuildAction,
                SchemaRowLayout, SchemaVersion, classify_schema_mutation_delta,
                mutation::{MutationPublicationBlocker, MutationPublicationStatus},
                schema_mutation_request_for_snapshots,
            },
        },
        error::InternalError,
        model::field::FieldModel,
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
        value::Value,
    };
    use std::borrow::Cow;

    struct RebuildSlotReader {
        values: Vec<Option<Value>>,
    }

    impl SlotReader for RebuildSlotReader {
        fn generated_compatible_field_model(
            &self,
            _slot: usize,
        ) -> Result<&FieldModel, InternalError> {
            panic!("rebuild key test reader should not reopen generated field models")
        }

        fn has(&self, slot: usize) -> bool {
            self.values.get(slot).is_some_and(Option::is_some)
        }

        fn get_bytes(&self, _slot: usize) -> Option<&[u8]> {
            panic!("rebuild key test reader should not decode raw bytes")
        }

        fn get_scalar(
            &self,
            _slot: usize,
        ) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
            panic!("rebuild key test reader should not route through scalar fast paths")
        }

        fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
            panic!("rebuild key test reader should not route through generated get_value")
        }
    }

    impl CanonicalSlotReader for RebuildSlotReader {
        fn field_decode_contract(
            &self,
            _slot: usize,
        ) -> Result<StructuralFieldDecodeContract, InternalError> {
            panic!("rebuild key test reader should not decode through field contracts")
        }

        fn required_value_by_contract_cow(
            &self,
            slot: usize,
        ) -> Result<Cow<'_, Value>, InternalError> {
            self.values
                .get(slot)
                .and_then(Option::as_ref)
                .map(Cow::Borrowed)
                .ok_or_else(|| InternalError::persisted_row_declared_field_missing("test"))
        }
    }

    fn nullable_text_field(name: &str, id: u32, slot: u16) -> PersistedFieldSnapshot {
        PersistedFieldSnapshot::new(
            FieldId::new(id),
            name.to_string(),
            SchemaFieldSlot::new(slot),
            PersistedFieldKind::Text { max_len: None },
            Vec::new(),
            true,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        )
    }

    fn non_unique_name_index() -> PersistedIndexSnapshot {
        PersistedIndexSnapshot::new(
            1,
            "by_name".to_string(),
            "test::mutation::by_name".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["name".to_string()],
                PersistedFieldKind::Text { max_len: None },
                false,
            )]),
            Some("name IS NOT NULL".to_string()),
        )
    }

    fn name_key_path() -> PersistedIndexFieldPathSnapshot {
        PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["name".to_string()],
            PersistedFieldKind::Text { max_len: None },
            false,
        )
    }

    fn expression_name_index() -> PersistedIndexSnapshot {
        PersistedIndexSnapshot::new(
            2,
            "by_lower_name".to_string(),
            "test::mutation::by_lower_name".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::Expression(
                Box::new(PersistedIndexExpressionSnapshot::new(
                    PersistedIndexExpressionOp::Lower,
                    name_key_path(),
                    PersistedFieldKind::Text { max_len: None },
                    PersistedFieldKind::Text { max_len: None },
                    "expr:v1:LOWER(name)".to_string(),
                )),
            )]),
            Some("LOWER(name) IS NOT NULL".to_string()),
        )
    }

    #[test]
    fn append_only_field_mutation_plan_is_no_rebuild() {
        let field = nullable_text_field("nickname", 3, 2);
        let plan = MutationPlan::append_only_fields(&[field]);

        assert_eq!(
            plan.compatibility(),
            MutationCompatibility::MetadataOnlySafe
        );
        assert_eq!(
            plan.rebuild_requirement(),
            RebuildRequirement::NoRebuildRequired
        );
        assert_eq!(plan.added_field_count(), 1);
        assert_eq!(
            plan.mutations(),
            &[SchemaMutation::AddNullableField {
                field_id: FieldId::new(3),
                name: "nickname".to_string(),
                slot: SchemaFieldSlot::new(2),
            }]
        );
    }

    #[test]
    fn mutation_plan_fingerprint_is_deterministic_and_semantic() {
        let nickname = nullable_text_field("nickname", 3, 2);
        let handle = nullable_text_field("handle", 3, 2);
        let first = MutationPlan::append_only_fields(std::slice::from_ref(&nickname));
        let second = MutationPlan::append_only_fields(&[nickname]);
        let changed = MutationPlan::append_only_fields(&[handle]);

        assert_eq!(first.fingerprint(), second.fingerprint());
        assert_ne!(first.fingerprint(), changed.fingerprint());
    }

    #[test]
    fn index_mutation_plans_are_rebuild_gated() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let expression =
            SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
                .expect("accepted expression index should lower")
                .lower_to_plan();
        let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
            &non_unique_name_index(),
        )
        .expect("non-unique secondary index should lower to drop cleanup")
        .lower_to_plan();

        for plan in [&field_path, &expression, &drop] {
            assert_eq!(plan.compatibility(), MutationCompatibility::RequiresRebuild);
            assert_eq!(
                plan.rebuild_requirement(),
                RebuildRequirement::IndexRebuildRequired
            );
            assert_eq!(
                plan.publication_status(),
                MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                    MutationCompatibility::RequiresRebuild,
                )),
            );
        }
    }

    #[test]
    fn rebuild_plan_derives_physical_index_actions() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let expression =
            SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
                .expect("accepted expression index should lower")
                .lower_to_plan();
        let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
            &non_unique_name_index(),
        )
        .expect("non-unique secondary index should lower to drop cleanup")
        .lower_to_plan();

        let field_path_rebuild = field_path.rebuild_plan();
        let [SchemaRebuildAction::BuildFieldPathIndex { target }] = field_path_rebuild.actions()
        else {
            panic!("field-path index addition should derive one field-path rebuild target");
        };
        assert_eq!(target.ordinal(), 1);
        assert_eq!(target.name(), "by_name");
        assert_eq!(target.store(), "test::mutation::by_name");
        assert!(!target.unique());
        assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
        let [key_path] = target.key_paths() else {
            panic!("field-path rebuild target should carry one accepted key path");
        };
        assert_eq!(key_path.field_id(), FieldId::new(2));
        assert_eq!(key_path.slot(), SchemaFieldSlot::new(1));
        assert_eq!(key_path.path(), &["name".to_string()]);
        assert_eq!(key_path.kind(), &PersistedFieldKind::Text { max_len: None });
        assert!(!key_path.nullable());
        let expression_rebuild = expression.rebuild_plan();
        let [SchemaRebuildAction::BuildExpressionIndex { target }] = expression_rebuild.actions()
        else {
            panic!("expression index addition should derive one expression rebuild target");
        };
        assert_eq!(target.ordinal(), 2);
        assert_eq!(target.name(), "by_lower_name");
        assert_eq!(target.store(), "test::mutation::by_lower_name");
        assert!(!target.unique());
        assert_eq!(target.predicate_sql(), Some("LOWER(name) IS NOT NULL"));
        let [super::SchemaExpressionIndexRebuildKey::Expression(expression)] = target.key_items()
        else {
            panic!("expression rebuild target should carry one expression key");
        };
        assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
        assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
        assert_eq!(
            expression.input_kind(),
            &PersistedFieldKind::Text { max_len: None }
        );
        assert_eq!(
            expression.output_kind(),
            &PersistedFieldKind::Text { max_len: None }
        );
        assert_eq!(expression.source().field_id(), FieldId::new(2));
        assert_eq!(expression.source().slot(), SchemaFieldSlot::new(1));
        let drop_rebuild = drop.rebuild_plan();
        let [SchemaRebuildAction::DropSecondaryIndex { target }] = drop_rebuild.actions() else {
            panic!("secondary index drop should derive one cleanup target");
        };
        assert_eq!(target.ordinal(), 1);
        assert_eq!(target.name(), "by_name");
        assert_eq!(target.store(), "test::mutation::by_name");
        assert!(!target.unique());
        assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
    }

    #[test]
    fn execution_plan_keeps_metadata_only_mutations_publishable_without_steps() {
        let field = nullable_text_field("nickname", 3, 2);
        let plan = MutationPlan::append_only_fields(&[field]);
        let execution = plan.execution_plan();

        assert_eq!(
            execution.readiness(),
            super::SchemaMutationExecutionReadiness::PublishableNow,
        );
        assert!(execution.steps().is_empty());
        assert!(execution.runner_capabilities().is_empty());
        assert_eq!(
            execution.execution_gate(),
            super::SchemaMutationExecutionGate::ReadyToPublish,
        );
        assert_eq!(
            execution.admit_runner_capabilities(&[]),
            super::SchemaMutationExecutionAdmission::PublishableNow,
        );
        assert_eq!(
            plan.publication_status(),
            MutationPublicationStatus::Publishable,
        );
    }

    #[test]
    fn execution_plan_schedules_index_work_before_validation_and_invalidation() {
        let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
            &non_unique_name_index(),
        )
        .expect("non-unique secondary index should lower to drop cleanup")
        .lower_to_plan();
        let execution = drop.execution_plan();

        assert_eq!(
            execution.readiness(),
            super::SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
                RebuildRequirement::IndexRebuildRequired,
            ),
        );
        assert_eq!(
            execution.execution_gate(),
            super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
                requirement: RebuildRequirement::IndexRebuildRequired,
                step_count: 3,
            },
        );
        let [
            super::SchemaMutationExecutionStep::DropSecondaryIndex { target },
            super::SchemaMutationExecutionStep::ValidatePhysicalWork,
            super::SchemaMutationExecutionStep::InvalidateRuntimeState,
        ] = execution.steps()
        else {
            panic!("drop execution should schedule cleanup, validation, and invalidation");
        };
        assert_eq!(target.name(), "by_name");
        assert_eq!(target.store(), "test::mutation::by_name");
    }

    #[test]
    fn execution_plan_reports_runner_capabilities_without_duplicates() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let expression =
            SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
                .expect("accepted expression index should lower")
                .lower_to_plan();
        let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
            &non_unique_name_index(),
        )
        .expect("non-unique secondary index should lower to drop cleanup")
        .lower_to_plan();

        assert_eq!(
            field_path.execution_plan().runner_capabilities(),
            vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
        assert_eq!(
            expression.execution_plan().runner_capabilities(),
            vec![
                super::SchemaMutationRunnerCapability::BuildExpressionIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
        assert_eq!(
            drop.execution_plan().runner_capabilities(),
            vec![
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
    }

    #[test]
    fn execution_admission_fails_closed_on_missing_runner_capabilities() {
        let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
            &non_unique_name_index(),
        )
        .expect("non-unique secondary index should lower to drop cleanup")
        .lower_to_plan();
        let execution = drop.execution_plan();

        assert_eq!(
            execution.admit_runner_capabilities(&[]),
            super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
                missing: vec![
                    super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                    super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                    super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
                ],
            },
        );
        assert_eq!(
            execution.admit_runner_capabilities(&[
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            ]),
            super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
                missing: vec![super::SchemaMutationRunnerCapability::InvalidateRuntimeState],
            },
        );
        assert_eq!(
            execution.admit_runner_capabilities(&[
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ]),
            super::SchemaMutationExecutionAdmission::RunnerReady {
                required: vec![
                    super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                    super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                    super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
                ],
            },
        );
    }

    #[test]
    fn runner_contract_preflight_deduplicates_capabilities_and_preserves_gate() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let execution = field_path.execution_plan();
        let runner = super::SchemaMutationRunnerContract::new(&[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]);

        assert_eq!(
            runner.capabilities(),
            &[
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
        assert_eq!(
            runner.preflight(&execution),
            super::SchemaMutationRunnerPreflight::Ready {
                step_count: 3,
                required: vec![
                    super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                    super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                    super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
                ],
            },
        );
        assert_eq!(
            execution.execution_gate(),
            super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
                requirement: RebuildRequirement::IndexRebuildRequired,
                step_count: 3,
            },
        );
    }

    #[test]
    fn runner_contract_preflight_keeps_no_work_and_rejections_non_executable() {
        let metadata_only =
            MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
        let rewrite = SchemaMutationRequest::Incompatible.lower_to_plan();
        let unsupported = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let runner = super::SchemaMutationRunnerContract::new(&[
            super::SchemaMutationRunnerCapability::RewriteAllRows,
        ]);

        assert_eq!(
            runner.preflight(&metadata_only.execution_plan()),
            super::SchemaMutationRunnerPreflight::NoPhysicalWork,
        );
        assert_eq!(
            runner.preflight(&rewrite.execution_plan()),
            super::SchemaMutationRunnerPreflight::Rejected {
                requirement: RebuildRequirement::FullDataRewriteRequired,
            },
        );
        assert_eq!(
            runner.preflight(&unsupported.execution_plan()),
            super::SchemaMutationRunnerPreflight::Rejected {
                requirement: RebuildRequirement::Unsupported,
            },
        );
    }

    #[test]
    fn runner_outcome_reports_no_work_and_ready_physical_work() {
        let metadata_only =
            MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let runner = super::SchemaMutationRunnerContract::new(&[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]);

        let super::SchemaMutationRunnerOutcome::NoPhysicalWork(no_work) =
            runner.outcome(&metadata_only.execution_plan())
        else {
            panic!("metadata-only mutation should not require physical work");
        };
        assert_eq!(no_work.step_count(), 0);
        assert!(no_work.required_capabilities().is_empty());
        assert_eq!(
            no_work.completed_phases(),
            &[super::SchemaMutationRunnerPhase::Preflight],
        );
        assert!(no_work.has_completed_phase(super::SchemaMutationRunnerPhase::Preflight));
        assert_eq!(no_work.store_visibility(), None);
        assert_eq!(no_work.rows_scanned(), 0);
        assert_eq!(no_work.rows_skipped(), 0);
        assert_eq!(no_work.index_keys_written(), 0);
        assert!(!no_work.physical_work_allows_publication());

        let super::SchemaMutationRunnerOutcome::ReadyForPhysicalWork(ready) =
            runner.outcome(&field_path.execution_plan())
        else {
            panic!("field-path index mutation should be ready for staged physical work");
        };
        assert_eq!(ready.step_count(), 3);
        assert_eq!(
            ready.required_capabilities(),
            &[
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
        assert_eq!(
            ready.completed_phases(),
            &[super::SchemaMutationRunnerPhase::Preflight],
        );
        assert_eq!(
            ready.store_visibility(),
            Some(super::SchemaMutationStoreVisibility::StagedOnly),
        );
        assert_eq!(ready.rows_scanned(), 0);
        assert_eq!(ready.rows_skipped(), 0);
        assert_eq!(ready.index_keys_written(), 0);
        assert!(!ready.physical_work_allows_publication());
    }

    #[test]
    fn runner_outcome_classifies_missing_capabilities_and_unsupported_requirements() {
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
        let no_runner = super::SchemaMutationRunnerContract::new(&[]);

        let super::SchemaMutationRunnerOutcome::Rejected(missing) =
            no_runner.outcome(&field_path.execution_plan())
        else {
            panic!("missing runner capabilities should reject before physical work");
        };
        assert_eq!(missing.phase(), super::SchemaMutationRunnerPhase::Preflight);
        assert_eq!(
            missing.kind(),
            super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
        );
        assert_eq!(
            missing.requirement(),
            Some(RebuildRequirement::IndexRebuildRequired),
        );
        assert_eq!(
            missing.missing_capabilities(),
            &[
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );

        let super::SchemaMutationRunnerOutcome::Rejected(unsupported) =
            no_runner.outcome(&incompatible.execution_plan())
        else {
            panic!("full rewrite should remain rejected by runner outcome");
        };
        assert_eq!(
            unsupported.kind(),
            super::SchemaMutationRunnerRejectionKind::UnsupportedRequirement,
        );
        assert_eq!(
            unsupported.requirement(),
            Some(RebuildRequirement::FullDataRewriteRequired),
        );
        assert!(unsupported.missing_capabilities().is_empty());
    }

    #[test]
    fn runner_input_binds_accepted_snapshots_to_execution_plan() {
        let before = base_snapshot();
        let added = nullable_text_field("nickname", 3, 2);
        let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
        let plan = MutationPlan::append_only_fields(&[added]);
        let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.execution_plan())
            .expect("same-entity accepted snapshots should build runner input");
        let runner = super::SchemaMutationRunnerContract::new(&[]);

        assert_eq!(input.accepted_before().entity_path(), before.entity_path());
        assert_eq!(
            input.accepted_after().fields().len(),
            before.fields().len() + 1,
        );
        assert_eq!(
            input.execution_plan().readiness(),
            super::SchemaMutationExecutionReadiness::PublishableNow,
        );
        assert!(matches!(
            input.outcome(&runner),
            super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
        ));
    }

    #[test]
    fn runner_input_rejects_cross_entity_snapshot_pairs() {
        let before = base_snapshot();
        let wrong_entity = PersistedSchemaSnapshot::new(
            before.version(),
            "test::OtherEntity".to_string(),
            before.entity_name().to_string(),
            before.primary_key_field_id(),
            before.row_layout().clone(),
            before.fields().to_vec(),
        );
        let wrong_name = PersistedSchemaSnapshot::new(
            before.version(),
            before.entity_path().to_string(),
            "OtherEntity".to_string(),
            before.primary_key_field_id(),
            before.row_layout().clone(),
            before.fields().to_vec(),
        );
        let wrong_pk = PersistedSchemaSnapshot::new(
            before.version(),
            before.entity_path().to_string(),
            before.entity_name().to_string(),
            FieldId::new(99),
            before.row_layout().clone(),
            before.fields().to_vec(),
        );

        assert_eq!(
            super::SchemaMutationRunnerInput::new(
                &before,
                &wrong_entity,
                MutationPlan::exact_match().execution_plan(),
            ),
            Err(super::SchemaMutationRunnerInputError::EntityPath),
        );
        assert_eq!(
            super::SchemaMutationRunnerInput::new(
                &before,
                &wrong_name,
                MutationPlan::exact_match().execution_plan(),
            ),
            Err(super::SchemaMutationRunnerInputError::EntityName),
        );
        assert_eq!(
            super::SchemaMutationRunnerInput::new(
                &before,
                &wrong_pk,
                MutationPlan::exact_match().execution_plan(),
            ),
            Err(super::SchemaMutationRunnerInputError::PrimaryKeyField),
        );
    }

    #[test]
    fn noop_runner_accepts_metadata_only_input_and_rejects_physical_work() {
        let before = base_snapshot();
        let added = nullable_text_field("nickname", 3, 2);
        let metadata_after = append_fields_snapshot(&before, std::slice::from_ref(&added));
        let metadata_input = super::SchemaMutationRunnerInput::new(
            &before,
            &metadata_after,
            MutationPlan::append_only_fields(&[added]).execution_plan(),
        )
        .expect("metadata-only same-entity input should build");
        let index_after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
        let index_plan = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let index_input = super::SchemaMutationRunnerInput::new(
            &before,
            &index_after,
            index_plan.execution_plan(),
        )
        .expect("index same-entity input should build");
        let runner = super::SchemaMutationNoopRunner::new();

        assert!(matches!(
            runner.run(&metadata_input),
            super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
        ));

        let super::SchemaMutationRunnerOutcome::Rejected(rejection) = runner.run(&index_input)
        else {
            panic!("no-op runner must reject physical index work");
        };
        assert_eq!(
            rejection.kind(),
            super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
        );
        assert_eq!(
            rejection.requirement(),
            Some(RebuildRequirement::IndexRebuildRequired),
        );
        assert_eq!(
            rejection.missing_capabilities(),
            &[
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
    }

    #[test]
    fn runtime_epoch_identity_tracks_accepted_snapshot_changes() {
        let before = base_snapshot();
        let repeated_before = base_snapshot();
        let added = nullable_text_field("nickname", 3, 2);
        let after = append_fields_snapshot(&before, &[added]);

        let before_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&before)
            .expect("base snapshot should hash into runtime epoch");
        let repeated_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&repeated_before)
            .expect("same snapshot should hash into runtime epoch");
        let after_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&after)
            .expect("changed snapshot should hash into runtime epoch");

        assert_eq!(before_epoch, repeated_epoch);
        assert_ne!(before_epoch, after_epoch);
        assert_eq!(before_epoch.entity_path(), before.entity_path());
        assert_eq!(after_epoch.schema_version(), after.version());
        assert_ne!(
            before_epoch.snapshot_fingerprint(),
            after_epoch.snapshot_fingerprint(),
        );
    }

    #[test]
    fn publication_identity_keeps_staged_epoch_invisible_until_published() {
        let before = base_snapshot();
        let added = nullable_text_field("nickname", 3, 2);
        let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
        let input = super::SchemaMutationRunnerInput::new(
            &before,
            &after,
            MutationPlan::append_only_fields(&[added]).execution_plan(),
        )
        .expect("same-entity metadata input should build");

        let staged = super::SchemaMutationPublicationIdentity::from_input(
            &input,
            super::SchemaMutationStoreVisibility::StagedOnly,
        )
        .expect("staged publication identity should derive from snapshots");
        let published = super::SchemaMutationPublicationIdentity::from_input(
            &input,
            super::SchemaMutationStoreVisibility::Published,
        )
        .expect("published publication identity should derive from snapshots");

        assert!(staged.changes_epoch());
        assert_eq!(
            staged.store_visibility(),
            super::SchemaMutationStoreVisibility::StagedOnly,
        );
        assert_eq!(staged.visible_epoch(), staged.before_epoch());
        assert_eq!(staged.published_epoch(), None);
        assert_eq!(
            published.store_visibility(),
            super::SchemaMutationStoreVisibility::Published,
        );
        assert_eq!(published.visible_epoch(), published.after_epoch());
        assert_eq!(published.published_epoch(), Some(published.after_epoch()));
    }

    #[test]
    fn field_path_rebuild_key_materializes_from_accepted_target_slots() {
        let request = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target");
        let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
            panic!("field-path index request should preserve rebuild target");
        };
        let slots = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Ada".to_string()))],
        };
        let storage_key = crate::db::data::StorageKey::Uint(42);

        let key = IndexKey::new_from_slots_with_field_path_rebuild_target(
            EntityTag::new(7),
            storage_key,
            &target,
            &slots,
        )
        .expect("accepted field-path target should build index key")
        .expect("text key component should be indexable");

        assert_eq!(key.index_id(), &IndexId::new(EntityTag::new(7), 1));
        assert_eq!(key.component_count(), 1);
        assert_eq!(
            key.primary_storage_key()
                .expect("index key should carry primary storage key"),
            storage_key,
        );
    }

    #[test]
    fn field_path_rebuild_stages_sorted_entries_without_publication() {
        let request = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target");
        let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
            panic!("field-path index request should preserve rebuild target");
        };
        let first = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Ada".to_string()))],
        };
        let skipped = RebuildSlotReader {
            values: vec![None, Some(Value::Null)],
        };
        let second = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Grace".to_string()))],
        };

        let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
            "test::mutation::entity",
            EntityTag::new(7),
            target,
            [
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(3), &skipped),
            ],
        )
        .expect("field-path rebuild rows should stage into raw index entries");

        assert_eq!(staged.target().name(), "by_name");
        assert_eq!(staged.source_rows(), 3);
        assert_eq!(staged.skipped_rows(), 1);
        assert_eq!(staged.entries().len(), 2);
        assert_eq!(
            staged.store_visibility(),
            super::SchemaMutationStoreVisibility::StagedOnly,
        );
        assert!(
            staged
                .entries()
                .windows(2)
                .all(|pair| pair[0].key() <= pair[1].key())
        );
        let staged_members = staged
            .entries()
            .iter()
            .map(|entry| {
                entry
                    .entry()
                    .try_decode()
                    .expect("staged entry should decode")
                    .iter_ids()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            staged_members,
            vec![vec![StorageKey::Uint(1)], vec![StorageKey::Uint(2)]],
        );

        let validation = staged
            .validate()
            .expect("fresh staged rebuild output should validate");
        assert_eq!(validation.entry_count(), 2);
        assert_eq!(validation.source_rows(), 3);
        assert_eq!(validation.skipped_rows(), 1);
        assert_eq!(
            validation.store_visibility(),
            super::SchemaMutationStoreVisibility::StagedOnly,
        );
    }

    #[test]
    fn field_path_rebuild_validation_fails_closed_for_mutated_staged_state() {
        let request = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target");
        let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
            panic!("field-path index request should preserve rebuild target");
        };
        let first = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Ada".to_string()))],
        };
        let second = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Grace".to_string()))],
        };
        let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
            "test::mutation::entity",
            EntityTag::new(7),
            target,
            [
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
            ],
        )
        .expect("field-path rebuild rows should stage into raw index entries");

        let mut duplicate = staged.clone();
        duplicate.entries[1] = duplicate.entries[0].clone();
        assert_eq!(
            duplicate.validate(),
            Err(super::SchemaFieldPathIndexStagedValidationError::UnsortedOrDuplicateEntries),
        );
        let plan = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target")
        .lower_to_plan();
        let rejection = duplicate
            .validated_runner_report(&plan.execution_plan())
            .expect_err("invalid staged state should reject runner reporting");
        assert_eq!(
            rejection.phase(),
            super::SchemaMutationRunnerPhase::ValidatePhysicalState,
        );
        assert_eq!(
            rejection.kind(),
            super::SchemaMutationRunnerRejectionKind::ValidationFailed,
        );
        assert_eq!(
            rejection.requirement(),
            Some(RebuildRequirement::IndexRebuildRequired),
        );

        let mut mismatched_count = staged.clone();
        mismatched_count.skipped_rows = 1;
        assert_eq!(
            mismatched_count.validate(),
            Err(super::SchemaFieldPathIndexStagedValidationError::EntryCountMismatch),
        );

        let mut published = staged;
        published.store_visibility = super::SchemaMutationStoreVisibility::Published;
        assert_eq!(
            published.validate(),
            Err(super::SchemaFieldPathIndexStagedValidationError::PublishedVisibility),
        );
    }

    #[test]
    fn field_path_rebuild_validation_reports_runner_diagnostics_without_publication() {
        let plan = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let request = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target");
        let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
            panic!("field-path index request should preserve rebuild target");
        };
        let first = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Ada".to_string()))],
        };
        let skipped = RebuildSlotReader {
            values: vec![None, Some(Value::Null)],
        };
        let second = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Grace".to_string()))],
        };
        let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
            "test::mutation::entity",
            EntityTag::new(7),
            target,
            [
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(3), &skipped),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
            ],
        )
        .expect("field-path rebuild rows should stage into raw index entries");

        let report = staged
            .validated_runner_report(&plan.execution_plan())
            .expect("valid staged rebuild output should produce runner diagnostics");

        assert_eq!(report.step_count(), 3);
        assert_eq!(
            report.required_capabilities(),
            &[
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        );
        assert_eq!(
            report.completed_phases(),
            &[
                super::SchemaMutationRunnerPhase::Preflight,
                super::SchemaMutationRunnerPhase::StageStores,
                super::SchemaMutationRunnerPhase::BuildPhysicalState,
                super::SchemaMutationRunnerPhase::ValidatePhysicalState,
            ],
        );
        assert_eq!(
            report.store_visibility(),
            Some(super::SchemaMutationStoreVisibility::StagedOnly),
        );
        assert_eq!(report.rows_scanned(), 3);
        assert_eq!(report.rows_skipped(), 1);
        assert_eq!(report.index_keys_written(), 2);
        assert!(
            report.has_completed_phase(super::SchemaMutationRunnerPhase::ValidatePhysicalState)
        );
        assert!(
            !report.has_completed_phase(super::SchemaMutationRunnerPhase::InvalidateRuntimeState)
        );
        assert!(!report.physical_work_allows_publication());
    }

    #[test]
    fn field_path_rebuild_writes_validated_entries_to_staged_store_buffer() {
        let plan = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let request = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower to a rebuild target");
        let SchemaMutationRequest::AddNonUniqueFieldPathIndex { target } = request else {
            panic!("field-path index request should preserve rebuild target");
        };
        let first = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Ada".to_string()))],
        };
        let second = RebuildSlotReader {
            values: vec![None, Some(Value::Text("Grace".to_string()))],
        };
        let staged = super::SchemaFieldPathIndexStagedRebuild::from_rows(
            "test::mutation::entity",
            EntityTag::new(7),
            target,
            [
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(2), &second),
                super::SchemaFieldPathIndexRebuildRow::new(StorageKey::Uint(1), &first),
            ],
        )
        .expect("field-path rebuild rows should stage into raw index entries");

        let buffer =
            super::SchemaFieldPathIndexStagedStore::from_rebuild(&staged, &plan.execution_plan())
                .expect("valid staged rebuild should write into an in-memory staged store buffer");

        assert_eq!(buffer.store(), "test::mutation::by_name");
        assert_eq!(buffer.entries(), staged.entries());
        assert_eq!(buffer.validation().entry_count(), 2);
        assert_eq!(
            buffer.store_visibility(),
            super::SchemaMutationStoreVisibility::StagedOnly,
        );
        assert_eq!(buffer.report().rows_scanned(), 2);
        assert_eq!(buffer.report().index_keys_written(), 2);
        assert!(!buffer.physical_work_allows_publication());

        let discard = buffer.discard();
        assert_eq!(discard.store(), "test::mutation::by_name");
        assert_eq!(discard.discarded_entries(), 2);
        assert_eq!(
            discard.store_visibility(),
            super::SchemaMutationStoreVisibility::StagedOnly,
        );
    }

    #[test]
    fn execution_plan_keeps_full_rewrite_and_unsupported_non_executable() {
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
        let rewrite_execution = incompatible.execution_plan();

        assert_eq!(
            rewrite_execution.readiness(),
            super::SchemaMutationExecutionReadiness::Unsupported(
                RebuildRequirement::FullDataRewriteRequired,
            ),
        );
        assert_eq!(
            rewrite_execution.execution_gate(),
            super::SchemaMutationExecutionGate::Rejected {
                requirement: RebuildRequirement::FullDataRewriteRequired,
            },
        );
        assert_eq!(
            rewrite_execution.steps(),
            &[super::SchemaMutationExecutionStep::RewriteAllRows],
        );
        assert_eq!(
            rewrite_execution.runner_capabilities(),
            vec![super::SchemaMutationRunnerCapability::RewriteAllRows],
        );
        assert_eq!(
            rewrite_execution.admit_runner_capabilities(&[
                super::SchemaMutationRunnerCapability::RewriteAllRows,
            ]),
            super::SchemaMutationExecutionAdmission::Rejected {
                requirement: RebuildRequirement::FullDataRewriteRequired,
            },
        );

        let nullability = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let unsupported_execution = nullability.execution_plan();

        assert_eq!(
            unsupported_execution.readiness(),
            super::SchemaMutationExecutionReadiness::Unsupported(RebuildRequirement::Unsupported),
        );
        assert_eq!(
            unsupported_execution.execution_gate(),
            super::SchemaMutationExecutionGate::Rejected {
                requirement: RebuildRequirement::Unsupported,
            },
        );
        assert_eq!(
            unsupported_execution.steps(),
            &[super::SchemaMutationExecutionStep::Unsupported {
                reason: "alter nullability requires data proof or rewrite",
            }],
        );
        assert!(unsupported_execution.runner_capabilities().is_empty());
    }

    #[test]
    fn field_path_index_request_lowering_fails_closed_for_unsupported_indexes() {
        let unique = PersistedIndexSnapshot::new(
            1,
            "unique_name".to_string(),
            "test::mutation::unique_name".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
            None,
        );
        let explicit_items = PersistedIndexSnapshot::new(
            2,
            "items_name".to_string(),
            "test::mutation::items_name".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
                name_key_path(),
            )]),
            None,
        );
        let empty = PersistedIndexSnapshot::new(
            3,
            "empty_name".to_string(),
            "test::mutation::empty_name".to_string(),
            false,
            PersistedIndexKeySnapshot::FieldPath(Vec::new()),
            None,
        );

        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&unique),
            Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&explicit_items),
            Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_field_path_index(&empty),
            Err(AcceptedSchemaMutationError::EmptyIndexKey),
        );
    }

    #[test]
    fn expression_index_request_lowering_fails_closed_for_unsupported_indexes() {
        let unique = PersistedIndexSnapshot::new(
            1,
            "unique_lower_name".to_string(),
            "test::mutation::unique_lower_name".to_string(),
            true,
            expression_name_index().key().clone(),
            None,
        );
        let field_path_only = non_unique_name_index();
        let items_without_expression = PersistedIndexSnapshot::new(
            2,
            "items_name".to_string(),
            "test::mutation::items_name".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
                name_key_path(),
            )]),
            None,
        );
        let empty = PersistedIndexSnapshot::new(
            3,
            "empty_expression".to_string(),
            "test::mutation::empty_expression".to_string(),
            false,
            PersistedIndexKeySnapshot::Items(Vec::new()),
            None,
        );

        assert_eq!(
            SchemaMutationRequest::from_accepted_expression_index(&unique),
            Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_expression_index(&field_path_only),
            Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_expression_index(&items_without_expression),
            Err(AcceptedSchemaMutationError::ExpressionIndexRequiresExpressionKey),
        );
        assert_eq!(
            SchemaMutationRequest::from_accepted_expression_index(&empty),
            Err(AcceptedSchemaMutationError::EmptyIndexKey),
        );
    }

    #[test]
    fn secondary_index_drop_request_lowering_fails_closed_for_unique_indexes() {
        let unique = PersistedIndexSnapshot::new(
            1,
            "unique_name".to_string(),
            "test::mutation::unique_name".to_string(),
            true,
            PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
            None,
        );

        assert_eq!(
            SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(&unique),
            Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
        );
    }

    #[test]
    fn rebuild_plan_keeps_unsupported_and_full_rewrite_shapes_explicit() {
        let nullability = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            nullability.rebuild_plan().actions(),
            &[SchemaRebuildAction::Unsupported {
                reason: "alter nullability requires data proof or rewrite",
            }],
        );
        assert_eq!(
            incompatible.rebuild_plan().actions(),
            &[SchemaRebuildAction::RewriteAllRows],
        );
    }

    #[test]
    fn unsupported_mutation_plans_fail_closed() {
        let alteration = SchemaMutationRequest::AlterNullability {
            field_id: FieldId::new(2),
        }
        .lower_to_plan();
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            alteration.compatibility(),
            MutationCompatibility::UnsupportedPreOne
        );
        assert_eq!(
            alteration.rebuild_requirement(),
            RebuildRequirement::Unsupported
        );
        assert_eq!(
            alteration.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::UnsupportedPreOne,
            )),
        );
        assert_eq!(
            incompatible.compatibility(),
            MutationCompatibility::Incompatible
        );
        assert_eq!(
            incompatible.rebuild_requirement(),
            RebuildRequirement::FullDataRewriteRequired
        );
    }

    #[test]
    fn publication_gate_allows_only_metadata_safe_no_rebuild_plans() {
        let field = nullable_text_field("nickname", 3, 2);
        let append_only = MutationPlan::append_only_fields(&[field]);
        let metadata_safe_but_rebuild_required = MutationPlan {
            mutations: Vec::new(),
            compatibility: MutationCompatibility::MetadataOnlySafe,
            rebuild: RebuildRequirement::IndexRebuildRequired,
        };
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            append_only.publication_status(),
            MutationPublicationStatus::Publishable
        );
        assert_eq!(
            metadata_safe_but_rebuild_required.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::RebuildRequired(
                RebuildRequirement::IndexRebuildRequired,
            )),
        );
        assert_eq!(
            incompatible.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::Incompatible,
            )),
        );
    }

    #[test]
    fn publication_preflight_requires_runner_readiness_before_physical_work() {
        let field = nullable_text_field("nickname", 3, 2);
        let append_only = MutationPlan::append_only_fields(&[field]);
        let field_path = SchemaMutationRequest::from_accepted_non_unique_field_path_index(
            &non_unique_name_index(),
        )
        .expect("non-unique field-path index should lower")
        .lower_to_plan();
        let no_runner = super::SchemaMutationRunnerContract::new(&[]);
        let index_runner = super::SchemaMutationRunnerContract::new(&[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]);
        let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

        assert_eq!(
            append_only.publication_preflight(&no_runner),
            super::MutationPublicationPreflight::PublishableNow,
        );
        assert_eq!(
            field_path.publication_preflight(&no_runner),
            super::MutationPublicationPreflight::MissingRunnerCapabilities {
                missing: vec![
                    super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                    super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                    super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
                ],
            },
        );
        assert_eq!(
            field_path.publication_preflight(&index_runner),
            super::MutationPublicationPreflight::PhysicalWorkReady {
                step_count: 3,
                required: vec![
                    super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                    super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                    super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
                ],
            },
        );
        assert_eq!(
            incompatible.publication_preflight(&index_runner),
            super::MutationPublicationPreflight::Rejected {
                requirement: RebuildRequirement::FullDataRewriteRequired,
            },
        );
    }

    fn base_snapshot() -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::MutationEntity".to_string(),
            "MutationEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        )
    }

    fn append_fields_snapshot(
        snapshot: &PersistedSchemaSnapshot,
        fields: &[PersistedFieldSnapshot],
    ) -> PersistedSchemaSnapshot {
        let mut next_fields = snapshot.fields().to_vec();
        next_fields.extend_from_slice(fields);

        let mut next_layout_entries = snapshot.row_layout().field_to_slot().to_vec();
        next_layout_entries.extend(fields.iter().map(|field| (field.id(), field.slot())));

        PersistedSchemaSnapshot::new(
            SchemaVersion::new(snapshot.version().get() + 1),
            snapshot.entity_path().to_string(),
            snapshot.entity_name().to_string(),
            snapshot.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::new(snapshot.row_layout().version().get() + 1),
                next_layout_entries,
            ),
            next_fields,
        )
    }

    fn snapshot_with_indexes(
        snapshot: &PersistedSchemaSnapshot,
        indexes: Vec<PersistedIndexSnapshot>,
    ) -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::new(snapshot.version().get() + 1),
            snapshot.entity_path().to_string(),
            snapshot.entity_name().to_string(),
            snapshot.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::new(snapshot.row_layout().version().get() + 1),
                snapshot.row_layout().field_to_slot().to_vec(),
            ),
            snapshot.fields().to_vec(),
            indexes,
        )
    }

    #[test]
    fn snapshot_delta_classifier_names_append_only_fields() {
        let stored = base_snapshot();
        let mut fields = stored.fields().to_vec();
        fields.push(nullable_text_field("nickname", 3, 2));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            fields,
        );

        let SchemaMutationDelta::AppendOnlyFields(added_fields) =
            classify_schema_mutation_delta(&stored, &generated)
        else {
            panic!("append-only snapshot change should classify as appended fields");
        };

        assert_eq!(added_fields.len(), 1);
        assert_eq!(added_fields[0].name(), "nickname");
    }

    #[test]
    fn snapshot_delta_request_lowers_append_only_fields_to_mutation_plan() {
        let stored = base_snapshot();
        let mut fields = stored.fields().to_vec();
        fields.push(nullable_text_field("nickname", 3, 2));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            fields,
        );

        let SchemaMutationRequest::AppendOnlyFields(added_fields) =
            schema_mutation_request_for_snapshots(&stored, &generated)
        else {
            panic!("append-only snapshot change should lower into append-only request");
        };

        let plan = SchemaMutationRequest::AppendOnlyFields(added_fields).lower_to_plan();
        assert_eq!(plan.added_field_count(), 1);
        assert_eq!(
            plan.publication_status(),
            MutationPublicationStatus::Publishable
        );
    }

    #[test]
    fn snapshot_delta_classifier_rejects_non_prefix_field_changes() {
        let stored = base_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields[1] = nullable_text_field("renamed", 2, 1);
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            stored.row_layout().clone(),
            generated_fields,
        );

        assert_eq!(
            classify_schema_mutation_delta(&stored, &generated),
            SchemaMutationDelta::Incompatible
        );
    }
}
