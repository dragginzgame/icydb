//! Module: metrics::sink::events
//! Responsibility: stable instrumentation event taxonomy for metrics sinks.
//! Does not own: global metrics state mutation or report rendering.
//! Boundary: exposes event enums consumed by metrics sinks and runtime instrumentation.

use crate::{
    db::{MutationJobPhase, MutationJobRestartReason, MutationJobTargetFailureReason},
    error::ErrorClass,
};
use std::rc::Rc;

///
/// ExecKind
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum ExecKind {
    Delete,
    Load,
    Save,
}

///
/// ExecOutcome
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum ExecOutcome {
    Aborted,
    ErrorConflict,
    ErrorCorruption,
    ErrorIncompatiblePersistedFormat,
    ErrorInternal,
    ErrorInvariantViolation,
    ErrorNotFound,
    ErrorUnsupported,
    Success,
}

///
/// CacheKind
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum CacheKind {
    SharedQueryPlan,
    SqlCompiledCommand,
}

///
/// CacheOutcome
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum CacheOutcome {
    Hit,
    Insert,
    Miss,
}

///
/// CacheMissReason
///
/// Stable cache miss reason buckets for cache identities that already have a
/// scoped entity path. These categories explain why a lookup missed without
/// exposing query text, field names, or schema hashes in the metrics report.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum CacheMissReason {
    Cold,
    DistinctKey,
    SchemaFingerprint,
    SchemaVersion,
    Surface,
    Visibility,
}

impl ExecOutcome {
    // Map the crate's typed runtime error taxonomy into stable metrics buckets.
}

///
/// SaveMutationKind
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum SaveMutationKind {
    Insert,
    Replace,
    Update,
}

///
/// MutationCommitClass
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum MutationCommitClass {
    DurableOnly,
    LiveOnly,
    MixedDurableAndLive,
}

/// Stable low-cardinality mutation-job lifecycle facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum MutationJobLifecycleEvent {
    AdvanceExactReplay,
    CancelUnadvanced,
    Complete,
    ForwardToVerify,
    InventoryLoaded,
    StartExactReplay,
    StartInserted,
    StateLoaded,
    TerminalAcknowledged,
    VerifyRestartResidualWork,
    VerifyRestartRevisionDrift,
}

///
/// SchemaReconcileOutcome
///
/// Stable startup/metadata reconciliation outcomes for the schema trust
/// boundary. The enum is intentionally low-cardinality so metrics can explain
/// schema acceptance failures without exposing field names or diagnostic text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum SchemaReconcileOutcome {
    ExactMatch,
    FirstCreate,
    LatestSnapshotCorrupt,
    RejectedFieldSlot,
    RejectedOther,
    RejectedRowLayout,
    RejectedSchemaVersion,
    StoreWriteError,
}

///
/// SchemaTransitionOutcome
///
/// Stable schema transition policy buckets. These counters isolate the policy
/// decision for an existing accepted snapshot from broader reconciliation
/// outcomes such as first-create writes, corrupt stores, or store failures.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum SchemaTransitionOutcome {
    AddExpressionIndex,
    AddFieldPathIndex,
    AppendOnlyFields,
    ConstraintActivation,
    ExactMatch,
    MetadataOnlyFieldDefault,
    MetadataOnlyIndexRename,
    RejectedEntityIdentity,
    RejectedFieldContract,
    RejectedFieldSlot,
    RejectedRowLayout,
    RejectedSchemaVersion,
    RejectedSnapshot,
}

///
/// SqlCompileRejectPhase
///
/// Stable SQL compile rejection buckets. These counters identify the broad
/// admission phase that rejected a SQL command without exposing SQL text,
/// parser diagnostics, field names, or lowered query details.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum SqlCompileRejectPhase {
    CacheKey,
    Parse,
    Semantic,
}

///
/// SqlWriteKind
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum SqlWriteKind {
    Delete,
    Insert,
    InsertSelect,
    Update,
}

///
/// PlanKind
///

#[derive(Clone, Copy, Debug)]
#[remain::sorted]
pub enum PlanKind {
    ByKey,
    ByKeys,
    FullScan,
    IndexBranchSet,
    IndexMultiLookup,
    IndexPrefix,
    IndexRange,
    Intersection,
    KeyRange,
    Union,
}

///
/// PlanChoiceReason
///
/// Stable selected-route reason buckets for non-index and primary-key access
/// choices. These counters explain why a query did not land on a secondary
/// index route without exposing predicates, literals, or index names.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum PlanChoiceReason {
    ConflictingPrimaryKeyChildrenAccessPreferred,
    ConstantFalsePredicate,
    EmptyChildAccessPreferred,
    FullScanAccess,
    IntentKeyAccessOverride,
    LimitZeroWindow,
    NonIndexAccess,
    PlannerCompositeNonIndex,
    PlannerFullScanFallback,
    PlannerKeySetAccess,
    PlannerPrimaryKeyLookup,
    PlannerPrimaryKeyRange,
    RequiredOrderPrimaryKeyRangePreferred,
    SingletonPrimaryKeyChildAccessPreferred,
}

///
/// GroupedPlanExecutionMode
///
/// Canonical grouped-plan mode carried by metrics events.
/// This keeps grouped metrics classification structured without routing
/// through string codes that the sink would immediately decode again.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[remain::sorted]
pub enum GroupedPlanExecutionMode {
    HashMaterialized,
    OrderedStreaming,
}

///
/// MetricsEvent
///

#[derive(Clone, Debug)]
#[remain::sorted]
pub enum MetricsEvent {
    AcceptedSchemaFootprint {
        entity_path: Rc<str>,
        fields: u64,
        nested_leaf_facts: u64,
    },
    Cache {
        entity_path: Rc<str>,
        kind: CacheKind,
        outcome: CacheOutcome,
    },
    CacheEntries {
        kind: CacheKind,
        entries: u64,
    },
    CacheMissReason {
        entity_path: Rc<str>,
        kind: CacheKind,
        reason: CacheMissReason,
    },
    ExecError {
        kind: ExecKind,
        entity_path: Rc<str>,
        outcome: ExecOutcome,
    },
    ExecFinish {
        kind: ExecKind,
        entity_path: Rc<str>,
        rows_touched: u64,
        inst_delta: u64,
        outcome: ExecOutcome,
    },
    ExecStart {
        kind: ExecKind,
        entity_path: Rc<str>,
    },
    IndexDelta {
        entity_path: Rc<str>,
        inserts: u64,
        removes: u64,
    },
    LoadRowEfficiency {
        entity_path: Rc<str>,
        candidate_rows_scanned: u64,
        candidate_rows_filtered: u64,
        result_rows_emitted: u64,
    },
    MutationCommitPlan {
        entity_count: u64,
        class: MutationCommitClass,
    },
    MutationJobCapacity {
        retained_count: u64,
        hard_limit: u64,
        reserved_integrity_headroom: u64,
        integrity_count: u64,
        resumable_count: u64,
        mutation_count: u64,
        retained_record_bytes: u64,
    },
    MutationJobLifecycle {
        event: MutationJobLifecycleEvent,
    },
    MutationJobRestart {
        reason: MutationJobRestartReason,
    },
    MutationJobStep {
        phase: MutationJobPhase,
        keys_scanned: u64,
        rows_updated: u64,
        scan_bytes: u64,
        staged_bytes: u64,
        keys_scanned_total: u64,
        rows_updated_total: u64,
        verify_restarts_total: u64,
    },
    MutationJobTargetFailure {
        reason: MutationJobTargetFailureReason,
    },
    NonAtomicPartialCommit {
        entity_path: Rc<str>,
        committed_rows: u64,
    },
    Plan {
        entity_path: Rc<str>,
        kind: PlanKind,
        grouped_execution_mode: Option<GroupedPlanExecutionMode>,
    },
    PlanChoice {
        entity_path: Rc<str>,
        reason: PlanChoiceReason,
    },
    PreparedShapeAlreadyFinalized {
        entity_path: Rc<str>,
    },
    RelationValidation {
        entity_path: Rc<str>,
        reverse_lookups: u64,
        blocked_deletes: u64,
    },
    ReverseIndexDelta {
        entity_path: Rc<str>,
        inserts: u64,
        removes: u64,
    },
    RowsAggregated {
        entity_path: Rc<str>,
        rows_aggregated: u64,
    },
    RowsEmitted {
        entity_path: Rc<str>,
        rows_emitted: u64,
    },
    RowsFiltered {
        entity_path: Rc<str>,
        rows_filtered: u64,
    },
    RowsScanned {
        entity_path: Rc<str>,
        rows_scanned: u64,
    },
    SaveMutation {
        entity_path: Rc<str>,
        kind: SaveMutationKind,
        rows_touched: u64,
    },
    SchemaReconcile {
        entity_path: Rc<str>,
        outcome: SchemaReconcileOutcome,
    },
    SchemaStoreFootprint {
        encoded_bytes: u64,
        entity_path: Rc<str>,
        latest_snapshot_bytes: u64,
        snapshots: u64,
    },
    SchemaTransition {
        entity_path: Rc<str>,
        outcome: SchemaTransitionOutcome,
    },
    SqlCompileReject {
        entity_path: Rc<str>,
        phase: SqlCompileRejectPhase,
    },
    SqlWrite {
        entity_path: Rc<str>,
        kind: SqlWriteKind,
        staged_rows: u64,
        matched_rows: u64,
        mutated_rows: u64,
        returning_rows: u64,
    },
    SqlWriteError {
        entity_path: Rc<str>,
        kind: SqlWriteKind,
        class: ErrorClass,
    },
    UniqueViolation {
        entity_path: Rc<str>,
    },
}
