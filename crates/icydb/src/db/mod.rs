//! Module: db
//!
//! Responsibility: facade module surface.
//! Does not own: core runtime ownership.
//! Boundary: keeps public facade shape stable for downstream code.

mod bootstrap;
pub mod query;
#[cfg(feature = "sql")]
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// Public facade-owned response/session surfaces.
pub use bootstrap::DatabaseBootstrapError;
#[doc(hidden)]
pub use bootstrap::ensure_default_memory_manager;
pub use icydb_core::db::{
    CompareProofAndAdvanceError, DynamicQuery, ExhaustiveQueryPageOutput, GroupedQueryOutput,
    GroupedRow, LiveQueryPageOutput, MAX_MUTATION_JOB_CONTINUATION_BYTES,
    MAX_MUTATION_JOB_IDEMPOTENCY_KEY_BYTES, MAX_MUTATION_JOB_INTENT_BYTES,
    MAX_MUTATION_JOB_RECEIPT_BYTES, MAX_MUTATION_JOB_RECORD_BYTES,
    MAX_MUTATION_JOB_STEP_KEYS_SCANNED, MAX_MUTATION_JOB_STEP_ROWS_UPDATED,
    MAX_READ_SET_PROOF_BYTES, MAX_READ_SET_PROOF_STORES, MAX_RESUMABLE_JOB_CONTINUATION_BYTES,
    MAX_RESUMABLE_JOB_IDEMPOTENCY_KEY_BYTES, MAX_RESUMABLE_JOB_RECEIPT_BYTES,
    MAX_RESUMABLE_JOB_STATE_BYTES, MutationJobAdvanceReceipt, MutationJobAdvanceRequest,
    MutationJobError, MutationJobId, MutationJobIdempotencyKey, MutationJobPayloadKind,
    MutationJobPhase, MutationJobRestartReason, MutationJobState, MutationJobStatus,
    ReadSetRevisionError, ReadSetRevisionProof, ReadSetStoreIdentity, ReadSetStoreRevision,
    ResumableJobAdvance, ResumableJobAdvanceReceipt, ResumableJobAdvanceRequest,
    ResumableJobAdvanceStatus, ResumableJobError, ResumableJobId, ResumableJobIdempotencyKey,
    ResumableJobState, ResumableJobStatus, RowProjectionOutput, ScalarPageWork,
};
#[cfg(feature = "migration")]
pub use icydb_core::db::{
    SchemaMigrationCommand, SchemaMigrationEntityTransition, SchemaMigrationFinding,
    SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationReceipt,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
};
#[cfg(feature = "sql")]
pub use response::ExecutionTrace;
#[cfg(feature = "sql")]
pub use session::SqlIntegrityError;
pub use session::{
    DbSession, ExhaustiveReadError, IntegrityCheckError, OutputRow, RequestExecutionFuture,
    RequestExecutionRoot, StructuralMutation, StructuralPatch, TypedAdapterError,
    TypedBindingError, TypedEntityAdapter, TypedEntityBinding, TypedRowAdapter, TypedRowError,
    TypedWrite, TypedWriteAdapter, TypedWriteError, WriteCell, with_request_execution,
    with_request_execution_async, with_request_execution_root,
};

/// Build the compact error returned when `db!()` has no active request scope.
#[doc(hidden)]
#[must_use]
pub const fn __request_execution_scope_required() -> crate::Error {
    crate::Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::RequestExecutionScopeRequired,
        crate::ErrorOrigin::Runtime,
    )
}

/// Build the compact error returned when an explicit root conflicts with the active request.
#[doc(hidden)]
#[must_use]
pub const fn __request_execution_root_mismatch() -> crate::Error {
    crate::Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::RequestExecutionRootMismatch,
        crate::ErrorOrigin::Runtime,
    )
}
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use session::{
    SqlExecutionPerfAttribution, SqlPureCoveringPerfAttribution, SqlQueryPerfAttribution,
};
#[doc(hidden)]
pub use session::{TypedFieldBindingRequest, TypedFieldType};

// Public core DTOs intentionally carried through the facade database surface.
pub use icydb_core::db::{
    ConstraintValidationProgressDescription, DataStoreSnapshot, DeepIntegrityPage,
    DeepIntegrityPageStatus, DynamicMutationResult, EntityCatalogCounts, EntityCatalogDescription,
    EntityConstraintDescription, EntityFieldDescription, EntityIdentityDescription,
    EntityIndexDescription, EntityRelationCardinality, EntityRelationDescription,
    EntitySchemaDescription, IndexStoreSnapshot, IntegrityAbortReceipt, IntegrityAbortStatus,
    IntegrityAuthorityClass, IntegrityAuthorityDiagnostic, IntegrityCheckRequest,
    IntegrityCheckResult, IntegrityEntityIdentity, IntegrityFinding, IntegrityFindingClass,
    IntegrityFindingKind, IntegrityJobError, IntegrityJobId, IntegrityJobOwner,
    IntegrityJobReceipt, IntegrityPendingTerminal, IntegrityPhase, IntegrityResourceDiagnostic,
    IntegritySeverity, IntegritySubmissionKey, IntegrityTerminalOutcome, IntegrityVerifierFamily,
    MemoryCatalogDescription, QuickIntegrityResult, QuickIntegrityStatus, SchemaApplicationStore,
    SchemaApplicationTarget, SchemaChangeJob, SchemaChangeJobId, SchemaChangeOutcome,
    SchemaChangeProgress, SchemaChangeProgressStatus, SchemaChangeReceipt,
    SchemaChangeValidationPhase, SchemaStoreSnapshot, SqlColumnDefault, SqlColumnExtra,
    SqlColumnKey, SqlColumnSummary, SqlDescribeOutput, SqlShowColumnsOutput,
    SqlShowRelationsOutput, StorageReport, StoreCatalogDescription,
};
#[cfg(feature = "sql")]
pub use icydb_core::db::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
pub use icydb_core::db::{ReadIntentKind, TraceReuseEvent};
#[cfg(feature = "diagnostics")]
pub use icydb_core::db::{
    RequestDiagnosticAccessPath, RequestDiagnosticWarning, RequestDiagnosticWarningKind,
    RequestDiagnostics, RequestQueryShapeDiagnostic,
};
pub use icydb_schema::{
    EntitySourceKey, ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldSourceKey,
    SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
};

// Hidden core wiring used by generated code and advanced diagnostics.
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, CompositePrimaryKeyValue,
    CompositePrimaryKeyValueError, Predicate, PrimaryKeyComponent, PrimaryKeyValue,
};
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;

// Diagnostics payloads stay feature-gated so normal canister builds do not
// retain observability surfaces they did not request.
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    ScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlOutputBlobAttribution,
    SqlPureCoveringAttribution, SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
    SqlStructuralWorkAttribution,
};
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
