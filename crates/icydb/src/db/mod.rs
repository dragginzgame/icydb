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
mod startup;

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
    MutationJobTargetFailureReason, ProgressJobFamily, ProgressJobInventory,
    ProgressJobInventoryRecord, ProgressJobLifecycle, ReadSetRevisionError, ReadSetRevisionProof,
    ReadSetStoreIdentity, ReadSetStoreRevision, ResumableJobAdvance, ResumableJobAdvanceReceipt,
    ResumableJobAdvanceRequest, ResumableJobAdvanceStatus, ResumableJobError, ResumableJobId,
    ResumableJobIdempotencyKey, ResumableJobState, ResumableJobStatus, RowProjectionOutput,
    ScalarPageWork,
};
#[cfg(feature = "migration")]
pub use icydb_core::db::{
    SchemaMigrationCommand, SchemaMigrationEntityTransition, SchemaMigrationFinding,
    SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationReceipt,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
};
#[cfg(feature = "sql")]
pub use session::SqlIntegrityError;
pub use session::{
    BoundWriteEncoder, DbSession, ExhaustiveReadError, IntegrityCheckError, LivePageStep,
    OutputRow, PreparedExactKeyOutput, PreparedLivePageCursor, PreparedLivePageOutput,
    PreparedOutputRows, RequestExecutionFuture, RequestExecutionRoot, StructuralMutation,
    StructuralPatch, TrustedTypedWriteBatch, TypedAdapterError, TypedEntityAdapter,
    TypedEntityBinding, TypedOperationError, TypedRowAdapter, TypedWrite, TypedWriteAdapter,
    TypedWriteBatchResult, TypedWriteBatchResults, TypedWriteHandle, WriteCell,
    with_request_execution, with_request_execution_async, with_request_execution_root,
};
pub use startup::{
    __clear_generated_startup_failure, __install_startup_recovery_wakeup,
    __observe_generated_startup_state, __record_generated_schema_startup_failure,
    __startup_bootstrap_failure, __startup_recovery_pending, DatabaseStartupState,
    GeneratedStartupDriverStep, StartupFailure, StartupFailureKind,
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
#[doc(hidden)]
pub use session::{TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType};

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
pub use icydb_schema::{
    EntitySourceKey, ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldSourceKey,
    SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
};

// Hidden core wiring used by generated code and query construction.
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, CompositePrimaryKeyValue,
    CompositePrimaryKeyValueError, Predicate, PrimaryKeyComponent, PrimaryKeyValue,
};
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_shell_surface, sql_statement_surface,
};
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;
