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
pub use icydb_core::db::{DynamicQuery, GroupedQueryOutput, GroupedRow, RowProjectionOutput};
#[cfg(feature = "migration")]
pub use icydb_core::db::{
    SchemaMigrationCommand, SchemaMigrationEntityTransition, SchemaMigrationFinding,
    SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationReceipt,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
};
#[cfg(feature = "sql")]
pub use icydb_core::db::{
    TrustedResumableUpdateContinuation, TrustedResumableUpdatePhase, TrustedResumableUpdateReceipt,
    TrustedResumableUpdateRestartReason,
};
#[cfg(feature = "sql")]
pub use response::ExecutionTrace;
#[cfg(feature = "sql")]
pub use session::SqlIntegrityError;
pub use session::{
    DbSession, IntegrityCheckError, OutputRow, RequestExecutionRoot, StructuralMutation,
    StructuralPatch, TypedAdapterError, TypedBindingError, TypedEntityAdapter, TypedEntityBinding,
    TypedRowAdapter, TypedRowError, TypedWrite, TypedWriteAdapter, TypedWriteError, WriteCell,
    with_request_execution, with_request_execution_root,
};

/// Build the compact error returned when `db!()` has no active request scope.
#[doc(hidden)]
#[must_use]
pub const fn __request_execution_scope_required() -> crate::Error {
    crate::Error::from_kind(
        crate::ErrorKind::Runtime(crate::RuntimeErrorKind::Internal),
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
    SchemaChangeValidationPhase, SchemaStoreSnapshot, StorageReport, StoreCatalogDescription,
};
#[cfg(feature = "sql")]
pub use icydb_core::db::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
pub use icydb_core::db::{ReadIntentKind, TraceReuseEvent};
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
