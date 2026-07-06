//! Module: db
//!
//! Responsibility: facade module surface.
//! Does not own: core runtime ownership.
//! Boundary: keeps public facade shape stable for downstream code.

pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// Public facade-owned response/session surfaces.
pub use response::{
    ExecutionTrace, GroupedRow, MutationResult, PagedResponse, ProjectedRow, ProjectionResponse,
    ProjectionRows, Response, RowProjectionOutput,
};
pub use session::{
    DbSession, FluentLoadQuery, MutationMode, PagedLoadQuery, SessionDeleteQuery, StructuralPatch,
};
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use session::{
    SqlExecutionPerfAttribution, SqlPureCoveringPerfAttribution, SqlQueryPerfAttribution,
};

// Public core DTOs intentionally carried through the facade database surface.
pub use icydb_core::db::{
    AdminBatchRequest, DataStoreSnapshot, EntityCatalogCounts, EntityCatalogDescription,
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaCheckDescription,
    EntitySchemaDescription, ExplainAggregateTerminalPlan, ExplainExecutionDescriptor,
    ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
    ExplainExecutionOrderingSource, IndexStoreSnapshot, MemoryCatalogDescription, PageRequest,
    QueryAdmissionAccessKind, QueryAdmissionDecision, QueryAdmissionGroupedSummary,
    QueryAdmissionLane, QueryAdmissionOrdering, QueryAdmissionPlanShape, QueryAdmissionRejection,
    QueryAdmissionResidualFilter, QueryAdmissionSummary, QueryBoundKind,
    QueryMaterializationSummary, QueryTracePlan, Row, SchemaStoreSnapshot, StorageReport,
    StoreCatalogDescription, TraceExecutionFamily, TraceReuseArtifactClass, TraceReuseEvent,
};

// Hidden core wiring used by generated code and advanced diagnostics.
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, CompositePrimaryKeyValue,
    CompositePrimaryKeyValueError, EntityAuthority, Predicate, PrimaryKeyComponent,
    PrimaryKeyValue,
};
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;

// Diagnostics payloads stay feature-gated so normal canister builds do not
// retain observability surfaces they did not request.
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::{
    DirectDataRowAttribution, FluentTerminalExecutionAttribution, GroupedCountAttribution,
    GroupedExecutionAttribution, QueryExecutionAttribution, ScalarAggregateAttribution,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::{
    RowCheckMetrics, StructuralReadMetrics, with_row_check_metrics, with_structural_read_metrics,
};
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlAdminBulkDeletePlan, SqlAdminBulkUpdatePlan, SqlDeleteExposurePolicy,
    SqlDeletePolicyContext, SqlDeletePolicyRejection, SqlDeletePolicyReport,
    SqlDeleteStatementClassification, SqlPublicBoundedDeletePlan, SqlPublicBoundedUpdatePlan,
    SqlPublicPrimaryKeyDeletePlan, SqlPublicPrimaryKeyUpdatePlan, SqlSessionCurrentDeletePlan,
    SqlSessionCurrentUpdatePlan, SqlStatementShellSurface, SqlStatementSurface,
    SqlUpdateAssignmentPolicy, SqlUpdateExposurePolicy, SqlUpdatePolicyContext,
    SqlUpdatePolicyRejection, SqlUpdatePolicyReport, SqlUpdateStatementClassification,
    SqlValidatedDeletePlan, SqlValidatedUpdatePlan, SqlWriteExecutionBounds, SqlWriteOrderProof,
    SqlWriteReturningBounds, SqlWriteReturningShape, SqlWriteStatementShape, SqlWriteWhereProof,
    classify_sql_delete_policy, classify_sql_update_policy, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlOutputBlobAttribution,
    SqlPureCoveringAttribution, SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
    SqlScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
