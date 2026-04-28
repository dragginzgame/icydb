pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// Public facade-owned response/session surfaces.
pub use response::{
    ExecutionTrace, GroupedRow, MutationResult, PagedResponse, ProjectedRow, ProjectionResponse,
    Response,
};
pub use session::{
    DbSession, FluentLoadQuery, MutationMode, PagedLoadQuery, SessionDeleteQuery, StructuralPatch,
};

// Public core DTOs intentionally carried through the facade database surface.
pub use icydb_core::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
    QueryTracePlan, Row, StorageReport, TraceExecutionFamily, TraceReuseArtifactClass,
    TraceReuseEvent,
};

// Hidden core wiring used by generated code and advanced diagnostics.
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, EntityAuthority, PersistedRow,
    Predicate, SlotReader, SlotWriter,
};
#[doc(hidden)]
pub use icydb_core::error::InternalError;
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;

// Diagnostics payloads stay feature-gated so normal canister builds do not
// retain observability surfaces they did not request.
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    QueryExecutionAttribution,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::{
    RowCheckMetrics, StructuralReadMetrics, with_row_check_metrics, with_structural_read_metrics,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
