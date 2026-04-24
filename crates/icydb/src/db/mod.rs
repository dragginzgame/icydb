pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// re-exports
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::QueryExecutionAttribution;
pub use icydb_core::db::Row;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::SqlQueryExecutionAttribution;
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, EntityAuthority, PersistedRow,
    Predicate, SlotReader, SlotWriter,
};
pub use icydb_core::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription, QueryTracePlan,
    StorageReport, TraceExecutionFamily, TraceReuseArtifactClass, TraceReuseEvent,
};
pub use icydb_core::db::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use icydb_core::db::{
    RowCheckMetrics, StructuralReadMetrics, with_row_check_metrics, with_structural_read_metrics,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[doc(hidden)]
pub use icydb_core::error::InternalError;
pub use response::{MutationResult, PagedResponse, ProjectionResponse, Response};
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;
pub use session::{
    DbSession, FluentLoadQuery, MutationMode, PagedLoadQuery, SessionDeleteQuery, UpdatePatch,
};
