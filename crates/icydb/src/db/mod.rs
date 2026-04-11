pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// re-exports
pub use icydb_core::db::Row;
#[doc(hidden)]
pub use icydb_core::db::{
    CoercionId, CompareOp, ComparePredicate, EntityAuthority, InternalError, PersistedRow,
    PersistedScalar, Predicate, ScalarSlotValueRef, ScalarValueRef, SlotReader, SlotWriter,
    debug_remove_entity_row_data_only, decode_persisted_custom_many_slot_payload,
    decode_persisted_custom_slot_payload, decode_persisted_non_null_slot_payload,
    decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload,
    encode_persisted_custom_many_slot_payload, encode_persisted_custom_slot_payload,
    encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload,
};
pub use icydb_core::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription, QueryTracePlan,
    StorageReport, TraceExecutionStrategy,
};
pub use icydb_core::db::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
#[cfg(feature = "structural-read-metrics")]
#[doc(hidden)]
pub use icydb_core::db::{
    GroupedCountFoldMetrics, RowCheckMetrics, StructuralReadMetrics,
    with_grouped_count_fold_metrics, with_row_check_metrics, with_structural_read_metrics,
};
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub use icydb_core::db::{
    LoweredSqlDispatchExecutorAttribution, SqlProjectionTextExecutorAttribution, SqlStatementRoute,
    identifiers_tail_match,
};
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
#[doc(hidden)]
pub use icydb_core::db::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(all(feature = "sql", not(feature = "perf-attribution")))]
pub use icydb_core::db::{SqlStatementRoute, identifiers_tail_match};
pub use response::{
    PagedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
};
#[cfg(feature = "sql")]
pub use session::SqlParsedStatement;
#[cfg(feature = "sql")]
#[doc(hidden)]
pub use session::generated::execute_generated_sql_query;
#[doc(hidden)]
pub use session::generated::execute_generated_storage_report;
pub use session::{
    DbSession, FluentLoadQuery, MutationMode, PagedLoadQuery, SessionDeleteQuery, UpdatePatch,
};
