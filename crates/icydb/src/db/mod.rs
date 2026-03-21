pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// re-exports
pub use icydb_core::db::Row;
pub use icydb_core::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription, QueryTracePlan,
    StorageReport, TraceExecutionStrategy,
};
#[doc(hidden)]
pub use icydb_core::db::{
    InternalError, PersistedRow, PersistedScalar, ScalarSlotValueRef, ScalarValueRef, SlotReader,
    SlotWriter, decode_persisted_option_scalar_slot_payload, decode_persisted_scalar_slot_payload,
    decode_persisted_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload,
    missing_persisted_slot_error,
};
#[cfg(feature = "sql")]
pub use icydb_core::db::{SqlStatementRoute, identifiers_tail_match};
pub use response::{
    PagedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
};
pub use session::{DbSession, FluentLoadQuery, PagedLoadQuery, SessionDeleteQuery};
#[cfg(feature = "sql")]
pub use session::{SqlParsedStatement, SqlPreparedStatement};
