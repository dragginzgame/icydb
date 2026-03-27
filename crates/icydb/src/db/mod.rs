pub mod query;
pub mod response;
mod session;
#[cfg(feature = "sql")]
pub mod sql;

// re-exports
pub use icydb_core::db::Row;
#[doc(hidden)]
pub use icydb_core::db::{
    EntityAuthority, InternalError, PersistedRow, PersistedScalar, ScalarSlotValueRef,
    ScalarValueRef, SlotReader, SlotWriter, decode_persisted_non_null_slot_payload,
    decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload,
    encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload,
};
pub use icydb_core::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription, QueryTracePlan,
    StorageReport, TraceExecutionStrategy,
};
#[cfg(feature = "sql")]
pub use icydb_core::db::{SqlStatementRoute, identifiers_tail_match};
pub use response::{
    PagedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
};
#[cfg(feature = "sql")]
pub use session::SqlParsedStatement;
pub use session::{
    DbSession, FluentLoadQuery, MutationMode, PagedLoadQuery, SessionDeleteQuery, UpdatePatch,
};
