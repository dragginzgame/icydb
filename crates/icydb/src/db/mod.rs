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
#[cfg(feature = "sql")]
pub use icydb_core::db::{SqlStatementRoute, identifiers_tail_match};
pub use response::{
    PagedResponse, ProjectionResponse, Response, WriteBatchResponse, WriteResponse,
};
pub use session::{DbSession, FluentLoadQuery, PagedLoadQuery, SessionDeleteQuery};
#[cfg(feature = "sql")]
pub use session::{SqlParsedStatement, SqlPreparedStatement};
