//! Module: session::sql::attribution
//! Responsibility: SQL diagnostics attribution facade.
//! Does not own: SQL execution, cache lookup, or response shaping.
//! Boundary: keeps compile, execute, cache, projection, and query attribution owners separate.

#[cfg(feature = "diagnostics")]
mod cache;
#[cfg(feature = "diagnostics")]
mod compile;
#[cfg(feature = "diagnostics")]
mod covering;
#[cfg(feature = "diagnostics")]
mod execution;
#[cfg(feature = "diagnostics")]
mod output_blob;
#[cfg(feature = "diagnostics")]
mod phase;
#[cfg(feature = "diagnostics")]
mod query;

#[cfg(feature = "diagnostics")]
pub use cache::SqlQueryCacheAttribution;
#[cfg(feature = "diagnostics")]
pub use compile::SqlCompileAttribution;
#[cfg(feature = "diagnostics")]
pub use covering::{SqlHybridCoveringAttribution, SqlPureCoveringAttribution};
#[cfg(feature = "diagnostics")]
pub use execution::SqlExecutionAttribution;
#[cfg(feature = "diagnostics")]
pub use output_blob::SqlOutputBlobAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db) use phase::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub use query::SqlQueryExecutionAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db::session::sql) use query::SqlQueryExecutionAttributionInputs;
