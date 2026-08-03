//! Module: query::intent
//! Responsibility: query intent construction, coercion, and semantic-plan compilation.
//! Does not own: executor runtime behavior or index storage details.
//! Boundary: typed/dynamic and SQL inputs lower into validated logical plans.

mod cache_key;
mod errors;
mod model;
mod mutation;
mod policy;
mod query;
mod state;
pub(in crate::db) use cache_key::StructuralQueryCacheKey;
pub use errors::{IntentError, QueryError, QueryExecutionError};
pub(in crate::db::query) use model::QueryModel;
pub(in crate::db) use query::StructuralQuery;
pub(in crate::db::query::intent) use state::QueryIntent;
