//! Module: query::intent
//! Responsibility: query intent construction, coercion, and semantic-plan compilation.
//! Does not own: executor runtime behavior or index storage details.
//! Boundary: typed/fluent query inputs lowered into validated logical plans.

mod errors;
mod key_access;
mod model;
mod mutation;
mod planning;
mod policy;
mod query;
mod state;
#[cfg(test)]
mod tests;

pub use errors::{IntentError, QueryError, QueryExecutionError};
pub(crate) use key_access::{
    KeyAccess, KeyAccessKind, KeyAccessState, build_access_plan_from_keys,
};
pub use query::PlannedQuery;
#[cfg(feature = "sql")]
pub(in crate::db) use query::StructuralQuery;
pub use query::{CompiledQuery, Query};
pub(in crate::db::query::intent) use state::QueryIntent;
