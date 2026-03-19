//! Module: query::intent
//! Responsibility: query intent construction, coercion, and semantic-plan compilation.
//! Does not own: executor runtime behavior or index storage details.
//! Boundary: typed/fluent query inputs lowered into validated logical plans.

mod errors;
mod key_access;
mod model;
mod mutation;
mod order;
mod planning;
mod policy;
mod query;
mod state;

pub use errors::{IntentError, QueryError, QueryExecutionError};
pub(crate) use key_access::{
    KeyAccess, KeyAccessKind, KeyAccessState, build_access_plan_from_keys,
};
#[expect(unreachable_pub)]
pub use query::PlannedQuery;
pub use query::{CompiledQuery, Query};
pub(in crate::db::query::intent) use state::QueryIntent;
