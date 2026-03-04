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

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::db::{
    predicate::{MissingRowPolicy, Predicate},
    query::plan::{OrderDirection, OrderSpec},
};

pub use errors::{IntentError, QueryError, QueryExecuteError};
#[expect(unused_imports)]
pub(crate) use key_access::coerce_entity_key;
pub(crate) use key_access::{
    KeyAccess, KeyAccessKind, KeyAccessState, access_plan_to_entity_keys,
    build_access_plan_from_keys,
};
#[cfg_attr(not(test), expect(unused_imports))]
pub(crate) use model::QueryModel;
#[expect(unreachable_pub)]
pub use query::PlannedQuery;
pub use query::{CompiledQuery, Query};
pub(in crate::db::query::intent) use state::QueryIntent;
