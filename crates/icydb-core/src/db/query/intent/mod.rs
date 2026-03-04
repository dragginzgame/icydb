//! Module: query::intent
//! Responsibility: query intent construction, coercion, and semantic-plan compilation.
//! Does not own: executor runtime behavior or index storage details.
//! Boundary: typed/fluent query inputs lowered into validated logical plans.

mod errors;
mod key_access;
mod model;
mod order;
mod query;

#[cfg(test)]
mod tests;
#[cfg(test)]
use crate::db::{
    predicate::{MissingRowPolicy, Predicate},
    query::plan::{OrderDirection, OrderSpec},
};

pub type DeleteSpec = crate::db::query::plan::DeleteSpec;
pub type LoadSpec = crate::db::query::plan::LoadSpec;
pub type QueryMode = crate::db::query::plan::QueryMode;

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
