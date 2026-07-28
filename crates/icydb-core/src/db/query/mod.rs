//! Module: db::query
//! Owns the engine-neutral semantic query boundary: intent construction,
//! planning, explain artifacts, dynamic contracts, and stable query-facing
//! helpers. SQL lowering is a sibling frontend that consumes this boundary.

pub(in crate::db) mod admission;
pub(in crate::db) mod builder;
#[cfg(feature = "query")]
mod dynamic;
pub(in crate::db) mod explain;
pub(in crate::db) mod expr;
mod fingerprint;
pub(in crate::db) mod intent;
pub(crate) mod plan;
pub(in crate::db) mod predicate;
pub(in crate::db) mod read_intent;
pub(in crate::db) mod trace;

#[cfg(feature = "query")]
pub use dynamic::{DynamicQuery, DynamicQueryResult};
#[cfg(feature = "sql")]
pub(in crate::db) use fingerprint::resumable_update_scope_fingerprint;
