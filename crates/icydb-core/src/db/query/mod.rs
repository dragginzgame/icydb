//! Module: db::query
//! Owns the semantic query boundary: intent construction, planning, explain,
//! fluent APIs, and stable query-facing helpers.

pub(in crate::db) mod api;
pub(in crate::db) mod builder;
pub(in crate::db) mod explain;
pub(in crate::db) mod expr;
mod fingerprint;
pub(in crate::db) mod fluent;
pub(in crate::db) mod intent;
pub(crate) mod plan;
pub(in crate::db) mod predicate;
pub(in crate::db) mod trace;
