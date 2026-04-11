//! Module: db::query
//! Owns the semantic query boundary: intent construction, planning, explain,
//! fluent APIs, and stable query-facing helpers.

pub(crate) mod api;
pub(crate) mod builder;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod fingerprint;
pub(crate) mod fluent;
pub(crate) mod intent;
pub(crate) mod plan;
pub(crate) mod trace;
