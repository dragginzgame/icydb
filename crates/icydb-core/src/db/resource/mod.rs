//! Neutral database resource ownership: bounded construction, execution/request
//! accounting and retained cache weight. Semantic frontends supply work and
//! attribution; this owner retains existing limits and typed exhaustion.

pub(in crate::db) mod bounded_cache;
pub(in crate::db) mod budget;
pub(in crate::db) mod construction;
pub(in crate::db) mod request;
