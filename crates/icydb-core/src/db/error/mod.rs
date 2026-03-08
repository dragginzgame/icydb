//! Module: db::error
//! Responsibility: shared db-local error construction helpers.
//! Does not own: core error taxonomy definitions.
//! Boundary: convenience wrappers over `crate::error::InternalError` for db modules.

mod invariant;

pub(in crate::db) use invariant::{cursor_invariant, executor_invariant, planner_invariant};
