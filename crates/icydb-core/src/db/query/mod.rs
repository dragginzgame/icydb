//! Query subsystem (Tier-2 boundary within `db`).
//!
//! This module defines the *semantic query contract* for IcyDB:
//! - Query intent construction
//! - Predicate expression modeling
//! - Planning and ordering semantics
//! - Session-level query wrappers
//!
//! Although it lives under `db/`, `query` acts as a **Tier-2 boundary**
//! within the database subsystem. Its public types (re-exported at
//! `db` root) form part of the stable query surface.
//!
//! Deep modules (e.g. `plan`, `predicate`, `intent`) are crate-visible
//! for internal use, but external crates must only rely on types
//! intentionally re-exported at the `db` boundary.
//!
//! Predicate semantics are defined in `docs/QUERY_PRACTICE.md` and are
//! the canonical contract for evaluation, coercion, and normalization.

pub(crate) mod builder;
pub(crate) mod cursor;
pub(crate) mod enum_filter;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod fingerprint;
pub(crate) mod intent;
pub(crate) mod plan;
pub(crate) mod policy;
pub(crate) mod predicate;
pub(crate) mod save;
pub(crate) mod session;

/// ---------------------------------------------------------------------
/// Public Contract Types
/// ---------------------------------------------------------------------

///
/// ReadConsistency
///
/// Missing-row handling policy for query execution.
///
/// This is part of the query contract and is re-exported at the `db`
/// boundary. It is stable API surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Missing rows are ignored (no error).
    MissingOk,

    /// Missing rows are treated as corruption.
    Strict,
}
