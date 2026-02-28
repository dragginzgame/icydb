//! Query subsystem (Tier-2 boundary within `db`).
//!
//! This module defines the *semantic query contract* for IcyDB:
//! - Query intent construction
//! - Planning and ordering semantics
//! - Session-level query wrappers
//!
//! Although it lives under `db/`, `query` acts as a **Tier-2 boundary**
//! within the database subsystem. Its public types (re-exported at
//! `db` root) form part of the stable query surface.
//!
//! Deep modules (e.g. `plan`, `intent`) are crate-visible
//! for internal use, but external crates must only rely on types
//! intentionally re-exported at the `db` boundary.

pub(crate) mod builder;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod fingerprint;
pub(crate) mod fluent;
pub(crate) mod intent;
pub(crate) mod plan;
