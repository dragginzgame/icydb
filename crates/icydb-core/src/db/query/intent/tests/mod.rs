//! Module: db::query::intent::tests
//! Covers query-intent builder, planning, and explain-facing invariants.
//! Does not own: shared fixtures and helper contracts for the topical suites.
//! Boundary: wires the owner `tests/` suite and imports shared support.

mod cache_key;
#[cfg(feature = "sql")]
mod explain;
#[cfg(feature = "sql")]
mod filter_expr;
#[cfg(feature = "sql")]
mod grouped;
mod scalar;
mod support;
mod verbose;
