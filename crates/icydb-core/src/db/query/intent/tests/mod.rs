//! Module: db::query::intent::tests
//! Covers query-intent builder, planning, and explain-facing invariants.
//! Does not own: shared fixtures and helper contracts for the topical suites.
//! Boundary: wires the owner `tests/` suite and imports shared support.

mod cache_key;
mod explain;
mod filter_expr;
mod grouped;
mod scalar;
mod support;
mod verbose;
