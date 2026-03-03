//! Module: contracts
//! Responsibility: shared db-level semantic contracts used across subsystems.
//! Does not own: predicate runtime/validation semantics (moved to `db::predicate`).
//! Boundary: retains only non-predicate helpers.

mod semantics;
#[cfg(test)]
mod tests;

pub(in crate::db) use semantics::canonical_value_compare;
