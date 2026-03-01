//! Module: contracts
//! Responsibility: shared db-level semantic contracts used across subsystems.
//! Does not own: predicate runtime/validation semantics (moved to `db::predicate`).
//! Boundary: retains only non-predicate helpers plus test-only compatibility bridges.

mod semantics;
#[cfg(test)]
mod tests;

// Test-only compatibility re-exports for legacy contracts test imports.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::db::predicate::MissingRowPolicy;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::db::predicate::{
    CoercionId, CompareOp, ComparePredicate, Predicate, ScalarType, UnsupportedQueryFeature,
    ValidateError,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::db::predicate::{CoercionSpec, FieldType, SchemaInfo, literal_matches_type};
#[cfg(test)]
#[allow(unused_imports)]
pub(in crate::db) use crate::db::predicate::{
    ResolvedComparePredicate, ResolvedPredicate, TextOp, canonical_cmp, compare_eq, compare_order,
    compare_text, supports_coercion,
};
pub(in crate::db) use semantics::canonical_value_compare;
