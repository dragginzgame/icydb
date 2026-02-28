//! Shared execution-level semantic helpers for `db` subsystems.
//!
//! Predicate ownership now lives under `db::predicate`.
//! This module intentionally retains only non-predicate cross-cutting helpers,
//! plus test-only compatibility re-exports for legacy test imports.

mod semantics;
#[cfg(test)]
mod tests;

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
