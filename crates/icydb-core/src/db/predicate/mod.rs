//! Module: predicate
//! Responsibility: predicate AST, normalization, validation, and runtime semantics.
//! Does not own: query routing, index key encoding, or executor commit behavior.
//! Boundary: query/executor/index consume this as predicate authority.

mod coercion;
mod encoding;
mod fingerprint;
mod identifiers;
mod model;
mod normalize;
mod resolved;
mod row_policy;
mod runtime;
mod semantics;
mod simplify;

pub use coercion::CoercionId;
pub use model::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use row_policy::MissingRowPolicy;

pub(crate) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(in crate::db) use identifiers::rewrite_field_identifiers;
pub(crate) use model::PredicateExecutionModel;
pub(in crate::db) use normalize::{normalize, normalize_enum_literals};

pub(in crate::db) use fingerprint::hash_predicate;
pub(in crate::db) use resolved::{ResolvedComparePredicate, ResolvedPredicate};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::{
    TextOp, canonical_cmp, compare_eq, compare_order, compare_text,
    evaluate_grouped_having_compare_v1, grouped_having_compare_op_supported,
};
