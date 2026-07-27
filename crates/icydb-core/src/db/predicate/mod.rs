//! Module: predicate
//! Responsibility: predicate AST, normalization, validation, and runtime semantics.
//! Does not own: query routing, index key encoding, or executor commit behavior.
//! Boundary: query/executor/index consume this as predicate authority.

#[cfg(any(test, feature = "sql"))]
mod capability;
mod coercion;
mod encoding;
#[cfg(any(test, feature = "sql"))]
mod fingerprint;
mod membership;
mod model;
mod normalize;
mod parser;
#[cfg(any(test, feature = "sql"))]
mod render;
mod resolved;
#[cfg(any(test, feature = "sql"))]
mod rewrite;
mod row_policy;
mod runtime;
mod semantics;
mod simplify;
pub use coercion::CoercionId;
pub use model::{CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate};
pub use row_policy::MissingRowPolicy;

#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use capability::{
    IndexCompileTarget, IndexCompileTargetKind, IndexPredicateCapability,
    PredicateCapabilityContext, PredicateCapabilityProfile, ScalarPredicateCapability,
    classify_index_compare_component, classify_index_compare_target,
    classify_predicate_capabilities, classify_predicate_capabilities_for_targets,
    lower_index_compare_literal_for_target, lower_index_starts_with_prefix_for_target,
};
pub(in crate::db) use coercion::CoercionSpec;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use coercion::supports_coercion;
pub(in crate::db) use normalize::normalize;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use normalize::normalize_enum_literals;
pub(in crate::db) use parser::parse_sql_predicate;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use render::{
    relabel_sql_predicate_field_root, sql_predicate_references_field_root,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use rewrite::rewrite_field_identifiers;

#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use fingerprint::hash_predicate;
#[cfg(feature = "sql")]
pub(in crate::db) use fingerprint::predicate_fingerprint_normalized;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use membership::canonical_membership_value_list;
pub(in crate::db) use membership::{MembershipCompareLeaf, collapse_membership_compare_leaves};
pub(in crate::db) use resolved::{
    ExecutableCompareOperand, ExecutableComparePredicate, ExecutablePredicate,
};
pub(in crate::db) use runtime::PredicateProgram;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use semantics::canonical_cmp;
pub(in crate::db) use semantics::{TextOp, compare_eq, compare_order, compare_text};
pub(in crate::db::predicate) use semantics::{
    casefold_text, eval_equality_compare_result, eval_list_membership_compare_result,
    eval_ordered_compare_result,
};
