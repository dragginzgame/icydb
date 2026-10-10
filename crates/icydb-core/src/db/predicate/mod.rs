//! Module: predicate
//! Responsibility: predicate AST, normalization, validation, and runtime semantics.
//! Does not own: query routing, index key encoding, or executor commit behavior.
//! Boundary: query/executor/index consume this as predicate authority.

mod capability;
mod coercion;
mod encoding;
mod fingerprint;
mod membership;
mod model;
mod normalize;
#[cfg(any(test, feature = "sql"))]
mod parser;
mod render;
mod resolved;
mod row_policy;
mod runtime;
mod semantics;
mod simplify;
pub use coercion::CoercionId;
pub use model::{CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate};
pub use row_policy::MissingRowPolicy;

pub(in crate::db) use capability::{
    IndexCompileTarget, IndexCompileTargetKind, IndexPredicateCapability,
    PredicateCapabilityContext, PredicateCapabilityProfile, ScalarPredicateCapability,
    admit_index_compare_literal_for_kind, classify_index_compare_component,
    classify_index_compare_target, classify_predicate_capabilities,
    classify_predicate_capabilities_for_targets, lower_index_compare_literal_for_kind,
    lower_index_starts_with_prefix_for_target,
};
pub(in crate::db) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(in crate::db) use normalize::normalize;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use normalize::normalize_accepted_compare_fields_coercion;
pub(in crate::db) use normalize::normalize_enum_literals;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use parser::parse_sql_predicate;
pub(in crate::db) use render::render_sql_predicate;

pub(in crate::db) use fingerprint::hash_predicate;
pub(in crate::db) use fingerprint::predicate_fingerprint_normalized;
pub(in crate::db) use membership::canonical_membership_value_list;
pub(in crate::db) use membership::{MembershipCompareLeaf, collapse_membership_compare_leaves};
pub(in crate::db) use resolved::{
    ExecutableCompareOperand, ExecutableComparePredicate, ExecutablePredicate,
};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::canonical_cmp;
pub(in crate::db) use semantics::{TextOp, compare_eq, compare_order, compare_text};
pub(in crate::db::predicate) use semantics::{
    eval_equality_compare_result, eval_list_membership_compare_result, eval_ordered_compare_result,
};

/// Return the literal prefix from the supported single-trailing-wildcard
/// `LIKE` pattern shape.
#[must_use]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn supported_like_prefix(pattern: &str) -> Option<&str> {
    if !pattern.ends_with('%') {
        return None;
    }

    let prefix = &pattern[..pattern.len() - 1];
    if prefix.contains('%') || prefix.contains('_') {
        return None;
    }

    Some(prefix)
}
