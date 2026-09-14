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
mod parser;
#[cfg(any(test, feature = "sql", feature = "migration"))]
mod render;
mod resolved;
#[cfg(any(test, feature = "sql", feature = "migration"))]
mod rewrite;
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
pub(in crate::db) use normalize::normalize_enum_literals;
pub(in crate::db) use parser::parse_sql_predicate;
#[cfg(any(test, feature = "sql", feature = "migration"))]
pub(in crate::db) use render::relabel_sql_predicate_field_root;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use render::sql_predicate_references_field_root;
#[cfg(any(test, feature = "sql", feature = "migration"))]
pub(in crate::db) use rewrite::rewrite_field_identifiers;

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

/// Parse and normalize one accepted persisted index predicate.
///
/// Accepted snapshots are runtime authority. Malformed persisted SQL is
/// corruption, never a valid predicate that silently excludes indexed rows.
pub(in crate::db) fn normalized_accepted_index_predicate(
    predicate_sql: Option<&str>,
) -> Result<Option<Predicate>, crate::error::InternalError> {
    predicate_sql
        .map(|sql| {
            parse_sql_predicate(sql)
                .map(normalize)
                .map_err(|_| crate::error::InternalError::store_corruption())
        })
        .transpose()
}

/// Return the literal prefix from the supported single-trailing-wildcard
/// `LIKE` pattern shape.
#[must_use]
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

#[cfg(test)]
mod tests {
    use crate::value::Value;

    use super::*;

    #[test]
    fn accepted_index_predicates_preserve_absence_and_normalized_semantics() {
        assert_eq!(normalized_accepted_index_predicate(None).unwrap(), None);
        assert_eq!(
            normalized_accepted_index_predicate(Some("active = true")).unwrap(),
            Some(Predicate::Compare(ComparePredicate::with_coercion(
                "active",
                CompareOp::Eq,
                Value::Bool(true),
                CoercionId::Strict,
            ))),
        );
        assert_eq!(
            normalized_accepted_index_predicate(Some("active = true AND active = false")).unwrap(),
            Some(Predicate::False),
        );
    }

    #[test]
    fn malformed_accepted_index_predicates_return_payload_free_corruption() {
        for sql in ["", "active =", "name = 'unterminated"] {
            let error = normalized_accepted_index_predicate(Some(sql)).unwrap_err();
            assert_eq!(
                error.diagnostic().code(),
                icydb_diagnostic_code::DiagnosticCode::StoreCorruption,
            );
            assert_eq!(error.diagnostic().detail(), None);
        }
    }
}
