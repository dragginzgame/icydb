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
#[cfg(any(test, feature = "sql"))]
mod render;
mod resolved;
#[cfg(any(test, feature = "sql"))]
mod rewrite;
mod row_policy;
mod runtime;
mod semantics;
mod simplify;
#[cfg(test)]
mod tests;

use crate::{
    db::{query::predicate::validate_predicate, schema::SchemaInfo},
    model::field::FieldModel,
};

pub use coercion::CoercionId;
pub use model::{CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate};
pub use row_policy::MissingRowPolicy;

pub(in crate::db) use capability::{
    IndexCompileTarget, IndexPredicateCapability, PredicateCapabilityContext,
    PredicateCapabilityProfile, ScalarPredicateCapability, classify_index_compare_component,
    classify_index_compare_target, classify_predicate_capabilities,
    classify_predicate_capabilities_for_targets, lower_index_compare_literal_for_target,
    lower_index_starts_with_prefix_for_target,
};
pub(in crate::db) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(in crate::db) use normalize::{normalize, normalize_enum_literals};
pub(in crate::db) use parser::parse_sql_predicate;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use render::{
    relabel_sql_predicate_field_root, sql_predicate_references_field_root,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use rewrite::rewrite_field_identifiers;

#[cfg(test)]
pub(in crate::db) use fingerprint::predicate_fingerprint;
pub(in crate::db) use fingerprint::{hash_predicate, predicate_fingerprint_normalized};
pub(in crate::db) use membership::{
    MembershipCompareLeaf, canonical_membership_value_list, collapse_membership_compare_leaves,
};
pub(in crate::db) use resolved::{
    ExecutableCompareOperand, ExecutableComparePredicate, ExecutablePredicate,
};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::{TextOp, canonical_cmp, compare_eq, compare_order, compare_text};
pub(in crate::db::predicate) use semantics::{
    casefold_text, eval_equality_compare_result, eval_list_membership_compare_result,
    eval_ordered_compare_result,
};

/// Parse one generated filtered-index predicate at macro/build time.
#[doc(hidden)]
pub fn parse_generated_index_predicate_sql(predicate_sql: &str) -> Result<Predicate, String> {
    parse_sql_predicate(predicate_sql).map_err(|_| "invalid generated index predicate".to_owned())
}

/// Validate one generated filtered-index predicate against trusted field metadata.
#[doc(hidden)]
pub fn validate_generated_index_predicate_fields(
    fields: &[FieldModel],
    predicate: &Predicate,
) -> Result<(), String> {
    let schema = SchemaInfo::from_field_models(fields);
    validate_predicate(&schema, predicate).map_err(|error| error.to_string())
}

/// Validate one generated check proposal against the V1 check subset.
///
/// This is a macro-expansion helper. Accepted reconciliation independently
/// binds the proposal to stable field IDs and validates its complete contract.
#[doc(hidden)]
pub fn validate_generated_check_predicate_fields(
    fields: &[FieldModel],
    predicate: &Predicate,
) -> Result<(), String> {
    let validation_fields = fields
        .iter()
        .map(|field| {
            let kind = if matches!(field.kind(), crate::model::field::FieldKind::Enum { .. }) {
                crate::model::field::FieldKind::Text { max_len: None }
            } else {
                field.kind()
            };
            FieldModel::generated_with_storage_decode_and_nullability(
                field.name(),
                kind,
                crate::model::field::FieldStorageDecode::ByKind,
                field.nullable(),
            )
        })
        .collect::<Vec<_>>();
    validate_generated_index_predicate_fields(validation_fields.as_slice(), predicate)?;
    validate_generated_check_predicate_shape(fields, predicate)
}

fn validate_generated_check_predicate_shape(
    fields: &[FieldModel],
    predicate: &Predicate,
) -> Result<(), String> {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. } => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                validate_generated_check_predicate_shape(fields, child)?;
            }
            Ok(())
        }
        Predicate::Not(inner) => validate_generated_check_predicate_shape(fields, inner),
        Predicate::Compare(compare) => {
            let enum_field = fields
                .iter()
                .find(|field| field.name() == compare.field())
                .is_some_and(|field| {
                    matches!(field.kind(), crate::model::field::FieldKind::Enum { .. })
                });
            match compare.op() {
                model::CompareOp::Eq | model::CompareOp::Ne => Ok(()),
                model::CompareOp::Lt
                | model::CompareOp::Lte
                | model::CompareOp::Gt
                | model::CompareOp::Gte
                    if !enum_field =>
                {
                    Ok(())
                }
                model::CompareOp::In | model::CompareOp::NotIn if enum_field => Ok(()),
                model::CompareOp::Lt
                | model::CompareOp::Lte
                | model::CompareOp::Gt
                | model::CompareOp::Gte
                | model::CompareOp::In
                | model::CompareOp::NotIn => Err(
                    "generated check enum fields support only equality or bounded membership"
                        .to_string(),
                ),
                model::CompareOp::Contains
                | model::CompareOp::StartsWith
                | model::CompareOp::EndsWith => {
                    Err("generated check uses an operator outside CheckExprV1".to_string())
                }
            }
        }
        Predicate::CompareFields(_) | Predicate::IsNull { .. } | Predicate::IsNotNull { .. } => {
            Ok(())
        }
        Predicate::IsMissing { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => {
            Err("generated check uses an operation outside CheckExprV1".to_string())
        }
    }
}
