//! Module: predicate
//! Responsibility: predicate AST, normalization, validation, and runtime semantics.
//! Does not own: query routing, index key encoding, or executor commit behavior.
//! Boundary: query/executor/index consume this as predicate authority.

mod capability;
mod coercion;
mod encoding;
mod fingerprint;
mod model;
mod normalize;
mod parser;
mod resolved;
mod row_policy;
mod runtime;
mod semantics;
mod simplify;

use crate::{
    db::schema::{SchemaInfo, reject_unsupported_query_features, validate},
    model::field::FieldModel,
};

pub use coercion::CoercionId;
pub use model::{
    CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature,
};
pub use row_policy::MissingRowPolicy;

pub(in crate::db) use capability::{
    IndexCompileTarget, IndexPredicateCapability, PredicateCapabilityContext,
    PredicateCapabilityProfile, ScalarPredicateCapability, classify_index_compare_component,
    classify_index_compare_target, classify_predicate_capabilities,
    classify_predicate_capabilities_for_targets, lower_index_compare_literal_for_target,
    lower_index_starts_with_prefix_for_target,
};
pub(crate) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(crate) use model::PredicateExecutionModel;
pub(in crate::db) use normalize::{normalize, normalize_enum_literals};
pub(crate) use parser::parse_sql_predicate;

#[cfg(test)]
pub(in crate::db) use fingerprint::predicate_fingerprint;
pub(in crate::db) use fingerprint::{hash_predicate, predicate_fingerprint_normalized};
pub(in crate::db) use resolved::{
    ExecutableCompareOperand, ExecutableComparePredicate, ExecutablePredicate,
};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::{
    TextOp, canonical_cmp, compare_eq, compare_order, compare_text,
    evaluate_grouped_having_compare, grouped_having_compare_op_supported,
};

/// Parse one generated filtered-index predicate at macro/build time.
#[doc(hidden)]
pub fn parse_generated_index_predicate_sql(predicate_sql: &str) -> Result<Predicate, String> {
    parse_sql_predicate(predicate_sql).map_err(|error| error.to_string())
}

/// Validate one generated filtered-index predicate against trusted field metadata.
#[doc(hidden)]
pub fn validate_generated_index_predicate_fields(
    fields: &[FieldModel],
    predicate: &Predicate,
) -> Result<(), String> {
    let schema = SchemaInfo::from_field_models(fields);
    reject_unsupported_query_features(predicate).map_err(|error| error.to_string())?;
    validate(&schema, predicate).map_err(|error| error.to_string())
}
