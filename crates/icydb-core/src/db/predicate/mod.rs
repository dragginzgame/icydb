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

pub use coercion::CoercionId;
pub use model::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use row_policy::MissingRowPolicy;

pub(crate) use crate::db::reduced_sql::SqlParseError;
pub(in crate::db) use capability::{
    IndexPredicateCapability, PredicateCapabilityContext, PredicateCapabilityProfile,
    ScalarPredicateCapability, classify_index_compare_component, classify_predicate_capabilities,
};
pub(crate) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(crate) use model::PredicateExecutionModel;
pub(in crate::db) use normalize::{normalize, normalize_enum_literals};
#[cfg(feature = "sql")]
pub(in crate::db) use parser::parse_predicate_from_cursor;
pub(crate) use parser::parse_sql_predicate;

pub(in crate::db) use fingerprint::hash_predicate;
pub(in crate::db) use resolved::{ExecutableComparePredicate, ExecutablePredicate};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::{
    TextOp, canonical_cmp, compare_eq, compare_order, compare_text,
    evaluate_grouped_having_compare_v1, grouped_having_compare_op_supported,
};
