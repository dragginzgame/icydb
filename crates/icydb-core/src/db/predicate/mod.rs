mod coercion;
mod fingerprint;
mod model;
mod normalize;
mod resolved;
mod row_policy;
mod runtime;
mod schema;
mod semantics;

pub use coercion::CoercionId;
pub use model::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use row_policy::MissingRowPolicy;
pub use schema::ValidateError;

pub(crate) use coercion::CoercionSpec;
pub(in crate::db) use coercion::supports_coercion;
pub(crate) use model::PredicateExecutionModel;
pub(in crate::db) use normalize::{normalize, normalize_enum_literals};
#[cfg(test)]
pub(crate) use schema::FieldType;
#[cfg(test)]
pub(crate) use schema::ScalarType;
#[allow(unused_imports)]
pub(crate) use schema::{
    SchemaInfo, literal_matches_type, reject_unsupported_query_features, validate,
};

pub(in crate::db) use fingerprint::hash_predicate;
pub(in crate::db) use resolved::{ResolvedComparePredicate, ResolvedPredicate};
pub(in crate::db) use runtime::PredicateProgram;
pub(in crate::db) use semantics::{
    TextOp, canonical_cmp, compare_eq, compare_order, compare_text, strict_value_order,
};
