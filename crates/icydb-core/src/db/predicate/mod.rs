mod consistency;
mod fingerprint;
mod model;
mod normalize;
mod resolved;
mod runtime;
mod schema;
mod semantics;

pub use consistency::MissingRowPolicy;
pub use model::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use schema::ValidateError;
pub use semantics::CoercionId;

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
pub(crate) use semantics::CoercionSpec;

pub(in crate::db) use fingerprint::hash_predicate;
pub(in crate::db) use resolved::{ResolvedComparePredicate, ResolvedPredicate};
pub(in crate::db) use runtime::PredicateProgram;
#[cfg(test)]
pub(in crate::db) use semantics::supports_coercion;
pub(in crate::db) use semantics::{
    TextOp, canonical_cmp, compare_eq, compare_order, compare_text, strict_value_order,
};
