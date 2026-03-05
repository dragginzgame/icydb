//! Module: predicate::schema
//! Responsibility: schema-aware predicate validation and coercion legality checks.
//! Does not own: runtime predicate execution or index planning strategy.
//! Boundary: validation boundary between user predicates and executable plans.

mod errors;
mod model_checks;
mod types;
mod validate;

pub use errors::ValidateError;

pub(crate) use model_checks::SchemaInfo;
pub(crate) use types::literal_matches_type;
pub(crate) use validate::{reject_unsupported_query_features, validate};
