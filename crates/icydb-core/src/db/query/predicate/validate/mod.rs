mod model;
mod rules;
mod schema;
#[cfg(test)]
mod tests;

pub(crate) use model::literal_matches_type;
pub use rules::{reject_unsupported_query_features, validate, validate_model};
pub use schema::{SchemaInfo, ValidateError};
