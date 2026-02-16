mod model;
mod rules;
mod schema;
#[cfg(test)]
mod tests;

pub(crate) use model::literal_matches_type;
#[cfg(test)]
pub use rules::validate_model;
pub use rules::{reject_unsupported_query_features, validate};
pub use schema::{SchemaInfo, ValidateError};
