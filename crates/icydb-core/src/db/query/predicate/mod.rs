pub mod ast;
pub mod coercion;
pub(crate) mod eval;
pub mod normalize;
pub mod validate;

#[cfg(test)]
mod tests;

pub use ast::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use coercion::{CoercionId, CoercionSpec};
pub(crate) use eval::eval;
pub use normalize::normalize;
#[cfg(test)]
pub use validate::validate_model;
pub use validate::{SchemaInfo, ValidateError, validate};
