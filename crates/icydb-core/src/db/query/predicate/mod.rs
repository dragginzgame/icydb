pub mod ast;
pub mod coercion;
pub mod eval;
pub mod normalize;
pub mod validate;

#[cfg(test)]
mod tests;

pub use ast::{CompareOp, ComparePredicate, Predicate};
pub use coercion::{CoercionId, CoercionSpec};
pub use eval::{FieldPresence, Row, eval};
pub use normalize::normalize;
pub use validate::{SchemaInfo, ValidateError, validate, validate_model};
