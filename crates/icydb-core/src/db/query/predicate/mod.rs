pub(crate) mod ast;
pub(crate) mod coercion;
pub(crate) mod eval;
pub(crate) mod normalize;
pub(crate) mod validate;

#[cfg(test)]
mod tests;

pub use ast::{CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature};
pub use coercion::CoercionId;
#[cfg(test)]
pub use coercion::CoercionSpec;
pub(crate) use eval::eval;
pub use normalize::normalize;
pub(crate) use validate::validate;
pub use validate::{SchemaInfo, ValidateError};
