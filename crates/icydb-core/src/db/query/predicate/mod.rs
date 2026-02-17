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
pub(crate) use coercion::CoercionSpec;
pub(crate) use eval::eval;
pub(crate) use normalize::normalize;
pub(crate) use validate::SchemaInfo;
pub use validate::ValidateError;
pub(crate) use validate::validate;
