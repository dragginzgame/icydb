pub(crate) mod eval;
pub(crate) mod normalize;
pub(crate) mod validate;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use crate::db::predicate::coercion::CoercionSpec;
#[allow(unused_imports)]
pub(crate) use crate::db::predicate::{
    CoercionId, CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature,
};
pub(crate) use eval::{PredicateFieldSlots, eval_with_slots};
pub(crate) use normalize::{normalize, normalize_enum_literals};
pub(crate) use validate::SchemaInfo;
pub use validate::ValidateError;
pub(crate) use validate::validate;
