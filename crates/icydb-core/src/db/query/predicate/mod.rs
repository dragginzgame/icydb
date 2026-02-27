pub(crate) mod normalize;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

use crate::db::contracts::{Predicate, PredicateExecutionModel};

pub(crate) use normalize::{normalize, normalize_enum_literals};
pub(crate) use validate::validate;

/// Lower query-owned predicate shape into the neutral execution model.
#[must_use]
pub(crate) const fn lower_to_execution_model(predicate: Predicate) -> PredicateExecutionModel {
    predicate
}
