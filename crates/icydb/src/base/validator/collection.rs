//! Module: base::validator::collection
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{
    design::prelude::*,
    visitor::{Validator, VisitorContext},
};

///
/// InArray
///
/// Validates that an input value appears in a fixed allow-list.
/// This is useful for small enum-like domains represented as raw values.
///

#[validator]
pub struct InArray<T> {
    values: Vec<T>,
}

impl<T> InArray<T> {
    /// Builds an allow-list validator from the provided set of accepted values.
    #[must_use]
    pub const fn new(values: Vec<T>) -> Self {
        Self { values }
    }
}

impl<T> Validator<T> for InArray<T>
where
    T: PartialEq,
{
    fn validate(&self, n: &T, ctx: &mut dyn VisitorContext) {
        if !self.values.contains(n) {
            ctx.issue(format!(
                "value must be one of {} allowed values",
                self.values.len()
            ));
        }
    }
}
