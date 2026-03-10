use crate::{design::prelude::*, traits::Validator, visitor::VisitorContext};

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
    T: PartialEq + std::fmt::Debug + std::fmt::Display,
{
    fn validate(&self, n: &T, ctx: &mut dyn VisitorContext) {
        if !self.values.contains(n) {
            ctx.issue(format!(
                "{n} is not in the allowed values: {:?}",
                self.values
            ));
        }
    }
}
