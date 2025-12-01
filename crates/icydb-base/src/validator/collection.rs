use crate::{core::traits::Validator, prelude::*};

///
/// InArray
///

#[validator]
pub struct InArray<T> {
    pub values: Vec<T>,
}

impl<T> InArray<T> {
    #[must_use]
    pub const fn new(values: Vec<T>) -> Self {
        Self { values }
    }
}

impl<T> Validator<T> for InArray<T>
where
    T: PartialEq + std::fmt::Debug + std::fmt::Display,
{
    fn validate(&self, n: &T) -> Result<(), String> {
        if self.values.contains(n) {
            Ok(())
        } else {
            Err(format!(
                "{n} is not in the allowed values: {:?}",
                self.values
            ))
        }
    }
}
