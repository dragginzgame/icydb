use crate::{
    core::{traits::Validator, visitor::ValidateIssue},
    prelude::*,
};

///
/// InArray
///

#[validator]
pub struct InArray<T> {
    pub values: Vec<T>,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl<T> InArray<T> {
    #[must_use]
    pub fn new(values: Vec<T>) -> Self {
        if values.is_empty() {
            Self {
                values,
                error: Some(ValidateIssue::invalid_config(
                    "InArray validator requires at least one allowed value",
                )),
            }
        } else {
            Self {
                values,
                error: None,
            }
        }
    }
}

impl<T> Validator<T> for InArray<T>
where
    T: PartialEq + std::fmt::Debug + std::fmt::Display,
{
    fn validate(&self, n: &T) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        if self.values.contains(n) {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "{n} is not in the allowed values: {:?}",
                self.values
            )))
        }
    }
}
