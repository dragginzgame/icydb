use crate::{core::traits::Validator, design::prelude::*};

///
/// InArray
///

#[validator]
pub struct InArray<T> {
    pub values: Vec<T>,
    #[serde(skip)]
    error: Option<String>,
}

impl<T> InArray<T> {
    #[must_use]
    pub fn new(values: Vec<T>) -> Self {
        if values.is_empty() {
            Self {
                values,
                error: Some("InArray validator requires at least one allowed value".to_string()),
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
    fn validate(&self, n: &T) -> Result<(), String> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

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
