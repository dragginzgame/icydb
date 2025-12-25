use crate::{
    core::{traits::Validator, visitor::ValidateIssue},
    prelude::*,
};
use std::convert::TryInto;

///
/// MaxDecimalPlaces
///

#[validator]
pub struct MaxDecimalPlaces {
    target: u32,
    #[serde(skip)]
    error: Option<ValidateIssue>,
}

impl MaxDecimalPlaces {
    /// Create a new validator with the given maximum number of decimal places.
    pub fn new<N>(target: N) -> Self
    where
        N: TryInto<u32>,
        N::Error: std::fmt::Debug,
    {
        match target.try_into() {
            Ok(target) => Self {
                target,
                error: None,
            },
            Err(e) => Self {
                target: 0,
                error: Some(ValidateIssue::invalid_config(format!(
                    "invalid number of decimal places: {:?}",
                    e
                ))),
            },
        }
    }
}

impl Validator<Decimal> for MaxDecimalPlaces {
    fn validate(&self, n: &Decimal) -> Result<(), ValidateIssue> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }

        if n.scale() <= self.target {
            Ok(())
        } else {
            let plural = if self.target == 1 { "" } else { "s" };

            Err(ValidateIssue::validation(format!(
                "{n} must not have more than {} decimal place{}",
                self.target, plural
            )))
        }
    }
}
