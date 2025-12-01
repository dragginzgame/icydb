use crate::{core::traits::Validator, prelude::*};
use std::convert::TryInto;

///
/// MaxDecimalPlaces
///

#[validator]
pub struct MaxDecimalPlaces {
    target: u32,
}

impl MaxDecimalPlaces {
    /// Create a new validator with the given maximum number of decimal places.
    pub fn new<N>(target: N) -> Self
    where
        N: TryInto<u32>,
        N::Error: std::fmt::Debug,
    {
        Self {
            target: target.try_into().expect("invalid number of decimal places"),
        }
    }
}

impl Validator<Decimal> for MaxDecimalPlaces {
    fn validate(&self, n: &Decimal) -> Result<(), String> {
        if n.scale() <= self.target {
            Ok(())
        } else {
            let plural = if self.target == 1 { "" } else { "s" };

            Err(format!(
                "{n} must not have more than {} decimal place{}",
                self.target, plural
            ))
        }
    }
}
