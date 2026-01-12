use crate::{design::prelude::*, traits::Validator};

///
/// MaxDecimalPlaces
///

#[validator]
pub struct MaxDecimalPlaces {
    target: u32,
}

impl MaxDecimalPlaces {
    /// Create a new validator with the given maximum number of decimal places.
    pub fn new(target: impl TryInto<u32>) -> Self {
        Self {
            target: target.try_into().unwrap_or_default(),
        }
    }
}

impl Validator<Decimal> for MaxDecimalPlaces {
    fn validate(&self, n: &Decimal, ctx: &mut dyn VisitorContext) {
        if n.scale() > self.target {
            let plural = if self.target == 1 { "" } else { "s" };

            ctx.issue(format!(
                "{n} must not have more than {} decimal place{}",
                self.target, plural
            ));
        }
    }
}
