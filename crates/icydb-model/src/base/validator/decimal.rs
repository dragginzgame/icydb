//! Module: base::validator::decimal
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{prelude::*, visitor::Validator};

///
/// MaxDecimalPlaces
///
/// Enforces an upper bound on fractional precision for `Decimal` values.
/// Values with a larger scale than `target` are rejected.
///

#[validator]
pub struct MaxDecimalPlaces {
    target: u32,
    invalid_configuration: bool,
}

impl MaxDecimalPlaces {
    /// Configure a precision limit; failed conversion reports an issue on validation.
    pub fn new(target: impl TryInto<u32>) -> Self {
        let target = target.try_into().ok();
        Self {
            target: target.unwrap_or_default(),
            invalid_configuration: target.is_none(),
        }
    }
}

impl Validator<Decimal> for MaxDecimalPlaces {
    fn validate(&self, n: &Decimal, ctx: &mut dyn VisitorContext) {
        if self.invalid_configuration {
            ctx.issue("maximum decimal places cannot be represented as u32".to_string());
            return;
        }
        if n.scale() > self.target {
            ctx.issue(format!(
                "decimal scale {} must be at most {}",
                n.scale(),
                self.target
            ));
        }
    }
}
