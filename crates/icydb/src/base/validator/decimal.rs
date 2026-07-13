//! Module: base::validator::decimal
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{design::prelude::*, visitor::Validator};

///
/// MaxDecimalPlaces
///
/// Enforces an upper bound on fractional precision for `Decimal` values.
/// Values with a larger scale than `target` are rejected.
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
            ctx.issue(format!(
                "decimal scale {} must be at most {}",
                n.scale(),
                self.target
            ));
        }
    }
}
