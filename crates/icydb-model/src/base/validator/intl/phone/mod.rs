//! Module: base::validator::intl::phone
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{prelude::*, visitor::Validator};

///
/// E164PhoneNumber
/// Ensures phone number has the canonical E.164 envelope:
/// `+` followed by 7 to 15 ASCII digits, with a non-zero first digit.
///

#[validator]
pub struct E164PhoneNumber;

impl Validator<str> for E164PhoneNumber {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        let Some(digits) = s.strip_prefix('+') else {
            ctx.issue("phone number must start with +");
            return;
        };

        if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
            ctx.issue("phone number must contain only digits after +");
            return;
        }

        if digits.starts_with('0') {
            ctx.issue("phone number country code must not start with 0");
            return;
        }

        let digit_count = digits.len();

        if !(7..=15).contains(&digit_count) {
            ctx.issue(format!(
                "phone number has {digit_count} digits; expected 7 to 15"
            ));
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
