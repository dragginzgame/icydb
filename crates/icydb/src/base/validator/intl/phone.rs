use crate::{design::prelude::*, traits::Validator};

///
/// E164PhoneNumber
/// Ensures phone number is valid and E.164 compliant
///
/// NOTE: not currently E.164 standard as the phonenumber crate is heavy
/// and includes regex.  So it's rough E.164.
///

#[validator]
pub struct E164PhoneNumber;

impl Validator<str> for E164PhoneNumber {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !s.starts_with('+') {
            ctx.issue(format!("phone number '{s}' must start with '+'"));
            return;
        }

        let digits = s.chars().filter(char::is_ascii_digit).count();

        if !(7..=15).contains(&digits) {
            ctx.issue(format!("phone number '{s}' has the wrong number of digits"));
        }
    }
}
