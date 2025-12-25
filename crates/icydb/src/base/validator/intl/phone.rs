use crate::{core::traits::Validator, design::prelude::*};

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
    fn validate(&self, s: &str) -> Result<(), String> {
        if !s.starts_with('+') {
            return Err(format!("phone number '{s}' must start with '+'"));
        }

        let digits = s.chars().filter(char::is_ascii_digit).count();

        if !(7..=15).contains(&digits) {
            return Err(format!("phone number '{s}' has the wrong number of digits"));
        }

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Validator;

    // -------------------------------------------------------------------------
    // E.164 phone numbers
    // -------------------------------------------------------------------------
    #[test]
    fn test_e164_valid() {
        let v = E164PhoneNumber {};
        assert!(v.validate("+13108675309").is_ok()); // US number
        assert!(v.validate("+442071838750").is_ok()); // UK number
        assert!(v.validate("+819012345678").is_ok()); // Japan number
    }

    #[test]
    fn test_e164_invalid_format() {
        let v = E164PhoneNumber {};
        assert!(v.validate("4155552671").is_err()); // missing +
        assert!(v.validate("+99999999999999999").is_err()); // too long (>15 digits)
    }
}
