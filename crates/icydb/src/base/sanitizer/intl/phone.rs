//! Module: base::sanitizer::intl::phone
//!
//! Responsibility: base sanitizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade sanitizer traits.

use crate::design::prelude::*;

///
/// E164PhoneNumber
/// Parses and re-formats input into canonical E.164 string
///

#[sanitizer]
pub struct E164PhoneNumber;

impl Sanitizer<String> for E164PhoneNumber {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        if trimmed.is_empty() || trimmed.starts_with('+') {
            *value = trimmed.to_owned();
            return Ok(());
        }

        if trimmed.chars().all(|c| c.is_ascii_digit()) {
            *value = format!("+{trimmed}");
            return Ok(());
        }

        *value = trimmed.to_owned();

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phone_sanitizer_preserves_canonical_e164() {
        let sanitizer = E164PhoneNumber;
        let mut value = "  +15551234567  ".to_string();

        sanitizer.sanitize(&mut value).unwrap();

        assert_eq!(value, "+15551234567");
    }

    #[test]
    fn phone_sanitizer_adds_plus_to_plain_digits() {
        let sanitizer = E164PhoneNumber;
        let mut value = "15551234567".to_string();

        sanitizer.sanitize(&mut value).unwrap();

        assert_eq!(value, "+15551234567");
    }

    #[test]
    fn phone_sanitizer_preserves_non_digit_input_for_validator_rejection() {
        let sanitizer = E164PhoneNumber;
        let mut value = "+1 (555) 123-4567".to_string();

        sanitizer.sanitize(&mut value).unwrap();

        assert_eq!(value, "+1 (555) 123-4567");
    }
}
