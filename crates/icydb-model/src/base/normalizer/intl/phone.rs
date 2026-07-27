//! Module: base::normalizer::intl::phone
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{prelude::*, visitor::Normalizer};

///
/// E164PhoneNumber
/// Parses and re-formats input into canonical E.164 string
///

#[normalizer]
pub struct E164PhoneNumber;

impl Normalizer<String> for E164PhoneNumber {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
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
    fn phone_normalizer_preserves_canonical_e164() {
        let normalizer = E164PhoneNumber;
        let mut value = "  +15551234567  ".to_string();

        normalizer.normalize(&mut value).unwrap();

        assert_eq!(value, "+15551234567");
    }

    #[test]
    fn phone_normalizer_adds_plus_to_plain_digits() {
        let normalizer = E164PhoneNumber;
        let mut value = "15551234567".to_string();

        normalizer.normalize(&mut value).unwrap();

        assert_eq!(value, "+15551234567");
    }

    #[test]
    fn phone_normalizer_preserves_non_digit_input_for_validator_rejection() {
        let normalizer = E164PhoneNumber;
        let mut value = "+1 (555) 123-4567".to_string();

        normalizer.normalize(&mut value).unwrap();

        assert_eq!(value, "+1 (555) 123-4567");
    }
}
