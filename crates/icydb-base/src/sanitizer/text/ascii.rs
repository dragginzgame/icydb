use crate::{core::traits::Sanitizer, prelude::*};

///
/// AlphaNumeric
///
/// Removes any non-alphanumeric characters from the input string.
/// Keeps only ASCII digits 0–9, A–Z, a–z
///

#[sanitizer]
pub struct AlphaNumeric;

impl Sanitizer<String> for AlphaNumeric {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
        // Retain only ASCII alphanumeric characters
        value.retain(|c| c.is_ascii_alphanumeric());

        Ok(())
    }
}

///
/// Numeric
///
/// Removes any non-numeric characters from the input string.
/// Keeps only ASCII digits 0–9.
///

#[sanitizer]
pub struct Numeric;

impl Sanitizer<String> for Numeric {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
        value.retain(|c| c.is_ascii_digit());

        Ok(())
    }
}
