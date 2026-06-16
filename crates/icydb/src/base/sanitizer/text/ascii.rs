//! Module: base::sanitizer::text::ascii
//!
//! Responsibility: base sanitizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade sanitizer traits.

use crate::design::prelude::*;

///
/// AlphaNumeric
///
/// Removes any non-alphanumeric characters from the input string.
/// Keeps only ASCII digits 0–9, A–Z, a–z
///

#[sanitizer]
pub struct AlphaNumeric;

impl Sanitizer<String> for AlphaNumeric {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
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
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        value.retain(|c| c.is_ascii_digit());

        Ok(())
    }
}
