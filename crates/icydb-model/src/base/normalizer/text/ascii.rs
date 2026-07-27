//! Module: base::normalizer::text::ascii
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{prelude::*, visitor::Normalizer};

///
/// AlphaNumeric
///
/// Removes any non-alphanumeric characters from the input string.
/// Keeps only ASCII digits 0–9, A–Z, a–z
///

#[normalizer]
pub struct AlphaNumeric;

impl Normalizer<String> for AlphaNumeric {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
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

#[normalizer]
pub struct Numeric;

impl Normalizer<String> for Numeric {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        value.retain(|c| c.is_ascii_digit());

        Ok(())
    }
}
