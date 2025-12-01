use crate::{core::traits::Sanitizer, prelude::*};

///
/// AlphaNumeric
///
/// Removes any non-alphanumeric characters from the input string.
/// Keeps only ASCII digits 0–9, A-Z, a-z
///

#[sanitizer]
pub struct AlphaNumeric;

impl Sanitizer<String> for AlphaNumeric {
    fn sanitize(&self, value: String) -> String {
        value.chars().filter(char::is_ascii_alphanumeric).collect()
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
    fn sanitize(&self, value: String) -> String {
        value.chars().filter(char::is_ascii_digit).collect()
    }
}
