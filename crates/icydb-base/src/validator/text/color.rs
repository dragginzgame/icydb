use crate::{core::traits::Validator, prelude::*};

///
/// RgbHex
///

#[validator]
pub struct RgbHex {}

impl Validator<str> for RgbHex {
    fn validate(&self, s: &str) -> Result<(), ValidateIssue> {
        if s.len() == 6 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "RGB hex string '{s}' must be exactly 6 hexadecimal characters"
            )))
        }
    }
}

///
/// RgbaHex
///

#[validator]
pub struct RgbaHex;

impl Validator<str> for RgbaHex {
    fn validate(&self, s: &str) -> Result<(), ValidateIssue> {
        if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "RGBA hex string '{s}' must be exactly 8 hexadecimal characters"
            )))
        }
    }
}
