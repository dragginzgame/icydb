use crate::{core::traits::Validator, prelude::*};

///
/// RgbHex
///

#[validator]
pub struct RgbHex {}

impl Validator<str> for RgbHex {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.len() == 6 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(format!(
                "RGBA string '{s}' should be 6 hexadecimal characters"
            ))
        }
    }
}

///
/// RgbaHex
///

#[validator]
pub struct RgbaHex {}

impl Validator<str> for RgbaHex {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            Ok(())
        } else {
            Err(format!(
                "RGBA string '{s}' should be 8 hexadecimal characters"
            ))
        }
    }
}
