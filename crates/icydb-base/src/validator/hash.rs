use crate::{core::traits::Validator, prelude::*};

///
/// Sha256
///

#[validator]
pub struct Sha256 {}

impl Validator<str> for Sha256 {
    fn validate(&self, s: &str) -> Result<(), String> {
        // len
        if s.len() != 64 {
            return Err(format!("must be 64 characters, got {}", s.len()));
        }

        // hex
        if !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("must contain only hexadecimal characters (0-9, a-f)".to_string());
        }

        Ok(())
    }
}
