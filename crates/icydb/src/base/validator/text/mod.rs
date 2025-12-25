pub mod case;
pub mod color;

use crate::{core::traits::Validator, design::prelude::*};

///
/// AlphaUscore
/// this doesn't force ASCII, instead we're using the unicode is_alphabetic
/// and ASCII is handled in a separate validator
///

#[validator]
pub struct AlphaUscore {}

impl Validator<str> for AlphaUscore {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.chars().all(|c| c.is_alphabetic() || c == '_') {
            Ok(())
        } else {
            Err(format!("'{s}' is not alphabetic with underscores"))
        }
    }
}

///
/// AlphanumUscore
///

#[validator]
pub struct AlphanumUscore {}

impl Validator<str> for AlphanumUscore {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(())
        } else {
            Err(format!("'{s}' is not alphanumeric with underscores"))
        }
    }
}

///
/// Ascii
///

#[validator]
pub struct Ascii {}

impl Validator<str> for Ascii {
    fn validate(&self, s: &str) -> Result<(), String> {
        if s.is_ascii() {
            Ok(())
        } else {
            Err("string contains non-ascii characters".to_string())
        }
    }
}
