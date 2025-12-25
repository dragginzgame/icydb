use crate::{core::traits::Validator, prelude::*};

///
/// Utf8
///

#[validator]
pub struct Utf8;

impl Validator<[u8]> for Utf8 {
    fn validate(&self, bytes: &[u8]) -> Result<(), ValidateIssue> {
        std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| ValidateIssue::validation("invalid UTF-8 data"))
    }
}
