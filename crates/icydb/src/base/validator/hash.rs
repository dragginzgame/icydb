use crate::{design::prelude::*, traits::Validator};

///
/// Sha256
///
/// Validates canonical SHA-256 hex digests.
/// Accepted values are exactly 64 ASCII hexadecimal characters.
///

#[validator]
pub struct Sha256;

impl Validator<str> for Sha256 {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        // length check
        if s.len() != 64 {
            ctx.issue(format!("SHA-256 hex digest length {} must be 64", s.len()));
            return;
        }

        // hex characters
        if !s.chars().all(|c| c.is_ascii_hexdigit()) {
            ctx.issue("SHA-256 digest must contain only hexadecimal characters");
        }
    }
}
