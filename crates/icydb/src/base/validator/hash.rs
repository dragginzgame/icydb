use crate::{design::prelude::*, traits::Validator};

///
/// Sha256
///

#[validator]
pub struct Sha256;

impl Validator<str> for Sha256 {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        // length check
        if s.len() != 64 {
            ctx.issue(format!("must be 64 characters, got {}", s.len()));
            return;
        }

        // hex characters
        if !s.chars().all(|c| c.is_ascii_hexdigit()) {
            ctx.issue("must contain only hexadecimal characters (0-9, a-f)".to_string());
        }
    }
}
