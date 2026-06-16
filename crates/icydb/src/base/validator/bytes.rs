//! Module: base::validator::bytes
//!
//! Responsibility: base validator definitions.
//! Does not own: sanitization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{design::prelude::*, traits::Validator};

///
/// Utf8
///
/// Validates that a byte slice is well-formed UTF-8.
/// Emits a single issue when decoding fails.
///

#[validator]
pub struct Utf8;

impl Validator<[u8]> for Utf8 {
    fn validate(&self, bytes: &[u8], ctx: &mut dyn VisitorContext) {
        if std::str::from_utf8(bytes).is_err() {
            ctx.issue("bytes must be valid UTF-8");
        }
    }
}
