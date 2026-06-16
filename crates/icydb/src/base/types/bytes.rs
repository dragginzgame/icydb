//! Module: base::types::bytes
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Utf8
///

#[newtype(
    primitive = "Blob",
    item(prim = "Blob", unbounded),
    traits(remove(ValidateCustom))
)]
pub struct Utf8;

impl ValidateCustom for Utf8 {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        base::validator::bytes::Utf8.validate(self.0.as_bytes(), ctx);
    }
}
