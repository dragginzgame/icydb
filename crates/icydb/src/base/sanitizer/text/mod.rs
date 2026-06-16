//! Module: base::sanitizer::text
//!
//! Responsibility: base sanitizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade sanitizer traits.

pub mod ascii;
pub mod case;
pub mod color;

use crate::design::prelude::*;

///
/// Trim
///

#[sanitizer]
pub struct Trim;

impl Sanitizer<String> for Trim {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        Ok(())
    }
}
