//! Module: base::normalizer::text
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

pub mod ascii;
pub mod case;
pub mod color;

use crate::{prelude::*, visitor::Normalizer};

///
/// Trim
///

#[normalizer]
pub struct Trim;

impl Normalizer<String> for Trim {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        Ok(())
    }
}
