//! Module: base::normalizer::intl::iso
//!
//! Responsibility: base normalizer definitions.
//! Does not own: validation policy, persistence, or schema mutation semantics.
//! Boundary: mutates schema field values through facade normalizer traits.

use crate::{prelude::*, visitor::Normalizer};

///
/// Iso3166_1A2
/// Trims and uppercases the code
///

#[normalizer]
pub struct Iso3166_1A2;

impl Normalizer<String> for Iso3166_1A2 {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        // trim in place
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        // uppercase in place (ASCII)
        value.make_ascii_uppercase();

        Ok(())
    }
}

///
/// Iso639_1
/// Trims and lowercases the code
///

#[normalizer]
pub struct Iso639_1;

impl Normalizer<String> for Iso639_1 {
    fn normalize(&self, value: &mut String) -> Result<(), String> {
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        value.make_ascii_lowercase();

        Ok(())
    }
}
