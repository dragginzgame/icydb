pub mod ascii;
pub mod case;
pub mod color;

use crate::{core::traits::Sanitizer, design::prelude::*};

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
