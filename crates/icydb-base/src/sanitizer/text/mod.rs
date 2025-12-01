pub mod ascii;
pub mod case;
pub mod color;

use crate::{core::traits::Sanitizer, prelude::*};

///
/// Trim
///

#[sanitizer]
pub struct Trim;

impl Sanitizer<String> for Trim {
    fn sanitize(&self, value: String) -> String {
        value.trim().to_string()
    }
}
