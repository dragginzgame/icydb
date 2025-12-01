use crate::prelude::*;

///
/// Iso3166_1A2
/// Trims and uppercases the code
///

#[sanitizer]
pub struct Iso3166_1A2;

impl Sanitizer<String> for Iso3166_1A2 {
    fn sanitize(&self, value: String) -> String {
        value.trim().to_ascii_uppercase()
    }
}

///
/// Iso639_1
/// Trims and lowercases the code
///

#[sanitizer]
pub struct Iso639_1;

impl Sanitizer<String> for Iso639_1 {
    fn sanitize(&self, value: String) -> String {
        value.trim().to_ascii_lowercase()
    }
}
