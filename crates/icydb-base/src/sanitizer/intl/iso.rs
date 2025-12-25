use crate::prelude::*;

///
/// Iso3166_1A2
/// Trims and uppercases the code
///

#[sanitizer]
pub struct Iso3166_1A2;

impl Sanitizer<String> for Iso3166_1A2 {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
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

#[sanitizer]
pub struct Iso639_1;

impl Sanitizer<String> for Iso639_1 {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
        let trimmed = value.trim();

        if trimmed.len() != value.len() {
            *value = trimmed.to_owned();
        }

        value.make_ascii_lowercase();

        Ok(())
    }
}
