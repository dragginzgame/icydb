use crate::design::prelude::*;

///
/// E164PhoneNumber
/// Parses and re-formats input into canonical E.164 string
///

#[sanitizer]
pub struct E164PhoneNumber;

impl Sanitizer<String> for E164PhoneNumber {
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        // Retain only ASCII digits
        value.retain(|c| c.is_ascii_digit());

        if !value.is_empty() {
            // Prepend '+'
            value.insert(0, '+');
        }

        Ok(())
    }
}
