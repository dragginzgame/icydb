use crate::prelude::*;

///
/// E164PhoneNumber
/// Parses and re-formats input into canonical E.164 string
///

#[sanitizer]
pub struct E164PhoneNumber;

impl Sanitizer<String> for E164PhoneNumber {
    fn sanitize(&self, value: String) -> String {
        let mut out = String::with_capacity(value.len());

        // Keep only digits
        for c in value.chars() {
            if c.is_ascii_digit() {
                out.push(c);
            }
        }

        // Always prefix with '+'
        if out.is_empty() {
            out
        } else {
            format!("+{out}")
        }
    }
}
