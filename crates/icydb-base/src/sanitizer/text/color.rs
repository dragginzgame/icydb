use crate::prelude::*;

///
/// RgbHex
///
/// Normalize RGB hex:
/// - `#RRGGBB` or `RRGGBB` (case-insensitive) → `RRGGBB`
/// - anything else → `"FFFFFF"`
///

#[sanitizer]
pub struct RgbHex;

impl Sanitizer<String> for RgbHex {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
        let raw = value.trim_start_matches('#');
        let hex = raw.trim(); // optional: keep or drop; remove if you only want to strip '#'

        let mut normalized = hex.to_ascii_uppercase();

        let ok = normalized.len() == 6 && normalized.chars().all(|c| c.is_ascii_hexdigit());
        if !ok {
            normalized = "FFFFFF".to_string();
        }

        *value = normalized;

        Ok(())
    }
}

///
/// RgbaHex
///
/// Normalize RGBA hex:
/// - `#RRGGBB` or `RRGGBB` → `RRGGBBFF`
/// - `#RRGGBBAA` or `RRGGBBAA` → `RRGGBBAA`
/// - anything else → `"FFFFFFFF"`
///

#[sanitizer]
pub struct RgbaHex;

impl Sanitizer<String> for RgbaHex {
    fn sanitize(&self, value: &mut String) -> Result<(), SanitizeIssue> {
        let raw = value.trim_start_matches('#');
        let hex = raw.trim(); // optional: keep or drop; remove if you only want to strip '#'

        let upper = hex.to_ascii_uppercase();

        let normalized = match upper.len() {
            6 if upper.chars().all(|c| c.is_ascii_hexdigit()) => format!("{upper}FF"),
            8 if upper.chars().all(|c| c.is_ascii_hexdigit()) => upper,
            _ => "FFFFFFFF".to_string(),
        };

        *value = normalized;

        Ok(())
    }
}
