use crate::design::prelude::*;

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
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = normalize_hex(value, "FFFFFF", |hex| {
            is_hex_width(hex, 6).then(|| hex.to_string())
        });

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
    fn sanitize(&self, value: &mut String) -> Result<(), String> {
        *value = normalize_hex(value, "FFFFFFFF", |hex| match hex.len() {
            6 if is_hex(hex) => Some(format!("{hex}FF")),
            8 if is_hex(hex) => Some(hex.to_string()),
            _ => None,
        });

        Ok(())
    }
}

fn normalize_hex(
    value: &str,
    fallback: &str,
    normalize: impl FnOnce(&str) -> Option<String>,
) -> String {
    let hex = value.trim_start_matches('#').trim().to_ascii_uppercase();

    normalize(hex.as_str()).unwrap_or_else(|| fallback.to_string())
}

fn is_hex_width(value: &str, width: usize) -> bool {
    value.len() == width && is_hex(value)
}

fn is_hex(value: &str) -> bool {
    value.chars().all(|c| c.is_ascii_hexdigit())
}
