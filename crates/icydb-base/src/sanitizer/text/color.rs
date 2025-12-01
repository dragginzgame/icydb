use crate::prelude::*;

///
/// RgbHex
///
/// Normalize RGB hex:
/// - `#RRGGBB` → `RRGGBB`
/// - anything else → `"FFFFFF"`
///

#[sanitizer]
pub struct RgbHex;

impl Sanitizer<String> for RgbHex {
    fn sanitize(&self, value: String) -> String {
        let hex = value.trim_start_matches('#').to_ascii_uppercase();

        if hex.len() == 6 {
            hex
        } else {
            String::from("FFFFFF")
        }
    }
}

///
/// RgbaHex
///
/// Normalize RGBA hex:
/// - `#RRGGBB` → `RRGGBBFF`
/// - `#RRGGBBAA` → `RRGGBBAA`
/// - anything else → `"FFFFFFFF"`
///

#[sanitizer]
pub struct RgbaHex;

impl Sanitizer<String> for RgbaHex {
    fn sanitize(&self, value: String) -> String {
        let hex = value.trim_start_matches('#').to_ascii_uppercase();

        match hex.len() {
            6 => format!("{hex}FF"),
            8 => hex,
            _ => String::from("FFFFFFFF"),
        }
    }
}
