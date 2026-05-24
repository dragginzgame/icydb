use crate::{design::prelude::*, traits::Validator};

///
/// RgbHex
///

#[validator]
pub struct RgbHex;

impl Validator<str> for RgbHex {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !is_hex_width(s, 6) {
            ctx.issue(format!(
                "RGB hex string '{s}' must be exactly 6 hexadecimal characters"
            ));
        }
    }
}

///
/// RgbaHex
///

#[validator]
pub struct RgbaHex;

impl Validator<str> for RgbaHex {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !is_hex_width(s, 8) {
            ctx.issue(format!(
                "RGBA hex string '{s}' must be exactly 8 hexadecimal characters"
            ));
        }
    }
}

fn is_hex_width(value: &str, width: usize) -> bool {
    value.len() == width && value.chars().all(|c| c.is_ascii_hexdigit())
}
