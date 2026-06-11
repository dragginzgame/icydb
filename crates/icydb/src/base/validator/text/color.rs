use crate::{design::prelude::*, traits::Validator};

///
/// RgbHex
///

#[validator]
pub struct RgbHex;

impl Validator<str> for RgbHex {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        if !is_hex_width(s, 6) {
            ctx.issue("hex color must contain 6 hexadecimal digits");
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
            ctx.issue("hex color must contain 8 hexadecimal digits");
        }
    }
}

fn is_hex_width(value: &str, width: usize) -> bool {
    value.len() == width && value.chars().all(|c| c.is_ascii_hexdigit())
}
