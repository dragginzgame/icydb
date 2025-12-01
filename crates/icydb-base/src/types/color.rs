use crate::prelude::*;

///
/// Rgb
///

#[record(fields(
    field(ident = "r", value(item(prim = "Nat8"))),
    field(ident = "g", value(item(prim = "Nat8"))),
    field(ident = "b", value(item(prim = "Nat8"))),
))]
pub struct Rgb {}

impl From<&RgbHex> for Rgb {
    fn from(hex: &RgbHex) -> Self {
        let r = u8::from_str_radix(&hex.0[0..2], 16).unwrap_or(0);
        let g = u8::from_str_radix(&hex.0[2..4], 16).unwrap_or(0);
        let b = u8::from_str_radix(&hex.0[4..6], 16).unwrap_or(0);

        Self { r, g, b }
    }
}

///
/// Rgba
///

#[record(fields(
    field(ident = "r", value(item(prim = "Nat8"))),
    field(ident = "g", value(item(prim = "Nat8"))),
    field(ident = "b", value(item(prim = "Nat8"))),
    field(ident = "a", value(item(prim = "Nat8"))),
))]
pub struct Rgba {}

impl Rgba {
    #[must_use]
    pub const fn new(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }
}

#[allow(clippy::many_single_char_names)]
impl From<&RgbaHex> for Rgba {
    fn from(hex: &RgbaHex) -> Self {
        let r = u8::from_str_radix(&hex.0[0..2], 16).unwrap_or(0);
        let g = u8::from_str_radix(&hex.0[2..4], 16).unwrap_or(0);
        let b = u8::from_str_radix(&hex.0[4..6], 16).unwrap_or(0);
        let a = u8::from_str_radix(&hex.0[6..8], 16).unwrap_or(0);

        Self { r, g, b, a }
    }
}

///
/// RgbHex
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    default = "FFFFFF",
    ty(
        sanitizer(path = "sanitizer::text::color::RgbHex"),
        validator(path = "validator::text::color::RgbHex")
    )
)]
pub struct RgbHex {}

impl From<Rgb> for RgbHex {
    fn from(rgb: Rgb) -> Self {
        Self(format!("{:02X}{:02X}{:02X}", rgb.r, rgb.g, rgb.b))
    }
}

///
/// RgbaHex
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    default = "FFFFFFFF",
    ty(
        sanitizer(path = "sanitizer::text::color::RgbaHex"),
        validator(path = "validator::text::color::RgbaHex")
    ),
    traits(remove(From))
)]
pub struct RgbaHex {}

impl From<Rgba> for RgbaHex {
    fn from(rgba: Rgba) -> Self {
        Self(format!(
            "{:02X}{:02X}{:02X}{:02X}",
            rgba.r, rgba.g, rgba.b, rgba.a
        ))
    }
}
