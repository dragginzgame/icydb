use crate::design::prelude::*;

///
/// Rgb
///

#[record(fields(
    field(ident = "r", value(item(prim = "Nat8"))),
    field(ident = "g", value(item(prim = "Nat8"))),
    field(ident = "b", value(item(prim = "Nat8"))),
))]
pub struct Rgb {}

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

///
/// RgbHex
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    default = "FFFFFF",
    ty(
        sanitizer(path = "base::sanitizer::text::color::RgbHex"),
        validator(path = "base::validator::text::color::RgbHex")
    )
)]
pub struct RgbHex {}

///
/// RgbaHex
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    default = "FFFFFFFF",
    ty(
        sanitizer(path = "base::sanitizer::text::color::RgbaHex"),
        validator(path = "base::validator::text::color::RgbaHex")
    ),
    traits(remove(From))
)]
pub struct RgbaHex {}
