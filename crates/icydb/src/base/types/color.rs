//! Module: base::types::color
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

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
    item(prim = "Text", unbounded),
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
    item(prim = "Text", unbounded),
    default = "FFFFFFFF",
    ty(
        sanitizer(path = "base::sanitizer::text::color::RgbaHex"),
        validator(path = "base::validator::text::color::RgbaHex")
    ),
    traits(remove(From))
)]
pub struct RgbaHex {}
