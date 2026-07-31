//! Module: base::types::color
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Rgb
///

#[record(fields(
    field(name = "r", value(item(prim = "Nat8"))),
    field(name = "g", value(item(prim = "Nat8"))),
    field(name = "b", value(item(prim = "Nat8"))),
))]
pub struct Rgb {}

///
/// Rgba
///

#[record(fields(
    field(name = "r", value(item(prim = "Nat8"))),
    field(name = "g", value(item(prim = "Nat8"))),
    field(name = "b", value(item(prim = "Nat8"))),
    field(name = "a", value(item(prim = "Nat8"))),
))]
pub struct Rgba {}

///
/// RgbHex
///

#[newtype(
    item(prim = "Text", unbounded),
    default = "FFFFFF",
    ty(
        normalizer(path = "base::normalizer::text::color::RgbHex"),
        validator(path = "base::validator::text::color::RgbHex")
    ),
    traits(add(Default))
)]
pub struct RgbHex {}

///
/// RgbaHex
///

#[newtype(
    item(prim = "Text", unbounded),
    default = "FFFFFFFF",
    ty(
        normalizer(path = "base::normalizer::text::color::RgbaHex"),
        validator(path = "base::validator::text::color::RgbaHex")
    ),
    traits(add(Default), remove(From))
)]
pub struct RgbaHex {}
