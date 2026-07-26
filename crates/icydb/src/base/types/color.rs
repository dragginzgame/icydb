//! Module: base::types::color
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Rgb
///

#[record(
    source_key = "crates/icydb/src/base/types/color.rs::record::1",
    fields(
        field(source_key = "r", ident = "r", value(item(prim = "Nat8"))),
        field(source_key = "g", ident = "g", value(item(prim = "Nat8"))),
        field(source_key = "b", ident = "b", value(item(prim = "Nat8"))),
    )
)]
pub struct Rgb {}

///
/// Rgba
///

#[record(
    source_key = "crates/icydb/src/base/types/color.rs::record::2",
    fields(
        field(source_key = "r", ident = "r", value(item(prim = "Nat8"))),
        field(source_key = "g", ident = "g", value(item(prim = "Nat8"))),
        field(source_key = "b", ident = "b", value(item(prim = "Nat8"))),
        field(source_key = "a", ident = "a", value(item(prim = "Nat8"))),
    )
)]
pub struct Rgba {}

///
/// RgbHex
///

#[newtype(
    source_key = "crates/icydb/src/base/types/color.rs::newtype::1",
    primitive = "Text",
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
    source_key = "crates/icydb/src/base/types/color.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    default = "FFFFFFFF",
    ty(
        normalizer(path = "base::normalizer::text::color::RgbaHex"),
        validator(path = "base::validator::text::color::RgbaHex")
    ),
    traits(add(Default), remove(From))
)]
pub struct RgbaHex {}
