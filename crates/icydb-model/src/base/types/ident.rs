//! Module: base::types::ident
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Constant
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        rule(name = "length", length_range_inclusive(min = 1, max = 40)),
        validator(path = "base::validator::text::case::UpperSnake"),
    )
)]
pub struct Constant {}

///
/// Field
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        rule(name = "length", length_range_inclusive(min = 2, max = 40)),
        validator(path = "base::validator::text::case::Snake"),
    )
)]
pub struct Field {}

///
/// Function
///
/// 30 characters, snake_case
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        rule(name = "length", length_range_inclusive(min = 2, max = 64)),
        validator(path = "base::validator::text::case::Snake"),
    )
)]
pub struct Function {}

///
/// Variable
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        rule(name = "length", length_range_inclusive(min = 2, max = 40)),
        validator(path = "base::validator::text::case::Snake"),
    )
)]
pub struct Variable {}

///
/// Variant
///

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        rule(name = "length", length_range_inclusive(min = 1, max = 40)),
        validator(path = "base::validator::text::case::UpperCamel"),
    )
)]
pub struct Variant {}
