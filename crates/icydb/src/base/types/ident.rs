//! Module: base::types::ident
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::design::prelude::*;

///
/// Constant
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ident.rs::newtype::1",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        validator(path = "base::validator::len::Range", args(1, 40)),
        validator(path = "base::validator::text::case::UpperSnake"),
    )
)]
pub struct Constant {}

///
/// Field
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ident.rs::newtype::2",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        validator(path = "base::validator::len::Range", args(2, 40)),
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
    source_key = "crates/icydb/src/base/types/ident.rs::newtype::3",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        validator(path = "base::validator::len::Range", args(2, 64)),
        validator(path = "base::validator::text::case::Snake"),
    )
)]
pub struct Function {}

///
/// Variable
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ident.rs::newtype::4",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        validator(path = "base::validator::len::Range", args(2, 40)),
        validator(path = "base::validator::text::case::Snake"),
    )
)]
pub struct Variable {}

///
/// Variant
///

#[newtype(
    source_key = "crates/icydb/src/base/types/ident.rs::newtype::5",
    primitive = "Text",
    item(prim = "Text", unbounded),
    ty(
        validator(path = "base::validator::len::Range", args(1, 40)),
        validator(path = "base::validator::text::case::UpperCamel"),
    )
)]
pub struct Variant {}
