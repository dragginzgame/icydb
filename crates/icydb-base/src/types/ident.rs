use crate::prelude::*;

///
/// Constant
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        validator(path = "validator::len::Range", args(1, 40)),
        validator(path = "validator::text::case::UpperSnake"),
    )
)]
pub struct Constant {}

///
/// Field
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        validator(path = "validator::len::Range", args(2, 40)),
        validator(path = "validator::text::case::Snake"),
    )
)]
pub struct Field {}

///
/// Function
///
/// 30 characters, snake_case
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        validator(path = "validator::len::Range", args(2, 64)),
        validator(path = "validator::text::case::Snake"),
    )
)]
pub struct Function {}

///
/// Variable
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        validator(path = "validator::len::Range", args(2, 40)),
        validator(path = "validator::text::case::Snake"),
    )
)]
pub struct Variable {}

///
/// Variant
///

#[newtype(
    primitive = "Text",
    item(prim = "Text"),
    ty(
        validator(path = "validator::len::Range", args(1, 40)),
        validator(path = "validator::text::case::UpperCamel"),
    )
)]
pub struct Variant {}
