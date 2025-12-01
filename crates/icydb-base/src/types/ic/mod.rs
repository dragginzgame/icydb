pub mod icrc1;
pub mod icrc3;

use crate::prelude::*;

///
/// Memo
///

#[newtype(primitive = "Blob", item(prim = "Blob"))]
pub struct Memo {}

///
/// Payment
///

#[record(fields(
    field(ident = "recipient", value(item(prim = "Principal"))),
    field(ident = "tokens", value(item(is = "Tokens")))
))]
pub struct Payment {}

///
/// Tokens
/// always denominated in e8s
///

#[newtype(primitive = "Nat64", item(prim = "Nat64"))]
pub struct Tokens {}
