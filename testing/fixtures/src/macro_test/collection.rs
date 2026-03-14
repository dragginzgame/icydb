use icydb::design::prelude::*;

///
/// Set
///

#[newtype(item(is = "SetInner"))]
pub struct Set {}

#[set(item(prim = "Nat8"))]
pub struct SetInner {}

///
/// ListValidated
///

#[list(item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))))]
pub struct ListValidated {}

///
/// MapValidated
///

#[map(
    key(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))),
    value(item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))))
)]
pub struct MapValidated {}

///
/// SetValidated
///

#[set(item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))))]
pub struct SetValidated {}
