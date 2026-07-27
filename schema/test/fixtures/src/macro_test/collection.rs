use icydb_model::prelude::*;

///
/// Set
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/collection.rs::newtype::1",
    item(is = "SetInner")
)]
pub struct Set {}

#[set(
    source_key = "schema/test/fixtures/src/macro_test/collection.rs::set::1",
    item(prim = "Nat8")
)]
pub struct SetInner {}

///
/// ListValidated
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/collection.rs::list::1",
    item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10)))
)]
pub struct ListValidated {}

///
/// MapValidated
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/collection.rs::map::1",
    key(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))),
    value(item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10))))
)]
pub struct MapValidated {}

///
/// SetValidated
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/collection.rs::set::2",
    item(prim = "Nat8", validator(path = "base::validator::num::Lt", args(10)))
)]
pub struct SetValidated {}
