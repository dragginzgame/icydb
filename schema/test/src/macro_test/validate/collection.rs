use icydb::design::prelude::*;

///
/// List
///

#[list(
    item(rel = "crate::macro_test::entity::Entity", prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct List {}

///
/// Set
///

#[set(
    item(prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Set {}

///
/// Map
///

#[map(
    key(prim = "Ulid"),
    value(item(prim = "Text")),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Map {}
