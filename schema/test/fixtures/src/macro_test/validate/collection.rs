use icydb::design::prelude::*;

///
/// List
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/validate/collection.rs::list::1",
    item(rel = "crate::macro_test::entity::Entity", prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct List {}

///
/// Set
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/validate/collection.rs::set::1",
    item(prim = "Ulid"),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Set {}

///
/// Map
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/validate/collection.rs::map::1",
    key(prim = "Ulid"),
    value(item(prim = "Text", unbounded)),
    ty(validator(path = "base::validator::len::Max", args(2)))
)]
pub struct Map {}
