use icydb_model::prelude::*;

///
/// List
///

#[list(
    source_key = "schema/test/fixtures/src/macro_test/core.rs::list::1",
    item(prim = "Text", unbounded)
)]
pub struct List;

///
/// Map
///

#[map(
    source_key = "schema/test/fixtures/src/macro_test/core.rs::map::1",
    key(prim = "Text", unbounded),
    value(item(prim = "Nat8"))
)]
pub struct Map;

///
/// Record
///

#[record(source_key = "schema/test/fixtures/src/macro_test/core.rs::record::1")]
pub struct Record;

///
/// Set
///

#[set(
    source_key = "schema/test/fixtures/src/macro_test/core.rs::set::1",
    item(prim = "Text", unbounded)
)]
pub struct Set;

///
/// EnumSorted
///

#[enum_(
    source_key = "schema/test/fixtures/src/macro_test/core.rs::enum_::nested::1",
    variant(source_key = "A", ident = "A"),
    variant(source_key = "B", ident = "B"),
    variant(source_key = "C", ident = "C"),
    variant(source_key = "D", ident = "D"),
    traits(add(Sorted))
)]
pub struct EnumSorted {}

///
/// Negative
/// (just to check on the rust-analyzer error)
///

#[newtype(source_key = "schema/test/fixtures/src/macro_test/core.rs::newtype::1",
    primitive = "Int8",
    item(prim = "Int8"),
    ty(validator(path = "base::validator::num::Range", args(-1, 3)))
)]
pub struct Negative {}

///
/// NewtypeValidated
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/core.rs::newtype::2",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 18),
    ty(validator(path = "base::validator::num::Lte", args(5.0)))
)]
pub struct NewtypeValidated {}
