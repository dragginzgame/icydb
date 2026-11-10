use icydb::model::prelude::*;

///
/// List
///

#[list(item(prim = "Text", unbounded))]
pub struct List;

///
/// Map
///

#[map(key(prim = "Text", unbounded), value(item(prim = "Nat8")))]
pub struct Map;

///
/// Record
///

#[record]
pub struct Record;

///
/// Set
///

#[set(item(prim = "Text", unbounded))]
pub struct Set;

///
/// EnumSorted
///

#[enum_(
    sorted,
    variant(name = "A"),
    variant(name = "B"),
    variant(name = "C"),
    variant(name = "D")
)]
pub struct EnumSorted {}

///
/// Negative
/// (just to check on the rust-analyzer error)
///

#[newtype(
    item(prim = "Int8"),
    ty(validator(path = "base::validator::num::Range", args(-1, 3)))
)]
pub struct Negative {}

///
/// NewtypeValidated
///

#[newtype(
    item(prim = "Decimal", scale = 18),
    ty(validator(path = "base::validator::num::Lte", args(5.0)))
)]
pub struct NewtypeValidated {}

#[cfg(test)]
mod tests;
