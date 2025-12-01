pub mod collection;
pub mod entity;
pub mod merge;
pub mod newtype;
pub mod relation;
pub mod sanitize;
pub mod validate;
pub mod view_into;

use crate::prelude::*;

//
// SIMPLE TESTS
// these types just test themselves by existing
//

///
/// List
///

#[list(item(prim = "Text"))]
pub struct List;

///
/// Map
///

#[map(key(prim = "Text"), value(item(prim = "Nat8")))]
pub struct Map;

///
/// Record
///

#[record]
pub struct Record;

///
/// Set
///

#[set(item(prim = "Text"))]
pub struct Set;

///
/// EnumSorted
///

#[enum_(
    variant(ident = "A", default),
    variant(ident = "B"),
    variant(ident = "C"),
    variant(ident = "D"),
    traits(add(Sorted))
)]
pub struct EnumSorted {}

///
/// EnumUnspecified
///

#[enum_(
    variant(unspecified, default),
    variant(ident = "A"),
    variant(ident = "B"),
    variant(ident = "C"),
    variant(ident = "D")
)]
pub struct EnumUnspecified {}

///
/// Negative
/// (just to check on the rust-analyzer error)
///

#[newtype(
    primitive = "Int8",
    item(prim = "Int8"),
    ty(validator(path = "base::validator::num::Range", args(-1, 3)))
)]
pub struct Negative {}

///
/// NewtypeValidated
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal"),
    ty(validator(path = "base::validator::num::Lte", args(5.0)))
)]
pub struct NewtypeValidated {}
