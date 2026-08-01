use icydb_model::prelude::*;

#[newtype(item(prim = "Duration"), traits(add(Add)))]
pub struct RedundantDurationAdd {}

#[newtype(item(prim = "Blob", unbounded), traits(add(Hash)))]
pub struct RedundantBlobHash {}

#[newtype(item(prim = "Unit"), traits(add(Ord)))]
pub struct RedundantUnitOrd {}

#[enum_(
    variant(name = "First"),
    variant(name = "Second"),
    traits(add(Ord))
)]
pub struct RedundantUnitEnumOrd {}

#[newtype(item(prim = "Int64"), traits(add(Neg)))]
pub struct RedundantSignedNeg {}

#[newtype(item(prim = "Nat64"), traits(add(Product)))]
pub struct RedundantUnsignedProduct {}

#[newtype(item(prim = "Nat64"), traits(add(RemAssign)))]
pub struct RedundantUnsignedRemAssign {}

fn main() {}
