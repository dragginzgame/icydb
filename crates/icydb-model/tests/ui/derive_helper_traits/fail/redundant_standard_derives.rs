use icydb_model::prelude::*;

#[newtype(item(prim = "Nat64"), traits(add(Copy)))]
pub struct RedundantCopy {}

#[newtype(item(prim = "Nat64"), traits(add(Hash)))]
pub struct RedundantHash {}

#[newtype(item(prim = "Nat64"), traits(add(Ord)))]
pub struct RedundantOrd {}

#[newtype(item(prim = "Nat64"), traits(add(PartialOrd)))]
pub struct RedundantPartialOrd {}

fn main() {}
