use icydb_model::prelude::*;

#[newtype(item(prim = "Nat64"), traits(add(Serialize)))]
pub struct RedundantSerialize {}

fn main() {}
