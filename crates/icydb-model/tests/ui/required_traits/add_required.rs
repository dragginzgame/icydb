use icydb_model::prelude::*;

#[newtype(item(prim = "Nat64"), traits(add(CandidType)))]
pub struct RedundantCandidType {}

fn main() {}
