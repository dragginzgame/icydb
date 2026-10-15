use icydb_model::prelude::*;

#[map(key(prim = "Unit"), value(item(prim = "Nat64")))]
pub struct UnitKey;

#[map(key(prim = "Nat64"), value(item(prim = "Unit")))]
pub struct UnitValue;

fn main() {}
