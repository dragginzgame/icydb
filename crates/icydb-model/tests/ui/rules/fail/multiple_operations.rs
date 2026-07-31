use icydb_model::prelude::*;

#[newtype(
    item(prim = "Nat8"),
    ty(rule(
        name = "range",
        numeric_minimum_inclusive(value = 0),
        numeric_maximum_inclusive(value = 100)
    ))
)]
pub struct MultipleOperations {}

fn main() {}
