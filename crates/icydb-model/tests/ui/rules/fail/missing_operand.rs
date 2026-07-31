use icydb_model::prelude::*;

#[newtype(
    item(prim = "Nat8"),
    ty(rule(name = "range", numeric_range_inclusive(min = 0)))
)]
pub struct MissingMaximum {}

fn main() {}
