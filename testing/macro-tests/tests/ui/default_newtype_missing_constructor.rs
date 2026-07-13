use icydb::design::prelude::*;

#[newtype(
    primitive = "Ulid",
    item(prim = "Ulid"),
    traits(add(Default))
)]
pub struct MissingNewtypeDefault;

fn main() {}
