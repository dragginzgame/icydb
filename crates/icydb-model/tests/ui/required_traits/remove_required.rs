use icydb_model::prelude::*;

#[record(
    fields(field(name = "value", value(item(prim = "Nat64")))),
    traits(remove(Path))
)]
pub struct MissingPath {}

fn main() {}
