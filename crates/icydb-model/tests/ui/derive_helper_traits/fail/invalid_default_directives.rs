use icydb_model::prelude::*;

#[list(item(prim = "Nat64"), traits(add(Default)))]
pub struct RedundantDefault {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(remove(Default)))]
pub struct MissingDefault {}

fn main() {}
