use icydb_model::prelude::*;

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Inner)))]
pub struct UnsupportedInner {}

fn main() {}
