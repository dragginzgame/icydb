use icydb_model::prelude::*;

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(ValidateCustom)))]
pub struct RedundantValidateCustom {}

fn main() {}
