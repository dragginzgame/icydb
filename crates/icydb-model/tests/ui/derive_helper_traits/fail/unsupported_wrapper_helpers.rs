use icydb_model::prelude::*;

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Deref)))]
pub struct UnsupportedDeref {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(DerefMut)))]
pub struct UnsupportedDerefMut {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Display)))]
pub struct UnsupportedDisplay {}

fn main() {}
