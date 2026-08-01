use icydb_model::prelude::*;

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Add)))]
pub struct UnsupportedAdd {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(AddAssign)))]
pub struct UnsupportedAddAssign {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Div)))]
pub struct UnsupportedDiv {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(DivAssign)))]
pub struct UnsupportedDivAssign {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Mul)))]
pub struct UnsupportedMul {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(MulAssign)))]
pub struct UnsupportedMulAssign {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Rem)))]
pub struct UnsupportedRem {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Sub)))]
pub struct UnsupportedSub {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(SubAssign)))]
pub struct UnsupportedSubAssign {}

#[record(fields(field(name = "value", value(item(prim = "Nat64")))), traits(add(Sum)))]
pub struct UnsupportedSum {}

fn main() {}
