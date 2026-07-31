use icydb_model::prelude::*;

#[newtype(
    item(prim = "Text", unbounded),
    ty(rule(
        name = "length",
        length_range_inclusive(min = 1, max = 40)
    ))
)]
pub struct Label {}

#[newtype(
    item(prim = "Nat64"),
    ty(rule(name = "step", multiple_of(divisor = 5)))
)]
pub struct Step {}

#[newtype(
    item(prim = "Decimal", scale = 2),
    ty(rule(name = "step", multiple_of(divisor = 0.25)))
)]
pub struct DecimalStep {}

#[newtype(
    item(prim = "Nat64"),
    ty(rule(
        name = "maximum",
        numeric_maximum_inclusive(value = 100)
    ))
)]
pub struct Maximum {}

#[newtype(
    item(prim = "Int64"),
    ty(rule(
        name = "minimum",
        numeric_minimum_inclusive(value = -100)
    ))
)]
pub struct Minimum {}

#[newtype(
    item(prim = "Nat8"),
    ty(rule(
        name = "range",
        numeric_range_inclusive(min = 0, max = 100)
    ))
)]
pub struct Range {}

fn main() {}
