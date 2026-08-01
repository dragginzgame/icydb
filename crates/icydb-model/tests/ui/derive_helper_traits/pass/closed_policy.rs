use std::{
    fmt::Display,
    hash::Hash,
    iter::Sum,
    ops::{Add, AddAssign, Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
};

use icydb_model::prelude::*;

#[list(item(prim = "Nat64"), traits(remove(Default)))]
pub struct ManualListDefault {}

impl Default for ManualListDefault {
    fn default() -> Self {
        Self(Vec::new())
    }
}

#[newtype(item(prim = "Nat64"), default = 7u64, traits(add(Default)))]
pub struct OptInDefault {}

#[record(
    fields(field(name = "value", value(item(prim = "Nat64")))),
    traits(add(Copy, Hash, Ord, PartialOrd))
)]
pub struct StandardOptIns {}

#[newtype(
    item(prim = "Text", unbounded),
    traits(add(Deref, DerefMut, Display))
)]
pub struct WrapperOptIns {}

#[newtype(item(prim = "Nat64"))]
pub struct ArithmeticInner {}

#[newtype(
    item(is = "ArithmeticInner"),
    traits(add(
        Add,
        AddAssign,
        Div,
        DivAssign,
        Mul,
        MulAssign,
        Rem,
        Sub,
        SubAssign,
        Sum
    ))
)]
pub struct ArithmeticOptIns {}

fn assert_default<T: Default>() {}

fn assert_standard<T: Copy + Hash + Ord + PartialOrd>() {}

fn assert_wrapper<T: Deref<Target = String> + DerefMut + Display>() {}

fn assert_arithmetic<T>()
where
    T: Add<T, Output = T>
        + AddAssign<T>
        + Div<T, Output = T>
        + DivAssign<T>
        + Mul<T, Output = T>
        + MulAssign<T>
        + Rem<T, Output = T>
        + Sub<T, Output = T>
        + SubAssign<T>
        + Sum<T>,
{
}

fn main() {
    assert_default::<ManualListDefault>();
    assert_default::<OptInDefault>();
    assert_standard::<StandardOptIns>();
    assert_wrapper::<WrapperOptIns>();
    assert_arithmetic::<ArithmeticOptIns>();
}
