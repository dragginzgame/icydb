use std::{
    hash::Hash,
    iter::Product,
    ops::{Add, AddAssign, Neg, RemAssign, Sub, SubAssign},
};

use icydb_model::prelude::*;

#[newtype(item(prim = "Duration"))]
pub struct DurationValue {}

#[newtype(item(prim = "Blob", unbounded))]
pub struct BlobValue {}

#[newtype(item(prim = "Unit"))]
pub struct UnitValue {}

#[enum_(variant(name = "First"), variant(name = "Second"))]
pub struct UnitEnum {}

#[newtype(item(prim = "Int64"))]
pub struct SignedValue {}

#[newtype(item(prim = "Nat64"))]
pub struct UnsignedValue {}

#[newtype(item(prim = "IntBig", max_bytes = 64))]
pub struct SignedBigValue {}

#[newtype(item(prim = "NatBig", max_bytes = 64))]
pub struct UnsignedBigValue {}

#[newtype(item(prim = "Decimal", scale = 4))]
pub struct DecimalValue {}

fn assert_additive<T>()
where
    T: Add<T, Output = T> + AddAssign<T> + Sub<T, Output = T> + SubAssign<T>,
{
}

fn assert_hash_and_order<T: Hash + Ord + PartialOrd>() {}

fn assert_copy_hash_and_order<T: Copy + Hash + Ord + PartialOrd>() {}

fn assert_neg<T: Neg<Output = T>>() {}

fn assert_product<T: Product<T>>() {}

fn assert_rem_assign<T: RemAssign<T>>() {}

fn main() {
    assert_additive::<DurationValue>();
    assert_hash_and_order::<BlobValue>();
    assert_copy_hash_and_order::<UnitValue>();
    assert_copy_hash_and_order::<UnitEnum>();
    assert_neg::<SignedValue>();
    assert_neg::<SignedBigValue>();
    assert_neg::<DecimalValue>();
    assert_product::<SignedValue>();
    assert_product::<UnsignedValue>();
    assert_product::<SignedBigValue>();
    assert_product::<UnsignedBigValue>();
    assert_product::<DecimalValue>();
    assert_rem_assign::<SignedValue>();
    assert_rem_assign::<UnsignedValue>();
    assert_rem_assign::<DecimalValue>();
}
