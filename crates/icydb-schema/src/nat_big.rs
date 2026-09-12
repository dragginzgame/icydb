//! Canonical arbitrary-precision unsigned-integer atom.

use crate::{
    Decimal, NumericValue,
    integer_wire::{self, IntegerWire},
};
use candid::{CandidType, Nat as WrappedNat};
use derive_more::{Add, AddAssign, Sub, SubAssign};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    iter::{Product, Sum},
    ops::{Div, DivAssign, Mul, MulAssign},
    str::FromStr,
};

//
// NatBig
//

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Sub,
    SubAssign,
)]
/// Arbitrary-precision unsigned integer used by schema and typed values.
///
/// Candid uses its native integer type; human-readable Serde uses decimal text.
/// Binary Serde uses native small integers and tagged little-endian wide bytes.
pub struct NatBig(WrappedNat);

impl NatBig {
    /// Return the magnitude's bit length without allocating an encoded copy.
    #[must_use]
    pub fn magnitude_bits(&self) -> u64 {
        self.0.0.bits()
    }

    /// Return the exact unsigned LEB128 byte length without allocating or encoding.
    #[must_use]
    pub fn leb128_len(&self) -> u64 {
        self.magnitude_bits().div_ceil(7).max(1)
    }

    /// Construct from the canonical Candid natural-number representation.
    #[must_use]
    pub const fn from_candid(value: WrappedNat) -> Self {
        Self(value)
    }

    /// Construct from a `num_bigint` unsigned integer.
    #[must_use]
    pub fn from_biguint(value: BigUint) -> Self {
        Self::from_candid(WrappedNat::from(value))
    }

    /// Borrow little-endian base-2^32 limbs without allocation.
    #[must_use]
    pub fn u32_digits(&self) -> impl DoubleEndedIterator<Item = u32> + ExactSizeIterator + '_ {
        self.0.0.iter_u32_digits()
    }

    /// Convert to `u128` when the value is in range.
    #[must_use]
    pub fn to_u128(&self) -> Option<u128> {
        let big = &self.0.0;

        u128::try_from(big).ok()
    }

    /// Convert to `u64` when the value is in range.
    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        let big = &self.0.0;

        u64::try_from(big).ok()
    }

    /// Serialize this arbitrary-precision natural for internal hash and sort-key framing.
    #[must_use]
    pub fn to_leb128(&self) -> Vec<u8> {
        self.leb128_bytes().collect()
    }

    /// Iterate canonical unsigned LEB128 bytes with constant scratch and no allocation.
    pub fn leb128_bytes(&self) -> impl Iterator<Item = u8> + '_ {
        crate::leb128::bytes(self.u32_digits(), false, self.leb128_len())
    }

    pub(crate) fn to_magnitude_bytes(&self) -> Vec<u8> {
        self.0.0.to_bytes_be()
    }

    pub(crate) fn from_magnitude_bytes(magnitude: &[u8]) -> Self {
        Self::from_biguint(BigUint::from_bytes_be(magnitude))
    }

    /// Saturating addition (unbounded; equivalent to normal addition).
    #[must_use]
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }

    /// Saturating subtraction; clamps at zero on underflow.
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        if rhs > self {
            return Self::default();
        }

        Self(self.0 - rhs.0)
    }
}

impl<'de> Deserialize<'de> for NatBig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        integer_wire::deserialize_integer(deserializer)
    }
}

impl fmt::Display for NatBig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NatBig {
    type Err = <WrappedNat as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedNat::from_str(s).map(Self::from_candid)
    }
}

impl Div for NatBig {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for NatBig {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl From<u64> for NatBig {
    fn from(n: u64) -> Self {
        Self::from_candid(WrappedNat::from(n))
    }
}

impl From<u32> for NatBig {
    fn from(n: u32) -> Self {
        Self::from_candid(WrappedNat::from(n))
    }
}

impl IntegerWire for NatBig {
    fn from_signed(value: i64) -> Option<Self> {
        u64::try_from(value).ok().map(Self::from)
    }

    fn from_unsigned(value: u64) -> Self {
        Self::from(value)
    }

    fn from_wire_bytes(value: &[u8]) -> Option<Self> {
        integer_wire::unsigned_body(value)
            .map(|body| Self::from_biguint(BigUint::from_bytes_le(body)))
    }
}

impl Mul for NatBig {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for NatBig {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl NumericValue for NatBig {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_u128().and_then(Decimal::from_u128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().map(WrappedNat::from).map(Self::from_candid)
    }
}

impl Product for NatBig {
    fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::from(1_u32), |acc, value| acc * value)
    }
}

impl Serialize for NatBig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return serializer.collect_str(&self.0.0);
        }
        if let Some(value) = self.to_u64() {
            return serializer.serialize_u64(value);
        }
        serializer.serialize_bytes(&integer_wire::unsigned_bytes(self.u32_digits()))
    }
}

impl Sum for NatBig {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl TryFrom<i32> for NatBig {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self::from_candid(WrappedNat::from(u32::try_from(n)?));
        Ok(v)
    }
}
