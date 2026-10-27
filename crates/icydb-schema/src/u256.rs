//! Canonical fixed-width unsigned 256-bit scalar.

use ethnum::U256 as EthU256;

use serde::{Deserialize, Deserializer, Serialize, Serializer as SerdeSerializer};
use std::{fmt, str::FromStr};

use crate::{Decimal, NumericValue};

const MAX_DECIMAL_DIGITS: usize = 78;
const DECIMAL_CHUNK_BASE: u64 = 100_000_000;
const DECIMAL_CHUNK_WIDTH: usize = 8;
const DECIMAL_BUFFER_LEN: usize = 80;
const U128_LOW_U32_MASK: u128 = 0xffff_ffff;

/// Error returned when decimal text is outside the unsigned 256-bit domain.
#[derive(Clone, Copy, Debug, Eq, thiserror::Error, PartialEq)]
#[error("value is not an unsigned 256-bit integer")]
pub struct ParseU256Error;

/// IcyDB-owned fixed-width unsigned 256-bit scalar.
///
/// Runtime values are inline and allocation-free. Human-readable Serde uses
/// decimal text; binary Serde uses an exact 32-byte big-endian byte string.
/// Persistence and index encodings are owned separately by IcyDB.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct U256(EthU256);

impl U256 {
    /// Minimum unsigned 256-bit value.
    pub const MIN: Self = Self::ZERO;

    /// Zero.
    pub const ZERO: Self = Self(EthU256::ZERO);

    /// One.
    pub const ONE: Self = Self(EthU256::ONE);

    /// Maximum unsigned 256-bit value.
    pub const MAX: Self = Self(EthU256::MAX);

    /// Build from two 128-bit words in numeric high/low order.
    #[must_use]
    pub const fn from_words(high: u128, low: u128) -> Self {
        Self(EthU256::from_words(high, low))
    }

    /// Split into two 128-bit words in numeric high/low order.
    #[must_use]
    pub const fn into_words(self) -> (u128, u128) {
        self.0.into_words()
    }

    /// Build from exactly 32 unsigned big-endian bytes.
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 32]) -> Self {
        Self(EthU256::from_be_bytes(bytes))
    }

    /// Return exactly 32 unsigned big-endian bytes.
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 32] {
        self.0.to_be_bytes()
    }

    /// Convert to `u128` when the value is in range.
    #[must_use]
    pub const fn to_u128(self) -> Option<u128> {
        let (high, low) = self.into_words();
        if high == 0 { Some(low) } else { None }
    }

    /// Add two values, returning `None` on unsigned 256-bit overflow.
    #[must_use]
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Self)
    }

    /// Subtract two values, returning `None` on unsigned 256-bit underflow.
    #[must_use]
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Self)
    }

    /// Multiply two values, returning `None` on unsigned 256-bit overflow.
    #[must_use]
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.0.checked_mul(rhs.0).map(Self)
    }

    /// Divide two values, returning `None` when the divisor is zero.
    #[must_use]
    pub fn checked_div(self, rhs: Self) -> Option<Self> {
        self.0.checked_div(rhs.0).map(Self)
    }

    /// Return the remainder, or `None` when the divisor is zero.
    #[must_use]
    pub fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.0.checked_rem(rhs.0).map(Self)
    }

    fn decimal_text(self) -> DecimalText {
        let (high, low) = self.into_words();
        let mut limbs = [
            low_u32_from_u128(high >> 96),
            low_u32_from_u128(high >> 64),
            low_u32_from_u128(high >> 32),
            low_u32_from_u128(high),
            low_u32_from_u128(low >> 96),
            low_u32_from_u128(low >> 64),
            low_u32_from_u128(low >> 32),
            low_u32_from_u128(low),
        ];
        let mut bytes = [0_u8; DECIMAL_BUFFER_LEN];
        let mut start = bytes.len();

        loop {
            let mut remainder = 0_u64;
            let mut quotient_is_zero = true;
            for limb in &mut limbs {
                let dividend = (remainder << 32) | u64::from(*limb);
                let quotient = dividend / DECIMAL_CHUNK_BASE;
                remainder = dividend % DECIMAL_CHUNK_BASE;
                // Long division keeps the quotient within one base-2^32 limb.
                *limb = u32::try_from(quotient).unwrap_or_default();
                quotient_is_zero &= quotient == 0;
            }

            // The remainder is strictly below the 1e8 decimal chunk base.
            let mut chunk = u32::try_from(remainder).unwrap_or_default();
            for _ in 0..DECIMAL_CHUNK_WIDTH {
                start -= 1;
                bytes[start] = b'0' + u8::try_from(chunk % 10).unwrap_or_default();
                chunk /= 10;
            }
            if quotient_is_zero {
                break;
            }
        }
        while start + 1 < bytes.len() && bytes[start] == b'0' {
            start += 1;
        }

        DecimalText { bytes, start }
    }
}

fn low_u32_from_u128(value: u128) -> u32 {
    u32::try_from(value & U128_LOW_U32_MASK).unwrap_or_default()
}

struct DecimalText {
    bytes: [u8; DECIMAL_BUFFER_LEN],
    start: usize,
}

impl DecimalText {
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[self.start..]).unwrap_or_default()
    }
}

impl fmt::Debug for U256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for U256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = self.decimal_text();
        formatter.write_str(text.as_str())
    }
}

impl From<u64> for U256 {
    fn from(value: u64) -> Self {
        Self(EthU256::from(value))
    }
}

impl From<u128> for U256 {
    fn from(value: u128) -> Self {
        Self(EthU256::from(value))
    }
}

impl FromStr for U256 {
    type Err = ParseU256Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > MAX_DECIMAL_DIGITS
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(ParseU256Error);
        }
        value
            .parse::<EthU256>()
            .map(Self)
            .map_err(|_| ParseU256Error)
    }
}

impl NumericValue for U256 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_u128().and_then(Decimal::from_u128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().map(Self::from)
    }
}

impl<'de> Deserialize<'de> for U256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return String::deserialize(deserializer)?
                .parse()
                .map_err(serde::de::Error::custom);
        }
        crate::atom::deserialize_fixed_bytes(deserializer).map(Self::from_be_bytes)
    }
}

impl Serialize for U256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: SerdeSerializer,
    {
        if serializer.is_human_readable() {
            return serializer.collect_str(self);
        }
        serializer.serialize_bytes(&self.to_be_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::U256;
    use crate::{Decimal, NumericValue};

    #[test]
    fn wire_uses_fixed_width_bytes_and_rejects_wrong_lengths() {
        let bytes = crate::wire::encode_one(U256::MAX).unwrap();
        assert_eq!(bytes, [&[32][..], &[0xff; 32][..]].concat());
        assert_eq!(crate::wire::decode_one::<U256>(&bytes).unwrap(), U256::MAX);
        for len in [0, 31, 33] {
            assert!(
                crate::wire::decode_one::<U256>(
                    &crate::wire::encode_one(serde_bytes::Bytes::new(&vec![0xff; len])).unwrap()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn fixed_bytes_and_decimal_are_exact() {
        let value = "57896044618658097711785492504343953926634992332820282019728792003956564819968"
            .parse::<U256>()
            .expect("2^255 should parse");
        assert_eq!(value.to_be_bytes()[0], 0x80);
        assert_eq!(U256::from_be_bytes(value.to_be_bytes()), value);
        assert_eq!(
            value.to_string(),
            "57896044618658097711785492504343953926634992332820282019728792003956564819968"
        );
        assert_eq!(U256::ZERO.to_string(), "0");
        assert_eq!(U256::ONE.to_string(), "1");
        assert_eq!(U256::MAX.to_string(), u256_max_decimal());
    }

    #[test]
    fn checked_arithmetic_enforces_the_u256_domain() {
        let two = U256::from(2_u64);
        let three = U256::from(3_u64);

        assert_eq!(two.checked_add(three), Some(U256::from(5_u64)));
        assert_eq!(three.checked_sub(two), Some(U256::ONE));
        assert_eq!(two.checked_mul(three), Some(U256::from(6_u64)));
        assert_eq!(U256::from(7_u64).checked_div(two), Some(three));
        assert_eq!(U256::from(7_u64).checked_rem(two), Some(U256::ONE));
        assert_eq!(U256::MAX.checked_add(U256::ONE), None);
        assert_eq!(U256::ZERO.checked_sub(U256::ONE), None);
        assert_eq!(U256::MAX.checked_mul(two), None);
        assert_eq!(U256::ONE.checked_div(U256::ZERO), None);
        assert_eq!(U256::ONE.checked_rem(U256::ZERO), None);
    }

    #[test]
    fn generic_numeric_conversion_is_fallible_without_widening_the_u256_domain() {
        let value = U256::from(u128::try_from(i128::MAX).expect("i128::MAX should fit u128"));
        assert_eq!(
            value.try_to_decimal().and_then(U256::try_from_decimal),
            Some(value),
        );
        assert_eq!(U256::MAX.try_to_decimal(), None);
        let negative_one = Decimal::from_i128(-1).expect("-1 should be a valid Decimal");
        assert_eq!(U256::try_from_decimal(negative_one), None);
    }

    fn u256_max_decimal() -> &'static str {
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    }
}
