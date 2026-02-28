//! Module: index::key::ordered::semantics
//! Responsibility: fixed-width scalar byte transforms preserving order.
//! Does not own: segment framing or canonical tag assignment.
//! Boundary: internal helper trait/functions for ordered encoding.

use crate::{
    db::index::key::ordered::OrderedValueEncodeError,
    types::{Date, Duration, Int128, Nat128, Repr, Timestamp},
};

///
/// OrderedEncode
///
/// Internal ordered-byte encoder for fixed-width value components.
///

pub(super) trait OrderedEncode {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError>;
}

impl OrderedEncode for Date {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError> {
        out.extend_from_slice(&ordered_i32_bytes(self.get()));
        Ok(())
    }
}

impl OrderedEncode for Duration {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError> {
        out.extend_from_slice(&self.repr().to_be_bytes());
        Ok(())
    }
}

impl OrderedEncode for Int128 {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError> {
        out.extend_from_slice(&ordered_i128_bytes(self.get()));
        Ok(())
    }
}

impl OrderedEncode for Nat128 {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError> {
        out.extend_from_slice(&self.get().to_be_bytes());
        Ok(())
    }
}

impl OrderedEncode for Timestamp {
    fn encode_ordered(&self, out: &mut Vec<u8>) -> Result<(), OrderedValueEncodeError> {
        out.extend_from_slice(&self.repr().to_be_bytes());
        Ok(())
    }
}

pub(super) const fn ordered_i32_bytes(value: i32) -> [u8; 4] {
    let biased = value.cast_unsigned() ^ (1u32 << 31);
    biased.to_be_bytes()
}

pub(super) const fn ordered_i64_bytes(value: i64) -> [u8; 8] {
    let biased = value.cast_unsigned() ^ (1u64 << 63);
    biased.to_be_bytes()
}

pub(super) const fn ordered_i128_bytes(value: i128) -> [u8; 16] {
    let biased = value.cast_unsigned() ^ (1u128 << 127);
    biased.to_be_bytes()
}

pub(super) const fn ordered_f32_bytes(value: f32) -> [u8; 4] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    };

    ordered.to_be_bytes()
}

pub(super) const fn ordered_f64_bytes(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    };

    ordered.to_be_bytes()
}
