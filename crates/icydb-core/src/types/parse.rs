//! Module: types::parse
//! Responsibility: shared strict text parsing helpers for fixed-width core type formats.
//! Does not own: type-specific validation or format policy for date, timestamp, or duration.
//! Boundary: core scalar types call into this module for reusable ASCII digit parsing only.

/// Parse one fixed-width ASCII digit slice into an `i32`.
#[must_use]
pub(crate) fn parse_fixed_ascii_i32(bytes: &[u8]) -> Option<i32> {
    let mut value = 0_i32;
    for &byte in bytes {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i32::from(digit))?;
    }

    Some(value)
}

/// Parse one fixed-width ASCII digit slice into a `u8`.
#[must_use]
pub(crate) fn parse_fixed_ascii_u8(bytes: &[u8]) -> Option<u8> {
    let mut value = 0_u8;
    for &byte in bytes {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit)?;
    }

    Some(value)
}
