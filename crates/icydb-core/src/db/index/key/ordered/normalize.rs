//! Module: index::key::ordered::normalize
//! Responsibility: normalized payload encoders for complex numeric domains.
//! Does not own: cross-kind canonical tagging.
//! Boundary: internal helper for ordered component encoding.

use crate::{
    db::index::key::ordered::{
        NEGATIVE_MARKER, OrderedValueEncodeError, POSITIVE_MARKER, ZERO_MARKER,
        parts::{encode_segment_len, push_inverted},
        semantics::ordered_i32_bytes,
    },
    types::{Decimal, Int, Nat},
};

const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;
const BIGINT_DECIMAL_CHUNK_BASE: u64 = 1_000_000_000;
const BIGINT_DECIMAL_CHUNK_WIDTH: usize = 9;

pub(super) const DECIMAL_POSITIVE_TERMINATOR: u8 = 0x00;
pub(super) const DECIMAL_NEGATIVE_TERMINATOR: u8 = 0xFF;

// Decimal ordering is sign bucket + exponent + significant digits + terminator.
pub(super) fn push_decimal_payload(
    out: &mut Vec<u8>,
    value: Decimal,
) -> Result<(), OrderedValueEncodeError> {
    let normalized = value.normalize();
    if normalized.is_zero() {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let parts = normalized.parts();
    let mut digits_buf = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
    let digit_len = write_u128_decimal_digits(parts.mantissa.unsigned_abs(), &mut digits_buf);
    let exponent = decimal_exponent(parts.scale, digit_len)?;

    let exponent_bytes = ordered_i32_bytes(exponent);
    let digits_bytes = &digits_buf[..digit_len];

    if parts.mantissa.is_negative() {
        out.push(NEGATIVE_MARKER);
        push_inverted(out, &exponent_bytes);
        push_inverted(out, digits_bytes);
        out.push(DECIMAL_NEGATIVE_TERMINATOR);
    } else {
        out.push(POSITIVE_MARKER);
        out.extend_from_slice(&exponent_bytes);
        out.extend_from_slice(digits_bytes);
        out.push(DECIMAL_POSITIVE_TERMINATOR);
    }

    Ok(())
}

fn write_u128_decimal_digits(mut value: u128, out: &mut [u8; DECIMAL_DIGIT_BUFFER_LEN]) -> usize {
    let mut write_idx = DECIMAL_DIGIT_BUFFER_LEN;

    loop {
        write_idx = write_idx.saturating_sub(1);
        let remainder = value % 10;
        debug_assert!(remainder <= 9, "decimal digit remainder must be in 0..=9");
        out[write_idx] =
            digit_to_ascii(u32::try_from(remainder).expect("decimal remainder should fit u32"));
        value /= 10;

        if value == 0 {
            break;
        }
    }

    let len = DECIMAL_DIGIT_BUFFER_LEN.saturating_sub(write_idx);
    out.copy_within(write_idx..DECIMAL_DIGIT_BUFFER_LEN, 0);
    len
}

// Signed big-int ordering uses sign bucket + digit length + digit bytes.
pub(super) fn push_signed_bigint_payload(
    out: &mut Vec<u8>,
    value: &Int,
) -> Result<(), OrderedValueEncodeError> {
    let (negative, limbs) = value.sign_and_u32_digits();
    let digits = u32_limbs_to_decimal_digits(limbs);

    if digits.len() == 1 && digits[0] == b'0' {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let digits_len = encode_segment_len(digits.len())?;

    if negative {
        out.push(NEGATIVE_MARKER);
        push_inverted(out, &digits_len);
        push_inverted(out, &digits);
    } else {
        out.push(POSITIVE_MARKER);
        out.extend_from_slice(&digits_len);
        out.extend_from_slice(&digits);
    }

    Ok(())
}

// Unsigned big-int ordering is length + digit bytes.
pub(super) fn push_unsigned_bigint_payload(
    out: &mut Vec<u8>,
    value: &Nat,
) -> Result<(), OrderedValueEncodeError> {
    let digits = u32_limbs_to_decimal_digits(value.u32_digits());

    let digits_len = encode_segment_len(digits.len())?;
    out.extend_from_slice(&digits_len);
    out.extend_from_slice(&digits);

    Ok(())
}

fn decimal_exponent(scale: u32, digit_len: usize) -> Result<i32, OrderedValueEncodeError> {
    if scale > Decimal::max_supported_scale() {
        return Err(OrderedValueEncodeError::DecimalExponentOverflow);
    }

    let digit_count =
        u32::try_from(digit_len).map_err(|_| OrderedValueEncodeError::DecimalExponentOverflow)?;
    let normalized_digits = digit_count
        .checked_sub(1)
        .ok_or(OrderedValueEncodeError::DecimalExponentOverflow)?;

    let exponent = i64::from(normalized_digits)
        .checked_sub(i64::from(scale))
        .ok_or(OrderedValueEncodeError::DecimalExponentOverflow)?;

    i32::try_from(exponent).map_err(|_| OrderedValueEncodeError::DecimalExponentOverflow)
}

// Convert little-endian base-2^32 limbs to ASCII decimal digits.
// This avoids decimal String formatting but still uses temporary vectors.
fn u32_limbs_to_decimal_digits(mut quotient: Vec<u32>) -> Vec<u8> {
    trim_zero_limbs(&mut quotient);
    if quotient.is_empty() {
        return vec![b'0'];
    }

    // base-2^32 and base-1e9 are close in radix width, so chunks are roughly
    // one-to-one with limbs; reserve once to reduce allocator churn.
    let mut chunks = Vec::with_capacity(quotient.len().saturating_add(1));
    while !quotient.is_empty() {
        let mut remainder = 0u64;
        for limb in quotient.iter_mut().rev() {
            let value = (remainder << 32) | u64::from(*limb);
            let quotient_limb = value / BIGINT_DECIMAL_CHUNK_BASE;
            *limb = u32::try_from(quotient_limb).expect("quotient limb always fits in u32");
            remainder = value % BIGINT_DECIMAL_CHUNK_BASE;
        }

        chunks.push(u32::try_from(remainder).expect("remainder always fits in u32"));
        trim_zero_limbs(&mut quotient);
    }

    chunks_to_decimal_digits(chunks)
}

fn trim_zero_limbs(limbs: &mut Vec<u32>) {
    while limbs.last().copied() == Some(0) {
        limbs.pop();
    }
}

fn chunks_to_decimal_digits(mut chunks: Vec<u32>) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunks.len().saturating_mul(BIGINT_DECIMAL_CHUNK_WIDTH));

    if let Some(most_significant) = chunks.pop() {
        push_unpadded_chunk_digits(&mut out, most_significant);
    } else {
        out.push(b'0');
        return out;
    }

    while let Some(chunk) = chunks.pop() {
        push_padded_chunk_digits(&mut out, chunk);
    }

    out
}

fn push_unpadded_chunk_digits(out: &mut Vec<u8>, chunk: u32) {
    let mut scratch = [0u8; BIGINT_DECIMAL_CHUNK_WIDTH];
    let mut write_idx = BIGINT_DECIMAL_CHUNK_WIDTH;
    let mut value = chunk;

    loop {
        write_idx = write_idx.saturating_sub(1);
        scratch[write_idx] = digit_to_ascii(value % 10);
        value /= 10;
        if value == 0 {
            break;
        }
    }

    out.extend_from_slice(&scratch[write_idx..BIGINT_DECIMAL_CHUNK_WIDTH]);
}

fn push_padded_chunk_digits(out: &mut Vec<u8>, chunk: u32) {
    let mut divisor = 100_000_000u32;
    for _ in 0..BIGINT_DECIMAL_CHUNK_WIDTH {
        out.push(digit_to_ascii((chunk / divisor) % 10));
        divisor = if divisor > 1 { divisor / 10 } else { 1 };
    }
}

fn digit_to_ascii(value: u32) -> u8 {
    const DECIMAL_DIGITS: [u8; 10] = *b"0123456789";

    debug_assert!(value <= 9, "decimal digit must be in 0..=9");
    let index = usize::try_from(value).expect("decimal digit should fit usize");

    DECIMAL_DIGITS.get(index).copied().unwrap_or(b'0')
}
