//! Module: index::key::ordered::normalize
//! Responsibility: normalized payload encoders for complex numeric domains.
//! Does not own: cross-kind canonical tagging.
//! Boundary: internal helper for ordered component encoding.

#[cfg(test)]
mod tests;

use crate::{
    db::index::key::ordered::{
        NEGATIVE_MARKER, OrderedValueEncodeError, POSITIVE_MARKER, ZERO_MARKER,
        segments::{encode_segment_len, push_inverted},
        semantics::ordered_i32_bytes,
    },
    types::{Decimal, IntBig, NatBig},
};

const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;
const BIGINT_DECIMAL_CHUNK_BASE: u64 = 1_000_000_000;
const BIGINT_DECIMAL_CHUNK_WIDTH: usize = 9;

pub(super) const DECIMAL_POSITIVE_TERMINATOR: u8 = 0x00;
pub(super) const DECIMAL_NEGATIVE_TERMINATOR: u8 = 0xFF;

/// Decimal ordering is sign bucket + exponent + significant digits + terminator.
pub(super) fn push_decimal_payload(
    out: &mut Vec<u8>,
    value: Decimal,
) -> Result<(), OrderedValueEncodeError> {
    let normalized = value.normalize();
    if normalized.is_zero() {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let decimal_parts = normalized.parts();
    let mut digits_buf = [0u8; DECIMAL_DIGIT_BUFFER_LEN];
    let mantissa = decimal_parts.mantissa();
    let digit_len = write_u128_decimal_digits(mantissa.unsigned_abs(), &mut digits_buf);
    let exponent = decimal_exponent(decimal_parts.scale(), digit_len)?;

    let exponent_bytes = ordered_i32_bytes(exponent);
    let digits_bytes = &digits_buf[..digit_len];

    if mantissa.is_negative() {
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
        out[write_idx] = digit_to_ascii(u32::try_from(remainder).unwrap_or_default());
        value /= 10;

        if value == 0 {
            break;
        }
    }

    let len = DECIMAL_DIGIT_BUFFER_LEN.saturating_sub(write_idx);
    out.copy_within(write_idx..DECIMAL_DIGIT_BUFFER_LEN, 0);
    len
}

/// `Value::IntBig` ordering uses sign bucket + digit length + digit bytes.
pub(super) fn push_signed_big_integer_payload(
    out: &mut Vec<u8>,
    value: &IntBig,
) -> Result<(), OrderedValueEncodeError> {
    let (negative, limbs) = value.sign_and_u32_digits();
    let chunks = u32_limbs_to_decimal_chunks(limbs);

    if chunks.is_empty() {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let digit_count = decimal_chunk_digit_count(&chunks)?;
    let digits_len = encode_segment_len(digit_count)?;
    // Validate length before allocating the final payload. Chunks write directly
    // into this destination; there is no intermediate ASCII buffer to copy.
    out.reserve_exact(1 + digits_len.len() + digit_count);

    if negative {
        out.push(NEGATIVE_MARKER);
        push_inverted(out, &digits_len);
    } else {
        out.push(POSITIVE_MARKER);
        out.extend_from_slice(&digits_len);
    }
    push_decimal_chunk_digits(out, &chunks, negative);

    Ok(())
}

/// `Value::NatBig` ordering is length + digit bytes.
pub(super) fn push_unsigned_big_integer_payload(
    out: &mut Vec<u8>,
    value: &NatBig,
) -> Result<(), OrderedValueEncodeError> {
    let chunks = u32_limbs_to_decimal_chunks(value.u32_digits());

    let digit_count = decimal_chunk_digit_count(&chunks)?;
    let digits_len = encode_segment_len(digit_count)?;
    out.reserve_exact(digits_len.len() + digit_count);
    out.extend_from_slice(&digits_len);
    push_decimal_chunk_digits(out, &chunks, false);

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

/// Convert little-endian base-2^32 limbs into little-endian base-1e9 chunks.
/// Empty chunks represent zero; both integer families share this conversion.
fn u32_limbs_to_decimal_chunks(mut quotient: Vec<u32>) -> Vec<u32> {
    trim_zero_limbs(&mut quotient);
    if quotient.is_empty() {
        return Vec::new();
    }

    // base-2^32 and base-1e9 are close in radix width, so chunks are roughly
    // one-to-one with limbs; reserve once to reduce allocator churn.
    let mut chunks = Vec::with_capacity(quotient.len().saturating_add(1));
    while !quotient.is_empty() {
        let mut remainder = 0u64;
        for limb in quotient.iter_mut().rev() {
            let value = (remainder << 32) | u64::from(*limb);
            let quotient_limb = value / BIGINT_DECIMAL_CHUNK_BASE;
            *limb = u32::try_from(quotient_limb).unwrap_or_default();
            remainder = value % BIGINT_DECIMAL_CHUNK_BASE;
        }

        chunks.push(u32::try_from(remainder).unwrap_or_default());
        trim_zero_limbs(&mut quotient);
    }

    chunks
}

fn trim_zero_limbs(limbs: &mut Vec<u32>) {
    while limbs.last().copied() == Some(0) {
        limbs.pop();
    }
}

// The digit length follows from chunk count plus at most nine leading digits;
// no digit traversal or temporary text allocation is needed for admission.
fn decimal_chunk_digit_count(chunks: &[u32]) -> Result<usize, OrderedValueEncodeError> {
    let Some(&leading) = chunks.last() else {
        return Ok(1);
    };
    (chunks.len() - 1)
        .checked_mul(BIGINT_DECIMAL_CHUNK_WIDTH)
        .and_then(|padded| padded.checked_add(decimal_chunk_width(leading)))
        .ok_or(OrderedValueEncodeError::SegmentTooLarge)
}

fn push_decimal_chunk_digits(out: &mut Vec<u8>, chunks: &[u32], inverted: bool) {
    let Some((&leading, remaining)) = chunks.split_last() else {
        out.push(if inverted { !b'0' } else { b'0' });
        return;
    };
    push_chunk_digits(out, leading, decimal_chunk_width(leading), inverted);
    for &chunk in remaining.iter().rev() {
        push_chunk_digits(out, chunk, BIGINT_DECIMAL_CHUNK_WIDTH, inverted);
    }
}

fn decimal_chunk_width(chunk: u32) -> usize {
    chunk.checked_ilog10().unwrap_or(0) as usize + 1
}

// A fixed stack scratch covers both the unpadded leading chunk and all padded
// chunks. Negative signed values invert these same bytes, preserving ordering.
fn push_chunk_digits(out: &mut Vec<u8>, mut chunk: u32, width: usize, inverted: bool) {
    let mut scratch = [b'0'; BIGINT_DECIMAL_CHUNK_WIDTH];
    for digit in scratch[..width].iter_mut().rev() {
        *digit = digit_to_ascii(chunk % 10);
        chunk /= 10;
    }
    if inverted {
        push_inverted(out, &scratch[..width]);
    } else {
        out.extend_from_slice(&scratch[..width]);
    }
}

fn digit_to_ascii(value: u32) -> u8 {
    const DECIMAL_DIGITS: [u8; 10] = *b"0123456789";

    debug_assert!(value <= 9, "decimal digit must be in 0..=9");
    let index = usize::try_from(value).unwrap_or_default();

    DECIMAL_DIGITS.get(index).copied().unwrap_or(b'0')
}
