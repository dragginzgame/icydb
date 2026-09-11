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
    value::decimal::{digit_count, signed_chunks, unsigned_chunks, visit_digits},
};
use std::convert::Infallible;

const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;

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
    // Encoding retains its current infallible construction policy. The shared
    // converter still requires an observer; R5 owns budget propagation here.
    let (negative, chunks) =
        signed_chunks(value, |_, _| Ok::<_, Infallible>(())).unwrap_or_else(|never| match never {});

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
    let chunks = unsigned_chunks(value, |_, _| Ok::<_, Infallible>(()))
        .unwrap_or_else(|never| match never {});

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

// The shared magnitude owner counts digits without constructing output text.
fn decimal_chunk_digit_count(chunks: &[u32]) -> Result<usize, OrderedValueEncodeError> {
    digit_count(chunks).ok_or(OrderedValueEncodeError::SegmentTooLarge)
}

fn push_decimal_chunk_digits(out: &mut Vec<u8>, chunks: &[u32], inverted: bool) {
    visit_digits(chunks, |digits| {
        if inverted {
            push_inverted(out, digits);
        } else {
            out.extend_from_slice(digits);
        }
        Ok::<_, Infallible>(())
    })
    .unwrap_or_else(|never| match never {});
}

fn digit_to_ascii(value: u32) -> u8 {
    const DECIMAL_DIGITS: [u8; 10] = *b"0123456789";

    debug_assert!(value <= 9, "decimal digit must be in 0..=9");
    let index = usize::try_from(value).unwrap_or_default();

    DECIMAL_DIGITS.get(index).copied().unwrap_or(b'0')
}
