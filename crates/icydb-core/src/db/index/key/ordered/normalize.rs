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

/// `Value::IntBig` ordering uses a sign bucket and a binary magnitude.
pub(super) fn push_signed_big_integer_payload(
    out: &mut Vec<u8>,
    value: &IntBig,
) -> Result<(), OrderedValueEncodeError> {
    let (negative, digits) = value.sign_and_u32_digits();
    push_big_integer_magnitude(out, Some(negative), digits)
}

/// `Value::NatBig` ordering uses byte length followed by big-endian magnitude.
pub(super) fn push_unsigned_big_integer_payload(
    out: &mut Vec<u8>,
    value: &NatBig,
) -> Result<(), OrderedValueEncodeError> {
    push_big_integer_magnitude(out, None, value.u32_digits())
}

// Equal-length big-endian magnitudes compare numerically. Inverting both length
// and magnitude reverses that order for negatives. Borrowed canonical limbs
// write directly to the destination without decimal chunks or a magnitude Vec.
fn push_big_integer_magnitude(
    out: &mut Vec<u8>,
    negative: Option<bool>,
    mut digits: impl DoubleEndedIterator<Item = u32> + ExactSizeIterator,
) -> Result<(), OrderedValueEncodeError> {
    let high = digits.next_back();
    let high_len = high.map_or(0, |digit| {
        (u32::BITS - digit.leading_zeros()).div_ceil(8) as usize
    });
    let len = digits
        .len()
        .checked_mul(4)
        .and_then(|len| len.checked_add(high_len))
        .ok_or(OrderedValueEncodeError::SegmentTooLarge)?;
    if negative.is_some() && len == 0 {
        out.push(ZERO_MARKER);
        return Ok(());
    }
    let length = encode_segment_len(len)?;
    out.reserve_exact(usize::from(negative.is_some()) + length.len() + len);
    if let Some(negative) = negative {
        out.push(if negative {
            NEGATIVE_MARKER
        } else {
            POSITIVE_MARKER
        });
    }
    let mask = if negative == Some(true) { u8::MAX } else { 0 };
    out.extend(length.map(|byte| byte ^ mask));
    if let Some(high) = high {
        // A u32 high limb contributes at most its four initialized bytes.
        out.extend(
            high.to_be_bytes()[4 - high_len..]
                .iter()
                .map(|byte| byte ^ mask),
        );
    }
    for digit in digits.rev() {
        out.extend(digit.to_be_bytes().map(|byte| byte ^ mask));
    }
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

fn digit_to_ascii(value: u32) -> u8 {
    const DECIMAL_DIGITS: [u8; 10] = *b"0123456789";

    debug_assert!(value <= 9, "decimal digit must be in 0..=9");
    let index = usize::try_from(value).unwrap_or_default();

    DECIMAL_DIGITS.get(index).copied().unwrap_or(b'0')
}
