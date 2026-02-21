use crate::{
    error::InternalError,
    types::{
        Account, Date, Decimal, Duration, Int, Int128, Nat, Nat128, Principal, Repr, Timestamp,
    },
    value::{Value, ValueEnum},
};
use thiserror::Error as ThisError;

const NEGATIVE_MARKER: u8 = 0x00;
const ZERO_MARKER: u8 = 0x01;
const POSITIVE_MARKER: u8 = 0x02;

const LENGTH_BYTES: usize = 2;
const MAX_SEGMENT_LEN: usize = u16::MAX as usize;
const DECIMAL_DIGIT_BUFFER_LEN: usize = 39;
const BIGINT_DECIMAL_CHUNK_BASE: u64 = 1_000_000_000;
const BIGINT_DECIMAL_CHUNK_WIDTH: usize = 9;
const DECIMAL_POSITIVE_TERMINATOR: u8 = 0x00;
const DECIMAL_NEGATIVE_TERMINATOR: u8 = 0xFF;
const ACCOUNT_OWNER_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
const ACCOUNT_SUBACCOUNT_LEN: usize = 32;
const ACCOUNT_SUBACCOUNT_TAG: u8 = 0x80;

///
/// OrderedValueEncodeError
///
/// Canonical index-encoding failures for one `Value` component.
///

#[derive(Debug, ThisError)]
pub(crate) enum OrderedValueEncodeError {
    #[error("null values are not indexable")]
    NullNotIndexable,

    #[error("value kind '{kind}' is not canonically index-orderable")]
    UnsupportedValueKind { kind: &'static str },

    #[error("ordered segment exceeds max length: {len} bytes (limit {max})")]
    SegmentTooLarge { len: usize, max: usize },

    #[error("decimal exponent overflow during canonical encoding")]
    DecimalExponentOverflow,
}

impl From<OrderedValueEncodeError> for InternalError {
    fn from(err: OrderedValueEncodeError) -> Self {
        Self::index_unsupported(format!(
            "index value is not canonically order-encodable: {err}"
        ))
    }
}

///
/// EncodedValue
///
/// Cached canonical index-component bytes for one logical `Value`.
/// This wrapper keeps value + bytes together so planning/execution callsites
/// can avoid re-encoding the same literal repeatedly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedValue {
    raw: Value,
    encoded: Vec<u8>,
}

impl EncodedValue {
    /// Encode a value once into canonical index-component bytes.
    pub(crate) fn try_new(raw: Value) -> Result<Self, OrderedValueEncodeError> {
        let encoded = encode_canonical_index_component(&raw)?;

        Ok(Self { raw, encoded })
    }

    /// Encode a borrowed value by cloning it into this cached wrapper.
    pub(crate) fn try_from_ref(raw: &Value) -> Result<Self, OrderedValueEncodeError> {
        Self::try_new(raw.clone())
    }

    /// Encode all values in order into cached wrappers.
    pub(crate) fn try_encode_all(values: &[Value]) -> Result<Vec<Self>, OrderedValueEncodeError> {
        values.iter().map(Self::try_from_ref).collect()
    }

    #[must_use]
    pub(crate) const fn encoded(&self) -> &[u8] {
        self.encoded.as_slice()
    }
}

/// OrderedEncode
///
/// Internal ordered-byte encoder for fixed-width value components.
pub(crate) trait OrderedEncode {
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

/// Encode one scalar index component so lexicographic byte order matches
/// canonical `Value` order for supported primitive variants.
pub(crate) fn encode_canonical_index_component(
    value: &Value,
) -> Result<Vec<u8>, OrderedValueEncodeError> {
    let mut out = Vec::new();

    out.push(value.canonical_tag().to_u8());
    encode_component_payload(&mut out, value)?;

    Ok(out)
}

// Encode the variant-local payload after the canonical variant tag.
fn encode_component_payload(
    out: &mut Vec<u8>,
    value: &Value,
) -> Result<(), OrderedValueEncodeError> {
    match value {
        Value::Account(v) => {
            push_account_payload(out, v);
            Ok(())
        }
        Value::Blob(_) | Value::List(_) | Value::Map(_) => {
            Err(OrderedValueEncodeError::UnsupportedValueKind {
                kind: value.canonical_tag().label(),
            })
        }
        Value::Bool(v) => {
            out.push(u8::from(*v));
            Ok(())
        }
        Value::Date(v) => v.encode_ordered(out),
        Value::Decimal(v) => push_decimal_payload(out, *v),
        Value::Duration(v) => v.encode_ordered(out),
        Value::Enum(v) => push_enum_payload(out, v),
        Value::Float32(v) => {
            out.extend_from_slice(&ordered_f32_bytes(v.get()));
            Ok(())
        }
        Value::Float64(v) => {
            out.extend_from_slice(&ordered_f64_bytes(v.get()));
            Ok(())
        }
        Value::Int(v) => {
            out.extend_from_slice(&ordered_i64_bytes(*v));
            Ok(())
        }
        Value::Int128(v) => v.encode_ordered(out),
        Value::IntBig(v) => push_signed_bigint_payload(out, v),
        Value::Null => Err(OrderedValueEncodeError::NullNotIndexable),
        Value::Principal(v) => {
            push_terminated_bytes(out, v.as_slice());
            Ok(())
        }
        Value::Subaccount(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        Value::Text(v) => {
            push_terminated_bytes(out, v.as_bytes());
            Ok(())
        }
        Value::Timestamp(v) => v.encode_ordered(out),
        Value::Uint(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        Value::Uint128(v) => v.encode_ordered(out),
        Value::UintBig(v) => push_unsigned_bigint_payload(out, v),
        Value::Ulid(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        Value::Unit => Ok(()),
    }
}

// Account ordering uses the same tuple contract as `Account::cmp`.
#[expect(clippy::cast_possible_truncation)]
fn push_account_payload(out: &mut Vec<u8>, account: &Account) {
    let owner = account.owner.as_slice();
    let owner_len = owner.len().min(ACCOUNT_OWNER_MAX_LEN).min(u8::MAX as usize);

    let mut ordering_tag = owner_len as u8;
    if account.subaccount.is_some() {
        ordering_tag |= ACCOUNT_SUBACCOUNT_TAG;
    }

    out.push(ordering_tag);

    let mut owner_padded = [0u8; ACCOUNT_OWNER_MAX_LEN];
    owner_padded[..owner_len].copy_from_slice(&owner[..owner_len]);
    out.extend_from_slice(&owner_padded);

    let subaccount = account.subaccount.unwrap_or_default().to_array();
    let _ = ACCOUNT_SUBACCOUNT_LEN;
    out.extend_from_slice(&subaccount);
}

// Enum ordering is variant -> path option -> payload option, recursively.
fn push_enum_payload(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), OrderedValueEncodeError> {
    push_terminated_bytes(out, value.variant.as_bytes());

    match &value.path {
        Some(path) => {
            out.push(1);
            push_terminated_bytes(out, path.as_bytes());
        }
        None => out.push(0),
    }

    match &value.payload {
        Some(payload) => {
            out.push(1);

            let payload_bytes = encode_canonical_index_component(payload)?;
            push_len_prefixed_bytes(out, &payload_bytes)?;
        }
        None => out.push(0),
    }

    Ok(())
}

// Decimal ordering is sign bucket + exponent + significant digits + terminator.
fn push_decimal_payload(out: &mut Vec<u8>, value: Decimal) -> Result<(), OrderedValueEncodeError> {
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
        out[write_idx] = match value % 10 {
            0 => b'0',
            1 => b'1',
            2 => b'2',
            3 => b'3',
            4 => b'4',
            5 => b'5',
            6 => b'6',
            7 => b'7',
            8 => b'8',
            9 => b'9',
            _ => unreachable!("decimal digit remainder must be in 0..=9"),
        };
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
fn push_signed_bigint_payload(
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
fn push_unsigned_bigint_payload(
    out: &mut Vec<u8>,
    value: &Nat,
) -> Result<(), OrderedValueEncodeError> {
    let digits = u32_limbs_to_decimal_digits(value.u32_digits());

    let digits_len = encode_segment_len(digits.len())?;
    out.extend_from_slice(&digits_len);
    out.extend_from_slice(&digits);

    Ok(())
}

// Byte strings are escaped so tuple boundaries remain unambiguous.
// Segment size bounds for these terminated payloads are enforced by the outer
// index-key component caps in `IndexKey`, not at this primitive encoder layer.
fn push_terminated_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        if byte == 0 {
            out.extend_from_slice(&[0, 0xFF]);
        } else {
            out.push(byte);
        }
    }

    out.extend_from_slice(&[0, 0]);
}

fn push_len_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), OrderedValueEncodeError> {
    let len = encode_segment_len(bytes.len())?;
    out.extend_from_slice(&len);
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_inverted(out: &mut Vec<u8>, bytes: &[u8]) {
    for &byte in bytes {
        out.push(!byte);
    }
}

fn encode_segment_len(len: usize) -> Result<[u8; LENGTH_BYTES], OrderedValueEncodeError> {
    let len_u16 = u16::try_from(len).map_err(|_| OrderedValueEncodeError::SegmentTooLarge {
        len,
        max: MAX_SEGMENT_LEN,
    })?;

    Ok(len_u16.to_be_bytes())
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
    match value {
        0 => b'0',
        1 => b'1',
        2 => b'2',
        3 => b'3',
        4 => b'4',
        5 => b'5',
        6 => b'6',
        7 => b'7',
        8 => b'8',
        9 => b'9',
        _ => unreachable!("decimal digit must be in 0..=9"),
    }
}

const fn ordered_i32_bytes(value: i32) -> [u8; 4] {
    let biased = value.cast_unsigned() ^ (1u32 << 31);
    biased.to_be_bytes()
}

const fn ordered_i64_bytes(value: i64) -> [u8; 8] {
    let biased = value.cast_unsigned() ^ (1u64 << 63);
    biased.to_be_bytes()
}

const fn ordered_i128_bytes(value: i128) -> [u8; 16] {
    let biased = value.cast_unsigned() ^ (1u128 << 127);
    biased.to_be_bytes()
}

const fn ordered_f32_bytes(value: f32) -> [u8; 4] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000 == 0 {
        bits ^ 0x8000_0000
    } else {
        !bits
    };

    ordered.to_be_bytes()
}

const fn ordered_f64_bytes(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000_0000_0000 == 0 {
        bits ^ 0x8000_0000_0000_0000
    } else {
        !bits
    };

    ordered.to_be_bytes()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        NEGATIVE_MARKER, OrderedValueEncodeError, POSITIVE_MARKER, ZERO_MARKER,
        encode_canonical_index_component,
    };
    use crate::{
        types::{
            Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
            Principal, Subaccount, Timestamp, Ulid,
        },
        value::{Value, ValueEnum},
    };
    use proptest::prelude::*;
    use std::cmp::Ordering;

    fn assert_encoded_order(left: Value, right: Value, expected: Ordering) {
        let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(&right).expect("right should encode");

        assert_eq!(left_bytes.cmp(&right_bytes), expected);
    }

    fn normalize_unsigned_decimal_text(raw: &str) -> String {
        let trimmed = raw.trim_start_matches('0');
        if trimmed.is_empty() {
            return "0".to_string();
        }

        trimmed.to_string()
    }

    fn normalize_signed_decimal_text(raw: &str) -> String {
        if let Some(unsigned) = raw.strip_prefix('-') {
            let digits = normalize_unsigned_decimal_text(unsigned);
            if digits == "0" {
                return digits;
            }

            return format!("-{digits}");
        }

        normalize_unsigned_decimal_text(raw)
    }

    fn decode_int_big_payload(encoded: &[u8]) -> (u8, Vec<u8>) {
        assert!(
            encoded.len() >= 2,
            "int-big payload must include tag + marker: {encoded:?}"
        );
        assert_eq!(
            encoded[0],
            Value::IntBig(Int::from(0)).canonical_tag().to_u8()
        );

        let marker = encoded[1];
        match marker {
            ZERO_MARKER => {
                assert_eq!(
                    encoded.len(),
                    2,
                    "zero int-big payload must be exactly two bytes: {encoded:?}"
                );
                (marker, vec![b'0'])
            }
            POSITIVE_MARKER => {
                assert!(
                    encoded.len() >= 4,
                    "positive int-big payload must include length: {encoded:?}"
                );
                let len = usize::from(u16::from_be_bytes([encoded[2], encoded[3]]));
                let digits = encoded[4..].to_vec();
                assert_eq!(len, digits.len(), "int-big length mismatch");
                (marker, digits)
            }
            NEGATIVE_MARKER => {
                assert!(
                    encoded.len() >= 4,
                    "negative int-big payload must include length: {encoded:?}"
                );
                let len = usize::from(u16::from_be_bytes([!encoded[2], !encoded[3]]));
                let digits: Vec<u8> = encoded[4..].iter().map(|byte| !byte).collect();
                assert_eq!(len, digits.len(), "int-big length mismatch");
                (marker, digits)
            }
            other => panic!("unexpected int-big marker {other:#x}"),
        }
    }

    fn decode_uint_big_payload(encoded: &[u8]) -> Vec<u8> {
        assert!(
            encoded.len() >= 3,
            "uint-big payload must include tag + length: {encoded:?}"
        );
        assert_eq!(
            encoded[0],
            Value::UintBig(Nat::from(0u64)).canonical_tag().to_u8()
        );

        let len = usize::from(u16::from_be_bytes([encoded[1], encoded[2]]));
        let digits = encoded[3..].to_vec();
        assert_eq!(len, digits.len(), "uint-big length mismatch");
        digits
    }

    fn decode_int_big_from_ordered_bytes(encoded: &[u8]) -> Int {
        let (marker, digits) = decode_int_big_payload(encoded);
        let digits_text = String::from_utf8(digits).expect("int-big digits must be utf8");

        let text = match marker {
            ZERO_MARKER | POSITIVE_MARKER => digits_text,
            NEGATIVE_MARKER => format!("-{digits_text}"),
            _ => unreachable!("unexpected int-big marker"),
        };

        text.parse().expect("decoded int-big text should parse")
    }

    fn decode_uint_big_from_ordered_bytes(encoded: &[u8]) -> Nat {
        let digits = decode_uint_big_payload(encoded);
        let digits_text = String::from_utf8(digits).expect("uint-big digits must be utf8");
        digits_text
            .parse()
            .expect("decoded uint-big text should parse")
    }

    #[test]
    fn canonical_encoder_rejects_non_indexable_and_unsupported_values() {
        assert!(encode_canonical_index_component(&Value::Null).is_err());
        assert!(encode_canonical_index_component(&Value::Blob(vec![1u8, 2u8])).is_err());
        assert!(encode_canonical_index_component(&Value::List(vec![Value::Int(1)])).is_err());
        assert!(encode_canonical_index_component(&Value::Map(vec![])).is_err());
    }

    #[test]
    fn canonical_encoder_respects_numeric_order_for_scalars() {
        assert_encoded_order(Value::Int(-2), Value::Int(7), Ordering::Less);
        assert_encoded_order(
            Value::Int128(Int128::from(-2i128)),
            Value::Int128(Int128::from(7i128)),
            Ordering::Less,
        );
        assert_encoded_order(Value::Uint(2), Value::Uint(7), Ordering::Less);
        assert_encoded_order(
            Value::Uint128(Nat128::from(2u128)),
            Value::Uint128(Nat128::from(7u128)),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::IntBig(Int::from(-20i32)),
            Value::IntBig(Int::from(-7i32)),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::UintBig(Nat::from(20u64)),
            Value::UintBig(Nat::from(700u64)),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::IntBig(Int::from(-1i32)),
            Value::IntBig(Int::from(0i32)),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::IntBig(Int::from(0i32)),
            Value::IntBig(Int::from(1i32)),
            Ordering::Less,
        );
    }

    #[test]
    fn canonical_encoder_bigint_same_value_same_bytes_across_construction_paths() {
        let int_cases = vec![
            (
                Value::IntBig(Int::from(70i32)),
                Value::IntBig("00070".parse().expect("int literal")),
            ),
            (
                Value::IntBig(Int::from(-70i32)),
                Value::IntBig("-00070".parse().expect("int literal")),
            ),
            (
                Value::IntBig(Int::from(0i32)),
                Value::IntBig("-0".parse().expect("int literal")),
            ),
            (
                Value::IntBig("4294967296".parse().expect("int literal")),
                Value::IntBig("00004294967296".parse().expect("int literal")),
            ),
        ];

        for (left, right) in int_cases {
            let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
            let right_bytes =
                encode_canonical_index_component(&right).expect("right should encode");
            assert_eq!(left_bytes, right_bytes, "int-big canonical bytes diverged");
        }

        let uint_cases = vec![
            (
                Value::UintBig(Nat::from(70u64)),
                Value::UintBig("00070".parse().expect("nat literal")),
            ),
            (
                Value::UintBig("4294967296".parse().expect("nat literal")),
                Value::UintBig("00004294967296".parse().expect("nat literal")),
            ),
            (
                Value::UintBig("18446744073709551616".parse().expect("nat literal")),
                Value::UintBig("000018446744073709551616".parse().expect("nat literal")),
            ),
        ];

        for (left, right) in uint_cases {
            let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
            let right_bytes =
                encode_canonical_index_component(&right).expect("right should encode");
            assert_eq!(left_bytes, right_bytes, "uint-big canonical bytes diverged");
        }
    }

    #[test]
    fn canonical_encoder_int_big_negative_zero_collapses_to_zero_marker() {
        let zero = Value::IntBig(Int::from(0i32));
        let negative_zero = Value::IntBig("-0".parse().expect("int literal"));

        let zero_bytes = encode_canonical_index_component(&zero).expect("zero should encode");
        let negative_zero_bytes =
            encode_canonical_index_component(&negative_zero).expect("negative zero should encode");

        assert_eq!(zero_bytes, negative_zero_bytes);
        assert_eq!(zero_bytes, vec![zero.canonical_tag().to_u8(), ZERO_MARKER]);
    }

    #[test]
    fn canonical_encoder_bigint_payload_uses_minimal_digits() {
        let int_literals = vec![
            "-000123456789",
            "-18446744073709551616",
            "0",
            "0004294967296",
            "340282366920938463463374607431768211455",
        ];

        for literal in int_literals {
            let value = Value::IntBig(literal.parse().expect("int literal"));
            let encoded = encode_canonical_index_component(&value).expect("int-big should encode");
            let (_, digits) = decode_int_big_payload(&encoded);
            assert!(digits.iter().all(u8::is_ascii_digit));

            let expected = normalize_signed_decimal_text(literal);
            let expected_digits = expected.strip_prefix('-').unwrap_or(&expected);
            assert_eq!(digits, expected_digits.as_bytes());

            if digits != b"0" {
                assert_ne!(digits[0], b'0', "int-big payload must not lead with zero");
            }
        }

        let uint_literals = vec![
            "0",
            "000123456789",
            "4294967296",
            "00018446744073709551616",
            "340282366920938463463374607431768211455",
        ];

        for literal in uint_literals {
            let value = Value::UintBig(literal.parse().expect("nat literal"));
            let encoded = encode_canonical_index_component(&value).expect("uint-big should encode");
            let digits = decode_uint_big_payload(&encoded);
            assert!(digits.iter().all(u8::is_ascii_digit));

            let expected = normalize_unsigned_decimal_text(literal);
            assert_eq!(digits, expected.as_bytes());

            if digits != b"0" {
                assert_ne!(digits[0], b'0', "uint-big payload must not lead with zero");
            }
        }
    }

    #[test]
    fn canonical_encoder_bigint_limb_boundary_ordering_is_monotonic() {
        let one_limb_max = "4294967295";
        let two_limb_min = "4294967296";
        let two_limb_next = "4294967297";

        assert_encoded_order(
            Value::UintBig(one_limb_max.parse().expect("nat literal")),
            Value::UintBig(two_limb_min.parse().expect("nat literal")),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::UintBig(two_limb_min.parse().expect("nat literal")),
            Value::UintBig(two_limb_next.parse().expect("nat literal")),
            Ordering::Less,
        );

        assert_encoded_order(
            Value::IntBig(format!("-{two_limb_next}").parse().expect("int literal")),
            Value::IntBig(format!("-{two_limb_min}").parse().expect("int literal")),
            Ordering::Less,
        );
        assert_encoded_order(
            Value::IntBig(format!("-{two_limb_min}").parse().expect("int literal")),
            Value::IntBig(format!("-{one_limb_max}").parse().expect("int literal")),
            Ordering::Less,
        );
    }

    #[test]
    fn canonical_encoder_bigint_roundtrip_stable_for_valid_bytes() {
        let int_literals = vec![
            "-18446744073709551616",
            "-1",
            "0",
            "1",
            "340282366920938463463374607431768211455",
        ];

        for literal in int_literals {
            let value: Int = literal.parse().expect("int literal");
            let encoded =
                encode_canonical_index_component(&Value::IntBig(value.clone())).expect("encode");
            let decoded = decode_int_big_from_ordered_bytes(&encoded);
            assert_eq!(decoded, value, "int-big decode(encode(x)) mismatch");

            let reencoded =
                encode_canonical_index_component(&Value::IntBig(decoded)).expect("reencode");
            assert_eq!(reencoded, encoded, "int-big encode(decode(bytes)) mismatch");
        }

        let uint_literals = vec![
            "0",
            "1",
            "4294967296",
            "18446744073709551616",
            "340282366920938463463374607431768211455",
        ];

        for literal in uint_literals {
            let value: Nat = literal.parse().expect("nat literal");
            let encoded =
                encode_canonical_index_component(&Value::UintBig(value.clone())).expect("encode");
            let decoded = decode_uint_big_from_ordered_bytes(&encoded);
            assert_eq!(decoded, value, "uint-big decode(encode(x)) mismatch");

            let reencoded =
                encode_canonical_index_component(&Value::UintBig(decoded)).expect("reencode");
            assert_eq!(
                reencoded, encoded,
                "uint-big encode(decode(bytes)) mismatch"
            );
        }
    }

    #[test]
    fn canonical_encoder_respects_decimal_order_and_normalization() {
        let one = Value::Decimal(Decimal::new(1, 0));
        let one_point_zero = Value::Decimal(Decimal::new(10, 1));
        let one_point_one = Value::Decimal(Decimal::new(11, 1));

        let one_bytes = encode_canonical_index_component(&one).expect("one should encode");
        let one_point_zero_bytes = encode_canonical_index_component(&one_point_zero)
            .expect("one_point_zero should encode");
        let one_point_one_bytes =
            encode_canonical_index_component(&one_point_one).expect("one_point_one should encode");

        assert_eq!(one_bytes, one_point_zero_bytes);
        assert!(one_bytes < one_point_one_bytes);
    }

    #[test]
    fn canonical_encoder_supports_decimal_max_scale() {
        let max_scale = Decimal::max_supported_scale();
        let value = Value::Decimal(Decimal::new(1, max_scale));

        let encoded = encode_canonical_index_component(&value).expect("decimal max scale encodes");
        assert!(!encoded.is_empty());
    }

    #[test]
    fn canonical_encoder_rejects_decimal_scale_above_max() {
        let over_scale = Decimal::max_supported_scale().saturating_add(1);
        let value = Value::Decimal(Decimal::new_unchecked(1, over_scale));

        let err = encode_canonical_index_component(&value)
            .expect_err("unordered decimal scale should fail ordered encoding");
        assert!(matches!(
            err,
            OrderedValueEncodeError::DecimalExponentOverflow
        ));
    }

    #[test]
    #[allow(clippy::unreadable_literal)]
    fn canonical_encoder_decimal_large_mantissa_regression() {
        let lhs = Decimal::from_i128_with_scale(100000000000000003890313744798756555321, 2);
        let rhs = Decimal::from_i128_with_scale(158022371435723639313993729503199393550, 2);

        let lhs_value = Value::Decimal(lhs);
        let rhs_value = Value::Decimal(rhs);
        let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
        let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

        assert_eq!(
            lhs.cmp(&rhs),
            lhs_bytes.cmp(&rhs_bytes),
            "lhs={lhs:?} rhs={rhs:?} lhs_bytes={lhs_bytes:?} rhs_bytes={rhs_bytes:?}"
        );
    }

    #[test]
    fn canonical_encoder_respects_float_order() {
        let neg = Value::Float64(Float64::try_new(-1.5).expect("finite"));
        let zero = Value::Float64(Float64::try_new(0.0).expect("finite"));
        let pos = Value::Float64(Float64::try_new(1.5).expect("finite"));

        assert_encoded_order(neg, zero.clone(), Ordering::Less);
        assert_encoded_order(zero, pos, Ordering::Less);

        let neg32 = Value::Float32(Float32::try_new(-1.5).expect("finite"));
        let zero32 = Value::Float32(Float32::try_new(0.0).expect("finite"));
        let pos32 = Value::Float32(Float32::try_new(1.5).expect("finite"));

        assert_encoded_order(neg32, zero32.clone(), Ordering::Less);
        assert_encoded_order(zero32, pos32, Ordering::Less);
    }

    #[test]
    fn canonical_encoder_respects_text_and_identifier_order() {
        assert_encoded_order(
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
            Ordering::Less,
        );

        assert_encoded_order(
            Value::Principal(Principal::from_slice(&[1u8])),
            Value::Principal(Principal::from_slice(&[2u8])),
            Ordering::Less,
        );

        assert_encoded_order(
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(2)),
            Ordering::Less,
        );
    }

    #[test]
    fn canonical_encoder_respects_enum_order() {
        let left = Value::Enum(ValueEnum::new("A", Some("Path")));
        let right = Value::Enum(ValueEnum::new("B", Some("Path")));
        assert_encoded_order(left, right, Ordering::Less);

        let no_payload = Value::Enum(ValueEnum::new("A", Some("Path")));
        let with_payload =
            Value::Enum(ValueEnum::new("A", Some("Path")).with_payload(Value::Uint(1)));
        assert_encoded_order(no_payload, with_payload, Ordering::Less);
    }

    #[test]
    fn canonical_encoder_golden_vectors_freeze_primitive_bytes() {
        let cases: Vec<(&str, Value, Vec<u8>)> = vec![
            ("Bool(false)", Value::Bool(false), vec![0x03, 0x00]),
            ("Bool(true)", Value::Bool(true), vec![0x03, 0x01]),
            (
                "Int(-1)",
                Value::Int(-1),
                vec![0x0A, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                "Uint(1)",
                Value::Uint(1),
                vec![0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
            ),
            (
                "Decimal(10,1)",
                Value::Decimal(Decimal::new(10, 1)),
                vec![0x05, 0x02, 0x80, 0x00, 0x00, 0x00, 0x31, 0x00],
            ),
            (
                "Float64(-1.0)",
                Value::Float64(Float64::try_new(-1.0).expect("finite")),
                vec![0x09, 0x40, 0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                "Float64(0.0)",
                Value::Float64(Float64::try_new(0.0).expect("finite")),
                vec![0x09, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                "Text(\"a\")",
                Value::Text("a".to_string()),
                vec![0x12, b'a', 0x00, 0x00],
            ),
            (
                "Principal([1,0,2])",
                Value::Principal(Principal::from_slice(&[1u8, 0u8, 2u8])),
                vec![0x10, 0x01, 0x00, 0xFF, 0x02, 0x00, 0x00],
            ),
            (
                "IntBig(-7)",
                Value::IntBig(Int::from(-7i32)),
                vec![0x0C, 0x00, 0xFF, 0xFE, 0xC8],
            ),
            (
                "UintBig(70)",
                Value::UintBig(Nat::from(70u64)),
                vec![0x16, 0x00, 0x02, 0x37, 0x30],
            ),
            (
                "Enum(State::MyPath(7))",
                Value::Enum(ValueEnum::new("State", Some("MyPath")).with_payload(Value::Int(7))),
                vec![
                    0x07, b'S', b't', b'a', b't', b'e', 0x00, 0x00, 0x01, b'M', b'y', b'P', b'a',
                    b't', b'h', 0x00, 0x00, 0x01, 0x00, 0x09, 0x0A, 0x80, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x07,
                ],
            ),
            (
                "Ulid(1)",
                Value::Ulid(Ulid::from_u128(1)),
                vec![
                    0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x01,
                ],
            ),
            ("Unit", Value::Unit, vec![0x18]),
        ];

        for (name, value, expected) in cases {
            let actual = encode_canonical_index_component(&value)
                .expect("golden-vector sample should encode");
            assert_eq!(
                actual, expected,
                "golden vector drift for {name}: {value:?}"
            );
        }
    }

    #[test]
    fn canonical_encoder_total_order_matches_value_canonical_cmp_for_supported_samples() {
        let samples = vec![
            Value::Account(Account::dummy(1)),
            Value::Bool(false),
            Value::Bool(true),
            Value::Date(Date::new(2024, 1, 1)),
            Value::Decimal(Decimal::new(-11, 1)),
            Value::Decimal(Decimal::new(0, 0)),
            Value::Decimal(Decimal::new(11, 1)),
            Value::Duration(Duration::from_secs(1)),
            Value::Enum(ValueEnum::new("A", Some("E"))),
            Value::Float32(Float32::try_new(-1.0).expect("finite")),
            Value::Float32(Float32::try_new(1.0).expect("finite")),
            Value::Float64(Float64::try_new(-1.0).expect("finite")),
            Value::Float64(Float64::try_new(1.0).expect("finite")),
            Value::Int(-2),
            Value::Int(7),
            Value::Int128(Int128::from(-2i128)),
            Value::Int128(Int128::from(7i128)),
            Value::IntBig(Int::from(-7i32)),
            Value::IntBig(Int::from(7i32)),
            Value::Principal(Principal::from_slice(&[1u8])),
            Value::Principal(Principal::from_slice(&[2u8])),
            Value::Subaccount(Subaccount::dummy(1)),
            Value::Subaccount(Subaccount::dummy(2)),
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
            Value::Timestamp(Timestamp::from_secs(1)),
            Value::Timestamp(Timestamp::from_secs(2)),
            Value::Uint(1),
            Value::Uint(2),
            Value::Uint128(Nat128::from(1u128)),
            Value::Uint128(Nat128::from(2u128)),
            Value::UintBig(Nat::from(1u64)),
            Value::UintBig(Nat::from(2u64)),
            Value::Ulid(Ulid::from_u128(1)),
            Value::Ulid(Ulid::from_u128(2)),
            Value::Unit,
        ];

        let mut by_value = samples.clone();
        by_value.sort_by(Value::canonical_cmp_key);

        let mut by_bytes = samples;
        by_bytes.sort_by(|left, right| {
            let left_bytes = encode_canonical_index_component(left).expect("left should encode");
            let right_bytes = encode_canonical_index_component(right).expect("right should encode");
            left_bytes.cmp(&right_bytes)
        });

        assert_eq!(by_value, by_bytes);
    }

    #[test]
    fn timestamp_ordered_encoding_is_monotonic_for_millisecond_values() {
        let t1000 = Value::Timestamp(Timestamp::from_millis(1_000));
        let t1001 = Value::Timestamp(Timestamp::from_millis(1_001));

        let b1000 = encode_canonical_index_component(&t1000).expect("timestamp 1000 should encode");
        let b1001 = encode_canonical_index_component(&t1001).expect("timestamp 1001 should encode");

        assert!(b1001 > b1000);
    }

    // Deterministic property-style check: for each primitive family fixture,
    // canonical value ordering must match canonical encoded-byte ordering.
    #[test]
    #[expect(clippy::too_many_lines)]
    fn canonical_encoder_pairwise_cmp_matches_bytes_for_primitive_families() {
        let families: Vec<(&str, Vec<Value>)> = vec![
            ("Bool", vec![Value::Bool(false), Value::Bool(true)]),
            (
                "Int",
                vec![Value::Int(-2), Value::Int(-1), Value::Int(0), Value::Int(7)],
            ),
            ("Uint", vec![Value::Uint(0), Value::Uint(1), Value::Uint(7)]),
            (
                "Int128",
                vec![
                    Value::Int128(Int128::from(-2i128)),
                    Value::Int128(Int128::from(0i128)),
                    Value::Int128(Int128::from(7i128)),
                ],
            ),
            (
                "Uint128",
                vec![
                    Value::Uint128(Nat128::from(0u128)),
                    Value::Uint128(Nat128::from(1u128)),
                    Value::Uint128(Nat128::from(7u128)),
                ],
            ),
            (
                "IntBig",
                vec![
                    Value::IntBig(Int::from(-10i32)),
                    Value::IntBig(Int::from(-1i32)),
                    Value::IntBig(Int::from(0i32)),
                    Value::IntBig(Int::from(7i32)),
                ],
            ),
            (
                "UintBig",
                vec![
                    Value::UintBig(Nat::from(0u64)),
                    Value::UintBig(Nat::from(1u64)),
                    Value::UintBig(Nat::from(70u64)),
                ],
            ),
            (
                "Decimal",
                vec![
                    Value::Decimal(Decimal::new(-11, 1)),
                    Value::Decimal(Decimal::new(-10, 1)),
                    Value::Decimal(Decimal::new(0, 0)),
                    Value::Decimal(Decimal::new(10, 1)),
                    Value::Decimal(Decimal::new(11, 1)),
                ],
            ),
            (
                "Float32",
                vec![
                    Value::Float32(Float32::try_new(-1.0).expect("finite")),
                    Value::Float32(Float32::try_new(-0.0).expect("finite")),
                    Value::Float32(Float32::try_new(0.0).expect("finite")),
                    Value::Float32(Float32::try_new(1.0).expect("finite")),
                ],
            ),
            (
                "Float64",
                vec![
                    Value::Float64(Float64::try_new(-1.0).expect("finite")),
                    Value::Float64(Float64::try_new(-0.0).expect("finite")),
                    Value::Float64(Float64::try_new(0.0).expect("finite")),
                    Value::Float64(Float64::try_new(1.0).expect("finite")),
                ],
            ),
            (
                "Text",
                vec![
                    Value::Text("a".to_string()),
                    Value::Text("aa".to_string()),
                    Value::Text("b".to_string()),
                ],
            ),
            (
                "Ulid",
                vec![
                    Value::Ulid(Ulid::from_u128(1)),
                    Value::Ulid(Ulid::from_u128(2)),
                    Value::Ulid(Ulid::from_u128(3)),
                ],
            ),
            ("Unit", vec![Value::Unit, Value::Unit]),
        ];

        for (family_name, values) in families {
            for left in &values {
                for right in &values {
                    let value_cmp = Value::canonical_cmp_key(left, right);
                    let left_bytes =
                        encode_canonical_index_component(left).expect("left should encode");
                    let right_bytes =
                        encode_canonical_index_component(right).expect("right should encode");
                    let byte_cmp = left_bytes.cmp(&right_bytes);

                    assert_eq!(
                        value_cmp, byte_cmp,
                        "encoded-byte ordering mismatch for family {family_name}: left={left:?} right={right:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn canonical_encoder_decimal_negative_domain_matrix() {
        let cases = vec![
            (Decimal::new(-2, 1), Decimal::new(-21, 2), Ordering::Greater),
            (Decimal::new(-21, 2), Decimal::new(-2001, 4), Ordering::Less),
            (Decimal::new(-1, 0), Decimal::new(-9, 1), Ordering::Less),
            (Decimal::new(-11, 1), Decimal::new(-1, 0), Ordering::Less),
            (Decimal::new(-10, 0), Decimal::new(-2, 0), Ordering::Less),
            (Decimal::new(-100, 2), Decimal::new(-1, 0), Ordering::Equal),
        ];

        for (left, right, expected) in cases {
            let left_value = Value::Decimal(left);
            let right_value = Value::Decimal(right);
            let left_bytes =
                encode_canonical_index_component(&left_value).expect("left should encode");
            let right_bytes =
                encode_canonical_index_component(&right_value).expect("right should encode");

            assert_eq!(
                left.cmp(&right),
                expected,
                "numeric matrix expectation drifted: left={left:?}, right={right:?}"
            );
            assert_eq!(
                left_bytes.cmp(&right_bytes),
                expected,
                "decimal encoded ordering mismatch: left={left:?}, right={right:?}"
            );
        }
    }

    #[test]
    fn canonical_encoder_decimal_small_domain_matches_numeric_order() {
        let mut decimals = Vec::new();
        for mantissa in -200i64..=200 {
            for scale in 0u32..=8 {
                decimals.push(Decimal::new(mantissa, scale));
            }
        }

        for left in &decimals {
            for right in &decimals {
                let left_bytes = encode_canonical_index_component(&Value::Decimal(*left))
                    .expect("left should encode");
                let right_bytes = encode_canonical_index_component(&Value::Decimal(*right))
                    .expect("right should encode");
                assert_eq!(
                    left.cmp(right),
                    left_bytes.cmp(&right_bytes),
                    "small-domain ordering mismatch: left={left:?}, right={right:?}"
                );
            }
        }
    }

    fn normalize_decimal_text(raw: String) -> String {
        let trimmed = raw.trim_start_matches('0');
        if trimmed.is_empty() {
            return "0".to_string();
        }

        trimmed.to_string()
    }

    fn unsigned_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
        proptest::collection::vec(proptest::char::range('0', '9'), 1..=max_len)
            .prop_map(|digits: Vec<char>| {
                let raw: String = digits.into_iter().collect();
                normalize_decimal_text(raw)
            })
            .boxed()
    }

    fn non_zero_unsigned_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
        unsigned_decimal_text_strategy(max_len)
            .prop_filter("non-zero decimal", |digits| digits != "0")
            .boxed()
    }

    fn signed_decimal_text_strategy(max_len: usize) -> BoxedStrategy<String> {
        (any::<bool>(), unsigned_decimal_text_strategy(max_len))
            .prop_map(|(negative, digits)| {
                if negative && digits != "0" {
                    format!("-{digits}")
                } else {
                    digits
                }
            })
            .boxed()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn int_big_cross_sign_ordered_encoding_property(
            magnitude in non_zero_unsigned_decimal_text_strategy(256),
        ) {
            let negative: Int = format!("-{magnitude}").parse().expect("negative literal should parse");
            let positive: Int = magnitude.parse().expect("positive literal should parse");

            let negative_bytes = encode_canonical_index_component(&Value::IntBig(negative)).expect("negative should encode");
            let positive_bytes = encode_canonical_index_component(&Value::IntBig(positive)).expect("positive should encode");

            prop_assert!(negative_bytes < positive_bytes);
        }

        #[test]
        fn int_big_ordered_encoding_matches_numeric_order_property(
            lhs_text in signed_decimal_text_strategy(96),
            rhs_text in signed_decimal_text_strategy(96),
        ) {
            let lhs: Int = lhs_text.parse().expect("lhs int literal should parse");
            let rhs: Int = rhs_text.parse().expect("rhs int literal should parse");

            let lhs_value = Value::IntBig(lhs.clone());
            let rhs_value = Value::IntBig(rhs.clone());
            let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
            let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

            prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
        }

        #[test]
        fn uint_big_ordered_encoding_matches_numeric_order_property(
            lhs_text in unsigned_decimal_text_strategy(96),
            rhs_text in unsigned_decimal_text_strategy(96),
        ) {
            let lhs: Nat = lhs_text.parse().expect("lhs nat literal should parse");
            let rhs: Nat = rhs_text.parse().expect("rhs nat literal should parse");

            let lhs_value = Value::UintBig(lhs.clone());
            let rhs_value = Value::UintBig(rhs.clone());
            let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
            let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

            prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
        }

        #[test]
        fn decimal_ordered_encoding_matches_numeric_order_property(
            lhs_mantissa in any::<i128>(),
            rhs_mantissa in any::<i128>(),
            lhs_scale in 0u32..=28,
            rhs_scale in 0u32..=28,
        ) {
            let lhs = Decimal::from_i128_with_scale(lhs_mantissa, lhs_scale);
            let rhs = Decimal::from_i128_with_scale(rhs_mantissa, rhs_scale);

            let lhs_value = Value::Decimal(lhs);
            let rhs_value = Value::Decimal(rhs);
            let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
            let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

            prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
        }

        #[test]
        fn decimal_ordered_encoding_negative_domain_property(
            lhs_mantissa in -i128::MAX..=-1i128,
            rhs_mantissa in -i128::MAX..=-1i128,
            lhs_scale in 0u32..=28,
            rhs_scale in 0u32..=28,
        ) {
            let lhs = Decimal::from_i128_with_scale(lhs_mantissa, lhs_scale);
            let rhs = Decimal::from_i128_with_scale(rhs_mantissa, rhs_scale);

            let lhs_value = Value::Decimal(lhs);
            let rhs_value = Value::Decimal(rhs);
            let lhs_bytes = encode_canonical_index_component(&lhs_value).expect("lhs should encode");
            let rhs_bytes = encode_canonical_index_component(&rhs_value).expect("rhs should encode");

            prop_assert_eq!(lhs.cmp(&rhs), lhs_bytes.cmp(&rhs_bytes));
        }
    }
}
