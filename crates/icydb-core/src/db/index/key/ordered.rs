use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::{Account, Decimal, Principal},
    value::{Value, ValueEnum},
};
use thiserror::Error as ThisError;

const NEGATIVE_MARKER: u8 = 0x00;
const ZERO_MARKER: u8 = 0x01;
const POSITIVE_MARKER: u8 = 0x02;

const LENGTH_BYTES: usize = 2;
const MAX_SEGMENT_LEN: usize = u16::MAX as usize;
const ACCOUNT_OWNER_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
const ACCOUNT_SUBACCOUNT_LEN: usize = 32;
const ACCOUNT_SUBACCOUNT_TAG: u8 = 0x80;

///
/// OrderedValueEncodeError
///
/// Canonical index-encoding failures for one `Value` component.
///

#[derive(Debug, ThisError)]
pub enum OrderedValueEncodeError {
    #[error("null values are not indexable")]
    NullNotIndexable,

    #[error("value kind '{kind}' is not canonically index-orderable")]
    UnsupportedValueKind { kind: &'static str },

    #[error("ordered segment exceeds max length: {len} bytes (limit {max})")]
    SegmentTooLarge { len: usize, max: usize },

    #[error("invalid signed integer decimal representation: '{value}'")]
    InvalidSignedDecimal { value: String },

    #[error("invalid unsigned integer decimal representation: '{value}'")]
    InvalidUnsignedDecimal { value: String },

    #[error("decimal exponent overflow during canonical encoding")]
    DecimalExponentOverflow,
}

impl OrderedValueEncodeError {
    #[must_use]
    pub const fn is_null_not_indexable(&self) -> bool {
        matches!(self, Self::NullNotIndexable)
    }
}

impl From<OrderedValueEncodeError> for InternalError {
    fn from(err: OrderedValueEncodeError) -> Self {
        Self::new(
            ErrorClass::Unsupported,
            ErrorOrigin::Index,
            format!("index value is not canonically order-encodable: {err}"),
        )
    }
}

/// Encode one scalar index component so lexicographic byte order matches
/// canonical `Value` order for supported primitive variants.
pub fn encode_canonical_index_component(value: &Value) -> Result<Vec<u8>, OrderedValueEncodeError> {
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
                kind: value_kind_label(value),
            })
        }
        Value::Bool(v) => {
            out.push(u8::from(*v));
            Ok(())
        }
        Value::Date(v) => {
            out.extend_from_slice(&ordered_i32_bytes(v.get()));
            Ok(())
        }
        Value::Decimal(v) => push_decimal_payload(out, *v),
        Value::Duration(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
            Ok(())
        }
        Value::Enum(v) => push_enum_payload(out, v),
        Value::E8s(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
            Ok(())
        }
        Value::E18s(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
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
        Value::Int128(v) => {
            out.extend_from_slice(&ordered_i128_bytes(v.get()));
            Ok(())
        }
        Value::IntBig(v) => push_signed_decimal_payload(out, &v.to_string()),
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
        Value::Timestamp(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
            Ok(())
        }
        Value::Uint(v) => {
            out.extend_from_slice(&v.to_be_bytes());
            Ok(())
        }
        Value::Uint128(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
            Ok(())
        }
        Value::UintBig(v) => push_unsigned_decimal_payload(out, &v.to_string()),
        Value::Ulid(v) => {
            out.extend_from_slice(&v.to_bytes());
            Ok(())
        }
        Value::Unit => Ok(()),
    }
}

// Account ordering uses the same tuple contract as `Account::cmp`.
#[allow(clippy::cast_possible_truncation)]
fn push_account_payload(out: &mut Vec<u8>, account: &Account) {
    let owner = account.owner.as_slice();
    let owner_len = owner.len().min(u8::MAX as usize);

    let mut ordering_tag = owner_len as u8;
    if account.subaccount.is_some() {
        ordering_tag |= ACCOUNT_SUBACCOUNT_TAG;
    }

    out.push(ordering_tag);

    let mut owner_padded = [0u8; ACCOUNT_OWNER_MAX_LEN];
    owner_padded[..owner.len()].copy_from_slice(owner);
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

// Decimal ordering is sign bucket + exponent + significant digits.
fn push_decimal_payload(out: &mut Vec<u8>, value: Decimal) -> Result<(), OrderedValueEncodeError> {
    let normalized = value.normalize();
    if normalized.is_zero() {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let parts = normalized.parts();
    let digits = parts.mantissa.unsigned_abs().to_string();
    let exponent = decimal_exponent(parts.scale, digits.len())?;

    let exponent_bytes = ordered_i32_bytes(exponent);
    let digits_bytes = digits.as_bytes();
    let digits_len = encode_segment_len(digits_bytes.len())?;

    if parts.mantissa.is_negative() {
        out.push(NEGATIVE_MARKER);
        push_inverted(out, &exponent_bytes);
        push_inverted(out, &digits_len);
        push_inverted(out, digits_bytes);
    } else {
        out.push(POSITIVE_MARKER);
        out.extend_from_slice(&exponent_bytes);
        out.extend_from_slice(&digits_len);
        out.extend_from_slice(digits_bytes);
    }

    Ok(())
}

// Signed big-int ordering uses sign bucket + digit length + digit bytes.
fn push_signed_decimal_payload(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), OrderedValueEncodeError> {
    let (negative, digits) = split_signed_decimal_digits(value)?;

    if digits == "0" {
        out.push(ZERO_MARKER);
        return Ok(());
    }

    let digits_len = encode_segment_len(digits.len())?;

    if negative {
        out.push(NEGATIVE_MARKER);
        push_inverted(out, &digits_len);
        push_inverted(out, digits.as_bytes());
    } else {
        out.push(POSITIVE_MARKER);
        out.extend_from_slice(&digits_len);
        out.extend_from_slice(digits.as_bytes());
    }

    Ok(())
}

// Unsigned big-int ordering is length + digit bytes.
fn push_unsigned_decimal_payload(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), OrderedValueEncodeError> {
    let digits = validate_unsigned_decimal_digits(value)?;

    let digits_len = encode_segment_len(digits.len())?;
    out.extend_from_slice(&digits_len);
    out.extend_from_slice(digits.as_bytes());

    Ok(())
}

// Byte strings are escaped so tuple boundaries remain unambiguous.
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
    let digit_count =
        i64::try_from(digit_len).map_err(|_| OrderedValueEncodeError::DecimalExponentOverflow)?;
    let scale = i64::from(scale);

    let exponent = digit_count
        .checked_sub(1)
        .and_then(|value| value.checked_sub(scale))
        .ok_or(OrderedValueEncodeError::DecimalExponentOverflow)?;

    i32::try_from(exponent).map_err(|_| OrderedValueEncodeError::DecimalExponentOverflow)
}

fn split_signed_decimal_digits(value: &str) -> Result<(bool, &str), OrderedValueEncodeError> {
    if let Some(digits) = value.strip_prefix('-') {
        if !is_valid_decimal_digits(digits) {
            return Err(OrderedValueEncodeError::InvalidSignedDecimal {
                value: value.to_string(),
            });
        }
        return Ok((true, digits));
    }

    if !is_valid_decimal_digits(value) {
        return Err(OrderedValueEncodeError::InvalidSignedDecimal {
            value: value.to_string(),
        });
    }

    Ok((false, value))
}

fn validate_unsigned_decimal_digits(value: &str) -> Result<&str, OrderedValueEncodeError> {
    if !is_valid_decimal_digits(value) {
        return Err(OrderedValueEncodeError::InvalidUnsignedDecimal {
            value: value.to_string(),
        });
    }

    Ok(value)
}

fn is_valid_decimal_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
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

const fn value_kind_label(value: &Value) -> &'static str {
    match value {
        Value::Account(_) => "Account",
        Value::Blob(_) => "Blob",
        Value::Bool(_) => "Bool",
        Value::Date(_) => "Date",
        Value::Decimal(_) => "Decimal",
        Value::Duration(_) => "Duration",
        Value::Enum(_) => "Enum",
        Value::E8s(_) => "E8s",
        Value::E18s(_) => "E18s",
        Value::Float32(_) => "Float32",
        Value::Float64(_) => "Float64",
        Value::Int(_) => "Int",
        Value::Int128(_) => "Int128",
        Value::IntBig(_) => "IntBig",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Null => "Null",
        Value::Principal(_) => "Principal",
        Value::Subaccount(_) => "Subaccount",
        Value::Text(_) => "Text",
        Value::Timestamp(_) => "Timestamp",
        Value::Uint(_) => "Uint",
        Value::Uint128(_) => "Uint128",
        Value::UintBig(_) => "UintBig",
        Value::Ulid(_) => "Ulid",
        Value::Unit => "Unit",
    }
}

#[cfg(test)]
mod tests {
    use super::encode_canonical_index_component;
    use crate::{
        types::{
            Account, Date, Decimal, Duration, E8s, E18s, Float32, Float64, Int, Int128, Nat,
            Nat128, Principal, Subaccount, Timestamp, Ulid,
        },
        value::{Value, ValueEnum},
    };
    use std::cmp::Ordering;

    fn assert_encoded_order(left: Value, right: Value, expected: Ordering) {
        let left_bytes = encode_canonical_index_component(&left).expect("left should encode");
        let right_bytes = encode_canonical_index_component(&right).expect("right should encode");

        assert_eq!(left_bytes.cmp(&right_bytes), expected);
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
            Value::E8s(E8s::from_atomic(1)),
            Value::E18s(E18s::from_atomic(1)),
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
            Value::Timestamp(Timestamp::from_seconds(1)),
            Value::Timestamp(Timestamp::from_seconds(2)),
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
}
