//! Module: cursor::token::value
//! Responsibility: cursor-token `Value` tags and recursive value payload codec.
//! Does not own: token envelope structure, string token formatting, or generic
//! `Value` serialization.
//! Boundary: scalar/grouped token codec -> value payload bytes.

use crate::{
    db::cursor::token::{
        TokenWireError,
        bytes::{
            ByteCursor, checked_len_u32, write_i64, write_i128, write_len_prefixed_bytes,
            write_string, write_u32, write_u64, write_u128,
        },
    },
    types::{
        Account, AccountStorageCodec, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig,
        Principal, Subaccount, Timestamp, U256, Ulid,
    },
    value::{CanonicalEnumBody, EnumTypeId, EnumVariantId, Value, ValueEnum},
};
use num_bigint::{BigInt, BigUint, Sign};

const VALUE_ACCOUNT: u8 = 0;
const VALUE_BLOB: u8 = 1;
const VALUE_BOOL: u8 = 2;
const VALUE_DATE: u8 = 3;
const VALUE_DECIMAL: u8 = 4;
const VALUE_DURATION: u8 = 5;
const VALUE_ENUM: u8 = 6;
const VALUE_FLOAT32: u8 = 7;
const VALUE_FLOAT64: u8 = 8;
const VALUE_INT: u8 = 9;
const VALUE_INT128: u8 = 10;
const VALUE_INT_BIG: u8 = 11;
const VALUE_LIST: u8 = 12;
const VALUE_MAP: u8 = 13;
const VALUE_NULL: u8 = 14;
const VALUE_PRINCIPAL: u8 = 15;
const VALUE_SUBACCOUNT: u8 = 16;
const VALUE_TEXT: u8 = 17;
const VALUE_TIMESTAMP: u8 = 18;
const VALUE_NAT: u8 = 19;
const VALUE_NAT128: u8 = 20;
const VALUE_NAT_BIG: u8 = 21;
const VALUE_ULID: u8 = 22;
const VALUE_UNIT: u8 = 23;
const VALUE_U256: u8 = 24;

/// Encode one runtime value through the current bounded binary value wire.
///
/// This deliberately shares the cursor-owned value variant map with private
/// durable engine protocols so a second value codec cannot drift from it.
#[cfg(feature = "sql")]
pub(in crate::db) fn encode_current_value_payload(
    value: &Value,
) -> Result<Vec<u8>, TokenWireError> {
    let mut bytes = Vec::new();
    write_value(&mut bytes, value)?;
    Ok(bytes)
}

/// Decode one runtime value through the current bounded binary value wire.
#[cfg(feature = "sql")]
pub(in crate::db) fn decode_current_value_payload(bytes: &[u8]) -> Result<Value, TokenWireError> {
    let mut cursor = ByteCursor::new(bytes);
    let value = read_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

pub(in crate::db::cursor::token) fn write_value_slice(
    out: &mut Vec<u8>,
    values: &[Value],
) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(values.len())?);

    for value in values {
        write_value(out, value)?;
    }

    Ok(())
}

pub(in crate::db::cursor::token) fn read_value_vec(
    cursor: &mut ByteCursor<'_>,
) -> Result<Vec<Value>, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
    if len > cursor.remaining() {
        return Err(TokenWireError::decode());
    }
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        values.push(read_value(cursor)?);
    }

    Ok(values)
}

// One recursive dispatcher owns every token-supported `Value` leaf shape so
// cursor-token encoding keeps a single authoritative variant map.
#[expect(clippy::too_many_lines)]
pub(in crate::db::cursor::token) fn write_value(
    out: &mut Vec<u8>,
    value: &Value,
) -> Result<(), TokenWireError> {
    match value {
        Value::Account(value) => {
            out.push(VALUE_ACCOUNT);
            write_account(out, *value)
        }
        Value::Blob(value) => {
            out.push(VALUE_BLOB);
            write_len_prefixed_bytes(out, value.as_slice())
        }
        Value::Bool(value) => {
            out.push(VALUE_BOOL);
            out.push(u8::from(*value));
            Ok(())
        }
        Value::Date(value) => {
            out.push(VALUE_DATE);
            write_i32_days(out, *value);
            Ok(())
        }
        Value::Decimal(value) => {
            out.push(VALUE_DECIMAL);
            write_decimal(out, *value);
            Ok(())
        }
        Value::Duration(value) => {
            out.push(VALUE_DURATION);
            write_u64(out, value.as_millis());
            Ok(())
        }
        Value::Enum(value) => {
            out.push(VALUE_ENUM);
            write_value_enum(out, value)
        }
        Value::Float32(value) => {
            out.push(VALUE_FLOAT32);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        Value::Float64(value) => {
            out.push(VALUE_FLOAT64);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        Value::Int64(value) => {
            out.push(VALUE_INT);
            write_i64(out, *value);
            Ok(())
        }
        Value::Int128(value) => {
            out.push(VALUE_INT128);
            write_i128(out, *value);
            Ok(())
        }
        Value::IntBig(value) => {
            out.push(VALUE_INT_BIG);
            let (negative, digits) = value.sign_and_u32_digits();
            write_big_magnitude(out, Some(negative), digits)
        }
        Value::List(items) => {
            out.push(VALUE_LIST);
            write_value_slice(out, items.as_slice())
        }
        Value::Map(entries) => {
            out.push(VALUE_MAP);
            write_map_entries(out, entries.as_slice())
        }
        Value::Null => {
            out.push(VALUE_NULL);
            Ok(())
        }
        Value::Principal(value) => {
            out.push(VALUE_PRINCIPAL);
            write_principal(out, *value)
        }
        Value::Subaccount(value) => {
            out.push(VALUE_SUBACCOUNT);
            out.extend_from_slice(&value.to_bytes());
            Ok(())
        }
        Value::Text(value) => {
            out.push(VALUE_TEXT);
            write_string(out, value)
        }
        Value::Timestamp(value) => {
            out.push(VALUE_TIMESTAMP);
            write_i64(out, value.as_millis());
            Ok(())
        }
        Value::Nat64(value) => {
            out.push(VALUE_NAT);
            write_u64(out, *value);
            Ok(())
        }
        Value::Nat128(value) => {
            out.push(VALUE_NAT128);
            write_u128(out, *value);
            Ok(())
        }
        Value::NatBig(value) => {
            out.push(VALUE_NAT_BIG);
            write_big_magnitude(out, None, value.u32_digits())
        }
        Value::Ulid(value) => {
            out.push(VALUE_ULID);
            out.extend_from_slice(&value.to_bytes());
            Ok(())
        }
        Value::Unit => {
            out.push(VALUE_UNIT);
            Ok(())
        }
        Value::U256(value) => {
            out.push(VALUE_U256);
            out.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
    }
}

fn write_account(out: &mut Vec<u8>, value: Account) -> Result<(), TokenWireError> {
    let bytes = value
        .to_stored_bytes()
        .map_err(|_| TokenWireError::encode())?;

    out.extend_from_slice(&bytes);
    Ok(())
}

fn write_principal(out: &mut Vec<u8>, value: Principal) -> Result<(), TokenWireError> {
    let bytes = value.to_bytes().map_err(|_| TokenWireError::encode())?;

    write_len_prefixed_bytes(out, bytes.as_slice())
}

fn write_i32_days(out: &mut Vec<u8>, value: Date) {
    out.extend_from_slice(&value.as_days_since_epoch().to_be_bytes());
}

fn write_decimal(out: &mut Vec<u8>, value: Decimal) {
    let decimal_parts = value.parts();
    write_i128(out, decimal_parts.mantissa());
    // Valid Decimal scales are 0–28; the full mantissa remains unchanged.
    out.push(decimal_parts.scale().to_be_bytes()[3]);
}

fn write_value_enum(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), TokenWireError> {
    write_u32(out, value.type_id().get());
    write_u32(out, value.variant_id().get());
    match value.body() {
        CanonicalEnumBody::Unit => out.push(0),
        CanonicalEnumBody::Payload(payload) => {
            out.push(1);
            write_value(out, payload)?;
        }
    }

    Ok(())
}

fn write_map_entries(out: &mut Vec<u8>, entries: &[(Value, Value)]) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(entries.len())?);

    for (key, value) in entries {
        write_value(out, key)?;
        write_value(out, value)?;
    }

    Ok(())
}

pub(in crate::db::cursor::token) fn read_value(
    cursor: &mut ByteCursor<'_>,
) -> Result<Value, TokenWireError> {
    match cursor.read_u8()? {
        VALUE_ACCOUNT => Ok(Value::Account(read_account(cursor)?)),
        VALUE_BLOB => Ok(Value::Blob(cursor.read_len_prefixed_bytes()?.to_vec())),
        VALUE_BOOL => read_bool(cursor),
        VALUE_DATE => Ok(Value::Date(read_date(cursor)?)),
        VALUE_DECIMAL => Ok(Value::Decimal(read_decimal(cursor)?)),
        VALUE_DURATION => Ok(Value::Duration(Duration::from_millis(cursor.read_u64()?))),
        VALUE_ENUM => Ok(Value::Enum(read_value_enum(cursor)?)),
        VALUE_FLOAT32 => Ok(Value::Float32(
            Float32::try_from_bytes(cursor.read_exact(4)?).map_err(|_| TokenWireError::decode())?,
        )),
        VALUE_FLOAT64 => Ok(Value::Float64(
            Float64::try_from_bytes(cursor.read_exact(8)?).map_err(|_| TokenWireError::decode())?,
        )),
        VALUE_INT => Ok(Value::Int64(cursor.read_i64()?)),
        VALUE_INT128 => Ok(Value::Int128(cursor.read_i128()?)),
        VALUE_INT_BIG => Ok(Value::IntBig(read_big_int(cursor)?)),
        VALUE_LIST => Ok(Value::List(read_value_vec(cursor)?)),
        VALUE_MAP => read_map_value(cursor),
        VALUE_NULL => Ok(Value::Null),
        VALUE_PRINCIPAL => Ok(Value::Principal(read_principal(cursor)?)),
        VALUE_SUBACCOUNT => Ok(Value::Subaccount(Subaccount::from_array(
            cursor.read_array()?,
        ))),
        VALUE_TEXT => Ok(Value::Text(cursor.read_string()?)),
        VALUE_TIMESTAMP => Ok(Value::Timestamp(Timestamp::from_millis(cursor.read_i64()?))),
        VALUE_NAT => Ok(Value::Nat64(cursor.read_u64()?)),
        VALUE_NAT128 => Ok(Value::Nat128(cursor.read_u128()?)),
        VALUE_NAT_BIG => Ok(Value::NatBig(read_big_nat(cursor)?)),
        VALUE_ULID => Ok(Value::Ulid(Ulid::from_bytes(cursor.read_array()?))),
        VALUE_UNIT => Ok(Value::Unit),
        VALUE_U256 => Ok(Value::U256(U256::from_be_bytes(cursor.read_array()?))),
        _ => Err(TokenWireError::decode()),
    }
}

fn read_bool(cursor: &mut ByteCursor<'_>) -> Result<Value, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(Value::Bool(false)),
        1 => Ok(Value::Bool(true)),
        _ => Err(TokenWireError::decode()),
    }
}

fn read_account(cursor: &mut ByteCursor<'_>) -> Result<Account, TokenWireError> {
    Account::try_from_bytes(cursor.read_exact(Account::STORED_SIZE as usize)?)
        .map_err(|_| TokenWireError::decode())
}

fn read_principal(cursor: &mut ByteCursor<'_>) -> Result<Principal, TokenWireError> {
    Principal::try_from_bytes(cursor.read_len_prefixed_bytes()?)
        .map_err(|_| TokenWireError::decode())
}

fn read_date(cursor: &mut ByteCursor<'_>) -> Result<Date, TokenWireError> {
    Date::try_from_days_since_epoch(i32::from_be_bytes(cursor.read_array()?))
        .ok_or_else(TokenWireError::decode)
}

fn read_decimal(cursor: &mut ByteCursor<'_>) -> Result<Decimal, TokenWireError> {
    let mantissa = cursor.read_i128()?;
    let scale = u32::from(cursor.read_u8()?);
    Decimal::try_from_i128_with_scale(mantissa, scale)
        .filter(|value| value.parts().scale() == scale && value.parts().mantissa() == mantissa)
        .ok_or_else(TokenWireError::decode)
}

fn read_value_enum(cursor: &mut ByteCursor<'_>) -> Result<ValueEnum, TokenWireError> {
    let type_id = EnumTypeId::new(cursor.read_u32()?).ok_or_else(TokenWireError::decode)?;
    let variant_id = EnumVariantId::new(cursor.read_u32()?).ok_or_else(TokenWireError::decode)?;
    let body = match cursor.read_u8()? {
        0 => CanonicalEnumBody::Unit,
        1 => CanonicalEnumBody::Payload(Box::new(read_value(cursor)?)),
        _ => {
            return Err(TokenWireError::decode());
        }
    };
    Ok(ValueEnum::new(type_id, variant_id, body))
}

// Stream the atom's borrowed limbs directly into one canonical magnitude frame.
// Signed zero has sign 0 and no magnitude; nonzero signs are 1 (+) and 2 (-).
fn write_big_magnitude(
    out: &mut Vec<u8>,
    negative: Option<bool>,
    mut digits: impl DoubleEndedIterator<Item = u32> + ExactSizeIterator,
) -> Result<(), TokenWireError> {
    let high = digits.next_back();
    let high_len = high.map_or(0, |digit| {
        (u32::BITS - digit.leading_zeros()).div_ceil(8) as usize
    });
    let len = digits
        .len()
        .checked_mul(4)
        .and_then(|len| len.checked_add(high_len))
        .ok_or_else(TokenWireError::encode)?;
    if let Some(negative) = negative {
        out.push(if len == 0 {
            0
        } else if negative {
            2
        } else {
            1
        });
    }
    write_u32(out, checked_len_u32(len)?);
    for digit in digits {
        out.extend_from_slice(&digit.to_le_bytes());
    }
    if let Some(high) = high {
        out.extend_from_slice(&high.to_le_bytes()[..high_len]);
    }
    Ok(())
}

fn read_big_int(cursor: &mut ByteCursor<'_>) -> Result<IntBig, TokenWireError> {
    let sign = match cursor.read_u8()? {
        0 => Sign::NoSign,
        1 => Sign::Plus,
        2 => Sign::Minus,
        _ => return Err(TokenWireError::decode()),
    };
    let magnitude = read_big_magnitude(cursor)?;
    if (sign == Sign::NoSign) != magnitude.is_empty() {
        return Err(TokenWireError::decode());
    }
    Ok(IntBig::from_bigint(BigInt::from_bytes_le(sign, magnitude)))
}

fn read_big_nat(cursor: &mut ByteCursor<'_>) -> Result<NatBig, TokenWireError> {
    Ok(NatBig::from_biguint(BigUint::from_bytes_le(
        read_big_magnitude(cursor)?,
    )))
}

// Validate the full borrowed frame before allocating a decoded integer.
fn read_big_magnitude<'a>(cursor: &mut ByteCursor<'a>) -> Result<&'a [u8], TokenWireError> {
    let magnitude = cursor.read_len_prefixed_bytes()?;
    if magnitude.last() == Some(&0) {
        return Err(TokenWireError::decode());
    }
    Ok(magnitude)
}

fn read_map_value(cursor: &mut ByteCursor<'_>) -> Result<Value, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
    if len > cursor.remaining() / 2 {
        return Err(TokenWireError::decode());
    }
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        entries.push((read_value(cursor)?, read_value(cursor)?));
    }

    Value::from_map(entries).map_err(|_| TokenWireError::decode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_value_decode_rejects_days_outside_bounded_calendar() {
        let mut encoded = vec![VALUE_DATE];
        encoded.extend_from_slice(&(Date::MIN.as_days_since_epoch() - 1).to_be_bytes());

        assert!(read_value(&mut ByteCursor::new(encoded.as_slice())).is_err());
    }

    #[test]
    fn cursor_value_u256_roundtrips_fixed_width_boundaries() {
        for value in [U256::ZERO, U256::ONE, U256::MAX] {
            let mut encoded = Vec::new();
            write_value(&mut encoded, &Value::U256(value)).expect("U256 should encode");

            assert_eq!(encoded.len(), 33);
            assert_eq!(encoded[0], VALUE_U256);
            assert_eq!(
                read_value(&mut ByteCursor::new(encoded.as_slice())).expect("U256 should decode"),
                Value::U256(value),
            );
        }
    }

    #[test]
    fn cursor_value_u256_rejects_truncated_payload() {
        let mut encoded = vec![VALUE_U256];
        encoded.extend_from_slice(&[0; 31]);

        assert!(read_value(&mut ByteCursor::new(encoded.as_slice())).is_err());
    }

    #[test]
    fn compact_cursor_values_preserve_sizes_domains_and_nested_boundaries() {
        let mut cases = Vec::new();
        for bits in [0_usize, 1, 8, 9, 32, 33, 256, 1024] {
            let magnitude = (BigUint::from(1_u8) << bits) - BigUint::from(1_u8);
            let width = bits.div_ceil(8);
            cases.push((
                Value::NatBig(NatBig::from_biguint(magnitude.clone())),
                5 + width,
            ));
            for sign in [Sign::Plus, Sign::Minus] {
                cases.push((
                    Value::IntBig(IntBig::from_bigint(BigInt::from_biguint(
                        sign,
                        magnitude.clone(),
                    ))),
                    6 + width,
                ));
            }
        }
        for subaccount in [None, Some(Subaccount::MAX)] {
            cases.push((Value::Account(Account::new(Principal::MAX, subaccount)), 63));
        }
        for mantissa in [i128::MIN, -1200, 0, 1200, i128::MAX] {
            for scale in [0, 2, 28] {
                cases.push((
                    Value::Decimal(Decimal::from_i128_with_scale(mantissa, scale)),
                    18,
                ));
            }
        }
        for (value, width) in cases {
            let mut encoded = Vec::new();
            write_value(&mut encoded, &value).unwrap();
            assert_eq!(encoded.len(), width);
            let mut cursor = ByteCursor::new(&encoded);
            let decoded = read_value(&mut cursor).unwrap();
            cursor.finish().unwrap();
            assert_eq!(decoded, value);
            if let (Value::Decimal(expected), Value::Decimal(actual)) = (&value, decoded) {
                assert_eq!(expected.parts(), actual.parts());
            }
            for len in 0..encoded.len() {
                assert!(read_value(&mut ByteCursor::new(&encoded[..len])).is_err());
            }
            let nested = Value::List(vec![value; 1000]);
            encoded.clear();
            write_value(&mut encoded, &nested).unwrap();
            assert_eq!(encoded.len(), 5 + 1000 * width);
            let mut cursor = ByteCursor::new(&encoded);
            assert_eq!(read_value(&mut cursor).unwrap(), nested);
            cursor.finish().unwrap();
        }
    }

    #[test]
    fn compact_cursor_values_reject_noncanonical_magnitudes_and_metadata() {
        for payload in [
            vec![VALUE_NAT_BIG, 0, 0, 0, 1, 0],
            vec![VALUE_NAT_BIG, 0, 0, 0, 2, 1, 0],
            vec![VALUE_NAT_BIG, 255, 255, 255, 255],
            vec![VALUE_INT_BIG, 3, 0, 0, 0, 0],
            vec![VALUE_INT_BIG, 1, 0, 0, 0, 0],
            vec![VALUE_INT_BIG, 2, 0, 0, 0, 0],
            vec![VALUE_INT_BIG, 0, 0, 0, 0, 1, 1],
            vec![VALUE_INT_BIG, 2, 0, 0, 0, 1, 0],
        ] {
            assert!(read_value(&mut ByteCursor::new(&payload)).is_err());
        }
        for scale in [29, 255] {
            let mut payload = vec![VALUE_DECIMAL];
            payload.extend_from_slice(&0_i128.to_be_bytes());
            payload.push(scale);
            assert!(read_value(&mut ByteCursor::new(&payload)).is_err());
        }
        let mut invalid_account = vec![VALUE_ACCOUNT];
        invalid_account.extend_from_slice(&[255; 62]);
        assert!(read_value(&mut ByteCursor::new(&invalid_account)).is_err());
    }
}
