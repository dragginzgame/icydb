//! Module: cursor::token::value
//! Responsibility: cursor-token `Value` tags and recursive value payload codec.
//! Does not own: token envelope structure, string token formatting, or generic
//! `Value` serialization.
//! Boundary: scalar/grouped token codec -> value payload bytes.

use crate::db::codec::writer::{
    checked_len_u32, write_i64_be, write_i128_be, write_len_prefixed_bytes_be, write_string_be,
    write_u32_be, write_u64_be, write_u128_be,
};

use crate::{
    db::{codec::ByteReader, cursor::token::TokenWireError},
    types::{
        Account, AccountStorageCodec, Date, Decimal, Duration, Float32, Float64, Principal,
        Subaccount, Timestamp, U256, Ulid,
    },
    value::{CanonicalEnumBody, EnumTypeId, EnumVariantId, Value, ValueEnum},
};

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
const VALUE_LIST: u8 = 11;
const VALUE_MAP: u8 = 12;
const VALUE_NULL: u8 = 13;
const VALUE_PRINCIPAL: u8 = 14;
const VALUE_SUBACCOUNT: u8 = 15;
const VALUE_TEXT: u8 = 16;
const VALUE_TIMESTAMP: u8 = 17;
const VALUE_NAT: u8 = 18;
const VALUE_NAT128: u8 = 19;
const VALUE_ULID: u8 = 20;
const VALUE_UNIT: u8 = 21;
const VALUE_U256: u8 = 22;

// Bound recursive construction and cleanup independently of payload byte size.
// A root is depth zero; list items, map keys/values and enum payloads each add
// one edge. Top-level cursor tuples and siblings do not consume nesting depth.
const MAX_VALUE_NESTING_DEPTH: usize = 128;

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
    let mut cursor = ByteReader::new(bytes);
    let value = read_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

pub(in crate::db::cursor::token) fn write_value_slice(
    out: &mut Vec<u8>,
    values: &[Value],
) -> Result<(), TokenWireError> {
    write_value_slice_at_depth(out, values, 0)
}

fn write_value_slice_at_depth(
    out: &mut Vec<u8>,
    values: &[Value],
    depth: usize,
) -> Result<(), TokenWireError> {
    write_u32_be(out, checked_len_u32(values.len())?);

    for value in values {
        write_value_at_depth(out, value, depth)?;
    }

    Ok(())
}

pub(in crate::db::cursor::token) fn read_value_vec(
    cursor: &mut ByteReader<'_>,
) -> Result<Vec<Value>, TokenWireError> {
    read_value_vec_at_depth(cursor, 0)
}

fn read_value_vec_at_depth(
    cursor: &mut ByteReader<'_>,
    depth: usize,
) -> Result<Vec<Value>, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
    if len > cursor.remaining() || (len != 0 && depth > MAX_VALUE_NESTING_DEPTH) {
        return Err(TokenWireError::decode());
    }
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        values.push(read_value_at_depth(cursor, depth)?);
    }

    Ok(values)
}

// Every external value entry starts a fresh depth. Recursive edges below must
// retain the current depth rather than re-entering this root function.
pub(in crate::db::cursor::token) fn write_value(
    out: &mut Vec<u8>,
    value: &Value,
) -> Result<(), TokenWireError> {
    write_value_at_depth(out, value, 0)
}

// One recursive dispatcher owns every token-supported value variant.
#[expect(clippy::too_many_lines)]
fn write_value_at_depth(
    out: &mut Vec<u8>,
    value: &Value,
    depth: usize,
) -> Result<(), TokenWireError> {
    if depth > MAX_VALUE_NESTING_DEPTH {
        return Err(TokenWireError::encode());
    }
    match value {
        Value::Account(value) => {
            out.push(VALUE_ACCOUNT);
            write_account(out, *value)
        }
        Value::Blob(value) => {
            out.push(VALUE_BLOB);
            write_len_prefixed_bytes_be(out, value.as_slice()).map_err(TokenWireError::from)
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
            write_u64_be(out, value.as_millis());
            Ok(())
        }
        Value::Enum(value) => {
            out.push(VALUE_ENUM);
            write_value_enum(out, value, depth)
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
            write_i64_be(out, *value);
            Ok(())
        }
        Value::Int128(value) => {
            out.push(VALUE_INT128);
            write_i128_be(out, *value);
            Ok(())
        }
        Value::List(items) => {
            out.push(VALUE_LIST);
            write_value_slice_at_depth(out, items.as_slice(), depth + 1)
        }
        Value::Map(entries) => {
            out.push(VALUE_MAP);
            write_map_entries(out, entries.as_slice(), depth + 1)
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
            write_string_be(out, value).map_err(TokenWireError::from)
        }
        Value::Timestamp(value) => {
            out.push(VALUE_TIMESTAMP);
            write_i64_be(out, value.as_millis());
            Ok(())
        }
        Value::Nat64(value) => {
            out.push(VALUE_NAT);
            write_u64_be(out, *value);
            Ok(())
        }
        Value::Nat128(value) => {
            out.push(VALUE_NAT128);
            write_u128_be(out, *value);
            Ok(())
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

    write_len_prefixed_bytes_be(out, bytes.as_slice()).map_err(TokenWireError::from)
}

fn write_i32_days(out: &mut Vec<u8>, value: Date) {
    out.extend_from_slice(&value.as_days_since_epoch().to_be_bytes());
}

fn write_decimal(out: &mut Vec<u8>, value: Decimal) {
    let decimal_parts = value.parts();
    write_i128_be(out, decimal_parts.mantissa());
    // Valid Decimal scales are 0–28; the full mantissa remains unchanged.
    out.push(decimal_parts.scale().to_be_bytes()[3]);
}

fn write_value_enum(
    out: &mut Vec<u8>,
    value: &ValueEnum,
    depth: usize,
) -> Result<(), TokenWireError> {
    write_u32_be(out, value.type_id().get());
    write_u32_be(out, value.variant_id().get());
    match value.body() {
        CanonicalEnumBody::Unit => out.push(0),
        CanonicalEnumBody::Payload(payload) => {
            out.push(1);
            write_value_at_depth(out, payload, depth + 1)?;
        }
    }

    Ok(())
}

fn write_map_entries(
    out: &mut Vec<u8>,
    entries: &[(Value, Value)],
    depth: usize,
) -> Result<(), TokenWireError> {
    write_u32_be(out, checked_len_u32(entries.len())?);

    for (key, value) in entries {
        write_value_at_depth(out, key, depth)?;
        write_value_at_depth(out, value, depth)?;
    }

    Ok(())
}

pub(in crate::db::cursor::token) fn read_value(
    cursor: &mut ByteReader<'_>,
) -> Result<Value, TokenWireError> {
    read_value_at_depth(cursor, 0)
}

fn read_value_at_depth(cursor: &mut ByteReader<'_>, depth: usize) -> Result<Value, TokenWireError> {
    if depth > MAX_VALUE_NESTING_DEPTH {
        return Err(TokenWireError::decode());
    }
    match cursor.read_u8()? {
        VALUE_ACCOUNT => Ok(Value::Account(read_account(cursor)?)),
        VALUE_BLOB => Ok(Value::Blob(cursor.read_len_prefixed_bytes()?.to_vec())),
        VALUE_BOOL => read_bool(cursor),
        VALUE_DATE => Ok(Value::Date(read_date(cursor)?)),
        VALUE_DECIMAL => Ok(Value::Decimal(read_decimal(cursor)?)),
        VALUE_DURATION => Ok(Value::Duration(Duration::from_millis(cursor.read_u64()?))),
        VALUE_ENUM => Ok(Value::Enum(read_value_enum(cursor, depth)?)),
        VALUE_FLOAT32 => Ok(Value::Float32(
            Float32::try_from_bytes(cursor.read_exact(4)?).map_err(|_| TokenWireError::decode())?,
        )),
        VALUE_FLOAT64 => Ok(Value::Float64(
            Float64::try_from_bytes(cursor.read_exact(8)?).map_err(|_| TokenWireError::decode())?,
        )),
        VALUE_INT => Ok(Value::Int64(cursor.read_i64()?)),
        VALUE_INT128 => Ok(Value::Int128(cursor.read_i128()?)),
        VALUE_LIST => Ok(Value::List(read_value_vec_at_depth(cursor, depth + 1)?)),
        VALUE_MAP => read_map_value(cursor, depth + 1),
        VALUE_NULL => Ok(Value::Null),
        VALUE_PRINCIPAL => Ok(Value::Principal(read_principal(cursor)?)),
        VALUE_SUBACCOUNT => Ok(Value::Subaccount(Subaccount::from_array(
            cursor.read_array()?,
        ))),
        VALUE_TEXT => Ok(Value::Text(cursor.read_string()?)),
        VALUE_TIMESTAMP => Ok(Value::Timestamp(Timestamp::from_millis(cursor.read_i64()?))),
        VALUE_NAT => Ok(Value::Nat64(cursor.read_u64()?)),
        VALUE_NAT128 => Ok(Value::Nat128(cursor.read_u128()?)),
        VALUE_ULID => Ok(Value::Ulid(Ulid::from_bytes(cursor.read_array()?))),
        VALUE_UNIT => Ok(Value::Unit),
        VALUE_U256 => Ok(Value::U256(U256::from_be_bytes(cursor.read_array()?))),
        _ => Err(TokenWireError::decode()),
    }
}

fn read_bool(cursor: &mut ByteReader<'_>) -> Result<Value, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(Value::Bool(false)),
        1 => Ok(Value::Bool(true)),
        _ => Err(TokenWireError::decode()),
    }
}

fn read_account(cursor: &mut ByteReader<'_>) -> Result<Account, TokenWireError> {
    Account::try_from_bytes(cursor.read_exact(Account::STORED_SIZE as usize)?)
        .map_err(|_| TokenWireError::decode())
}

fn read_principal(cursor: &mut ByteReader<'_>) -> Result<Principal, TokenWireError> {
    Principal::try_from_bytes(cursor.read_len_prefixed_bytes()?)
        .map_err(|_| TokenWireError::decode())
}

fn read_date(cursor: &mut ByteReader<'_>) -> Result<Date, TokenWireError> {
    Date::try_from_days_since_epoch(i32::from_be_bytes(cursor.read_array()?))
        .ok_or_else(TokenWireError::decode)
}

fn read_decimal(cursor: &mut ByteReader<'_>) -> Result<Decimal, TokenWireError> {
    let mantissa = cursor.read_i128()?;
    let scale = u32::from(cursor.read_u8()?);
    Decimal::try_from_i128_with_scale(mantissa, scale)
        .filter(|value| value.parts().scale() == scale && value.parts().mantissa() == mantissa)
        .ok_or_else(TokenWireError::decode)
}

fn read_value_enum(cursor: &mut ByteReader<'_>, depth: usize) -> Result<ValueEnum, TokenWireError> {
    let type_id = EnumTypeId::new(cursor.read_u32()?).ok_or_else(TokenWireError::decode)?;
    let variant_id = EnumVariantId::new(cursor.read_u32()?).ok_or_else(TokenWireError::decode)?;
    let body = match cursor.read_u8()? {
        0 => CanonicalEnumBody::Unit,
        1 => CanonicalEnumBody::Payload(Box::new(read_value_at_depth(cursor, depth + 1)?)),
        _ => {
            return Err(TokenWireError::decode());
        }
    };
    Ok(ValueEnum::new(type_id, variant_id, body))
}

fn read_map_value(cursor: &mut ByteReader<'_>, depth: usize) -> Result<Value, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
    if len > cursor.remaining() / 2 || (len != 0 && depth > MAX_VALUE_NESTING_DEPTH) {
        return Err(TokenWireError::decode());
    }
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        entries.push((
            read_value_at_depth(cursor, depth)?,
            read_value_at_depth(cursor, depth)?,
        ));
    }

    Value::from_map(entries).map_err(|_| TokenWireError::decode())
}

#[cfg(test)]
mod tests;
