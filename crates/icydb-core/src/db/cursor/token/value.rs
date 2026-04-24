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
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint};

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
const VALUE_UINT: u8 = 19;
const VALUE_UINT128: u8 = 20;
const VALUE_UINT_BIG: u8 = 21;
const VALUE_ULID: u8 = 22;
const VALUE_UNIT: u8 = 23;

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
    let len = usize::try_from(cursor.read_u32()?)
        .map_err(|_| TokenWireError::decode("cursor value count does not fit usize"))?;
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
        Value::Int(value) => {
            out.push(VALUE_INT);
            write_i64(out, *value);
            Ok(())
        }
        Value::Int128(value) => {
            out.push(VALUE_INT128);
            write_i128(out, value.get());
            Ok(())
        }
        Value::IntBig(value) => {
            out.push(VALUE_INT_BIG);
            write_string(out, &value.to_string())
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
        Value::Uint(value) => {
            out.push(VALUE_UINT);
            write_u64(out, *value);
            Ok(())
        }
        Value::Uint128(value) => {
            out.push(VALUE_UINT128);
            write_u128(out, value.get());
            Ok(())
        }
        Value::UintBig(value) => {
            out.push(VALUE_UINT_BIG);
            write_string(out, &value.to_string())
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
    }
}

fn write_account(out: &mut Vec<u8>, value: Account) -> Result<(), TokenWireError> {
    let bytes = value
        .to_bytes()
        .map_err(|err| TokenWireError::encode(err.to_string()))?;

    write_len_prefixed_bytes(out, bytes.as_slice())
}

fn write_principal(out: &mut Vec<u8>, value: Principal) -> Result<(), TokenWireError> {
    let bytes = value
        .to_bytes()
        .map_err(|err| TokenWireError::encode(err.to_string()))?;

    write_len_prefixed_bytes(out, bytes.as_slice())
}

fn write_i32_days(out: &mut Vec<u8>, value: Date) {
    out.extend_from_slice(&value.as_days_since_epoch().to_be_bytes());
}

fn write_decimal(out: &mut Vec<u8>, value: Decimal) {
    let parts = value.parts();
    write_i128(out, parts.mantissa());
    write_u32(out, parts.scale());
}

fn write_value_enum(out: &mut Vec<u8>, value: &ValueEnum) -> Result<(), TokenWireError> {
    write_string(out, value.variant())?;

    match value.path() {
        Some(path) => {
            out.push(1);
            write_string(out, path)?;
        }
        None => out.push(0),
    }

    match value.payload() {
        Some(payload) => {
            out.push(1);
            write_value(out, payload)?;
        }
        None => out.push(0),
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
            Float32::try_from_bytes(cursor.read_exact(4)?)
                .map_err(|err| TokenWireError::decode(err.to_string()))?,
        )),
        VALUE_FLOAT64 => Ok(Value::Float64(
            Float64::try_from_bytes(cursor.read_exact(8)?)
                .map_err(|err| TokenWireError::decode(err.to_string()))?,
        )),
        VALUE_INT => Ok(Value::Int(cursor.read_i64()?)),
        VALUE_INT128 => Ok(Value::Int128(Int128::from(cursor.read_i128()?))),
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
        VALUE_UINT => Ok(Value::Uint(cursor.read_u64()?)),
        VALUE_UINT128 => Ok(Value::Uint128(Nat128::from(cursor.read_u128()?))),
        VALUE_UINT_BIG => Ok(Value::UintBig(read_big_uint(cursor)?)),
        VALUE_ULID => Ok(Value::Ulid(Ulid::from_bytes(cursor.read_array()?))),
        VALUE_UNIT => Ok(Value::Unit),
        other => Err(TokenWireError::decode(format!(
            "unsupported cursor value tag {other}"
        ))),
    }
}

fn read_bool(cursor: &mut ByteCursor<'_>) -> Result<Value, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(Value::Bool(false)),
        1 => Ok(Value::Bool(true)),
        other => Err(TokenWireError::decode(format!(
            "unsupported cursor bool tag {other}"
        ))),
    }
}

fn read_account(cursor: &mut ByteCursor<'_>) -> Result<Account, TokenWireError> {
    Account::try_from_bytes(cursor.read_len_prefixed_bytes()?)
        .map_err(|err| TokenWireError::decode(err.to_string()))
}

fn read_principal(cursor: &mut ByteCursor<'_>) -> Result<Principal, TokenWireError> {
    Principal::try_from_bytes(cursor.read_len_prefixed_bytes()?)
        .map_err(|err| TokenWireError::decode(err.to_string()))
}

fn read_date(cursor: &mut ByteCursor<'_>) -> Result<Date, TokenWireError> {
    Ok(Date::from_days_since_epoch(i32::from_be_bytes(
        cursor.read_array()?,
    )))
}

fn read_decimal(cursor: &mut ByteCursor<'_>) -> Result<Decimal, TokenWireError> {
    let mantissa = cursor.read_i128()?;
    let scale = cursor.read_u32()?;
    let value = Decimal::from_i128_with_scale(mantissa, scale);

    if value.parts().scale() != scale || value.parts().mantissa() != mantissa {
        return Err(TokenWireError::decode("invalid decimal token payload"));
    }

    Ok(value)
}

fn read_value_enum(cursor: &mut ByteCursor<'_>) -> Result<ValueEnum, TokenWireError> {
    let variant = cursor.read_string()?;

    let path = match cursor.read_u8()? {
        0 => None,
        1 => Some(cursor.read_string()?),
        other => {
            return Err(TokenWireError::decode(format!(
                "unsupported enum path presence tag {other}"
            )));
        }
    };

    let payload = match cursor.read_u8()? {
        0 => None,
        1 => Some(read_value(cursor)?),
        other => {
            return Err(TokenWireError::decode(format!(
                "unsupported enum payload presence tag {other}"
            )));
        }
    };

    let mut value = ValueEnum::new(variant.as_str(), path.as_deref());
    if let Some(payload) = payload {
        value = value.with_payload(payload);
    }

    Ok(value)
}

fn read_big_int(cursor: &mut ByteCursor<'_>) -> Result<Int, TokenWireError> {
    let text = cursor.read_string()?;
    let big = BigInt::parse_bytes(text.as_bytes(), 10)
        .ok_or_else(|| TokenWireError::decode("invalid bigint token payload"))?;

    Ok(Int::from(WrappedInt::from(big)))
}

fn read_big_uint(cursor: &mut ByteCursor<'_>) -> Result<Nat, TokenWireError> {
    let text = cursor.read_string()?;
    let big = BigUint::parse_bytes(text.as_bytes(), 10)
        .ok_or_else(|| TokenWireError::decode("invalid biguint token payload"))?;

    Ok(Nat::from(WrappedNat::from(big)))
}

fn read_map_value(cursor: &mut ByteCursor<'_>) -> Result<Value, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?)
        .map_err(|_| TokenWireError::decode("cursor map entry count does not fit usize"))?;
    let mut entries = Vec::with_capacity(len);

    for _ in 0..len {
        entries.push((read_value(cursor)?, read_value(cursor)?));
    }

    Value::from_map(entries).map_err(|err| TokenWireError::decode(err.to_string()))
}
