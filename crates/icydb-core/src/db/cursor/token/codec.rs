//! Module: cursor::token::codec
//! Responsibility: custom binary wire codec for scalar and grouped cursor tokens.
//! Does not own: cursor validation policy or higher-level continuation planning.
//! Boundary: token-owned bounded wire encoding that avoids generic serde/CBOR
//! serialization on the cursor runtime path.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, CursorBoundarySlot, IndexRangeCursorAnchor,
        },
        direction::Direction,
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint};
use std::str;

use crate::db::cursor::token::TokenWireError;

pub(in crate::db::cursor::token) const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;
pub(in crate::db::cursor::token) const MAX_GROUPED_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

const TOKEN_VARIANT_SCALAR: u8 = 1;
const TOKEN_VARIANT_GROUPED: u8 = 2;
const TOKEN_WIRE_VERSION: u8 = 1;

const SLOT_MISSING: u8 = 0;
const SLOT_PRESENT: u8 = 1;

const DIRECTION_ASC: u8 = 0;
const DIRECTION_DESC: u8 = 1;

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

///
/// ByteCursor
///
/// ByteCursor is the bounded decode reader for the token-owned binary wire
/// format. It never panics on malformed input and reports every truncation or
/// type mismatch through `TokenWireError`.
///

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    // Start one bounded decode cursor over the provided byte slice.
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    // Return the remaining unread byte count.
    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    // Read one exact byte window, advancing only on success.
    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], TokenWireError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| TokenWireError::decode("cursor token length overflow"))?;

        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(TokenWireError::decode(format!(
                "cursor token truncated: needed {len} bytes with {} remaining",
                self.remaining()
            )));
        };

        self.offset = end;
        Ok(slice)
    }

    // Read one fixed-width primitive through an exact byte window.
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], TokenWireError> {
        let bytes = self.read_exact(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    // Read one tagged byte.
    fn read_u8(&mut self) -> Result<u8, TokenWireError> {
        Ok(self.read_exact(1)?[0])
    }

    // Read one big-endian u32.
    fn read_u32(&mut self) -> Result<u32, TokenWireError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian u64.
    fn read_u64(&mut self) -> Result<u64, TokenWireError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian i64.
    fn read_i64(&mut self) -> Result<i64, TokenWireError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian i128.
    fn read_i128(&mut self) -> Result<i128, TokenWireError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }

    // Read one big-endian u128.
    fn read_u128(&mut self) -> Result<u128, TokenWireError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    // Read one length-prefixed byte payload.
    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], TokenWireError> {
        let len = usize::try_from(self.read_u32()?)
            .map_err(|_| TokenWireError::decode("cursor token length does not fit usize"))?;

        self.read_exact(len)
    }

    // Read one UTF-8 string from a length-prefixed byte payload.
    fn read_string(&mut self) -> Result<String, TokenWireError> {
        let bytes = self.read_len_prefixed_bytes()?;
        let text = str::from_utf8(bytes)
            .map_err(|err| TokenWireError::decode(format!("cursor token invalid utf-8: {err}")))?;

        Ok(text.to_string())
    }

    // Require full cursor consumption at the end of decode.
    fn finish(self) -> Result<(), TokenWireError> {
        if self.remaining() == 0 {
            return Ok(());
        }

        Err(TokenWireError::decode(format!(
            "cursor token has {} trailing bytes",
            self.remaining()
        )))
    }
}

///
/// ScalarTokenParts
///
/// ScalarTokenParts is the decode handoff from the token wire layer back to the
/// scalar continuation token domain type.
///

pub(in crate::db::cursor::token) struct ScalarTokenParts {
    pub(in crate::db::cursor::token) signature: ContinuationSignature,
    pub(in crate::db::cursor::token) boundary: CursorBoundary,
    pub(in crate::db::cursor::token) direction: Direction,
    pub(in crate::db::cursor::token) initial_offset: u32,
    pub(in crate::db::cursor::token) index_range_anchor: Option<IndexRangeCursorAnchor>,
}

///
/// GroupedTokenParts
///
/// GroupedTokenParts is the decode handoff from the token wire layer back to
/// the grouped continuation token domain type.
///

pub(in crate::db::cursor::token) struct GroupedTokenParts {
    pub(in crate::db::cursor::token) signature: ContinuationSignature,
    pub(in crate::db::cursor::token) last_group_key: Vec<Value>,
    pub(in crate::db::cursor::token) direction: Direction,
    pub(in crate::db::cursor::token) initial_offset: u32,
}

///
/// TOKEN ENCODE
///

pub(in crate::db::cursor::token) fn encode_scalar_token(
    signature: ContinuationSignature,
    boundary: &CursorBoundary,
    direction: Direction,
    initial_offset: u32,
    index_range_anchor: Option<&IndexRangeCursorAnchor>,
) -> Result<Vec<u8>, TokenWireError> {
    let mut out = Vec::new();

    // Phase 1: write the scalar token envelope header and fixed fields.
    write_token_header(&mut out, TOKEN_VARIANT_SCALAR);
    out.extend_from_slice(&signature.into_bytes());
    write_direction(&mut out, direction);
    write_u32(&mut out, initial_offset);

    // Phase 2: encode the scalar boundary slots and optional range anchor.
    write_cursor_boundary(&mut out, boundary)?;
    write_optional_anchor(&mut out, index_range_anchor)?;

    Ok(out)
}

pub(in crate::db::cursor::token) fn encode_grouped_token(
    signature: ContinuationSignature,
    last_group_key: &[Value],
    direction: Direction,
    initial_offset: u32,
) -> Result<Vec<u8>, TokenWireError> {
    let mut out = Vec::new();

    // Phase 1: write the grouped token envelope header and fixed fields.
    write_token_header(&mut out, TOKEN_VARIANT_GROUPED);
    out.extend_from_slice(&signature.into_bytes());
    write_direction(&mut out, direction);
    write_u32(&mut out, initial_offset);

    // Phase 2: encode the grouped continuation key tuple.
    write_value_slice(&mut out, last_group_key)?;

    Ok(out)
}

///
/// TOKEN DECODE
///

pub(in crate::db::cursor::token) fn decode_scalar_token(
    bytes: &[u8],
) -> Result<ScalarTokenParts, TokenWireError> {
    let mut cursor = start_token_decode(bytes, MAX_CONTINUATION_TOKEN_BYTES)?;

    // Phase 1: validate the scalar token envelope and fixed-width header.
    expect_token_variant(&mut cursor, TOKEN_VARIANT_SCALAR)?;
    let signature = ContinuationSignature::from_bytes(cursor.read_array()?);
    let direction = read_direction(&mut cursor)?;
    let initial_offset = cursor.read_u32()?;

    // Phase 2: decode the scalar boundary payload and optional range anchor.
    let boundary = read_cursor_boundary(&mut cursor)?;
    let index_range_anchor = read_optional_anchor(&mut cursor)?;

    cursor.finish()?;

    Ok(ScalarTokenParts {
        signature,
        boundary,
        direction,
        initial_offset,
        index_range_anchor,
    })
}

pub(in crate::db::cursor::token) fn decode_grouped_token(
    bytes: &[u8],
) -> Result<GroupedTokenParts, TokenWireError> {
    let mut cursor = start_token_decode(bytes, MAX_GROUPED_CONTINUATION_TOKEN_BYTES)?;

    // Phase 1: validate the grouped token envelope and fixed-width header.
    expect_token_variant(&mut cursor, TOKEN_VARIANT_GROUPED)?;
    let signature = ContinuationSignature::from_bytes(cursor.read_array()?);
    let direction = read_direction(&mut cursor)?;
    let initial_offset = cursor.read_u32()?;

    // Phase 2: decode the grouped continuation key payload.
    let last_group_key = read_value_vec(&mut cursor)?;

    cursor.finish()?;

    Ok(GroupedTokenParts {
        signature,
        last_group_key,
        direction,
        initial_offset,
    })
}

///
/// TOKEN HEADER
///

fn start_token_decode(bytes: &[u8], max_bytes: usize) -> Result<ByteCursor<'_>, TokenWireError> {
    if bytes.len() > max_bytes {
        return Err(TokenWireError::decode(format!(
            "cursor token exceeds max length: {} bytes (max {max_bytes})",
            bytes.len()
        )));
    }

    Ok(ByteCursor::new(bytes))
}

fn write_token_header(out: &mut Vec<u8>, variant: u8) {
    out.push(TOKEN_WIRE_VERSION);
    out.push(variant);
}

fn expect_token_variant(
    cursor: &mut ByteCursor<'_>,
    expected_variant: u8,
) -> Result<(), TokenWireError> {
    let version = cursor.read_u8()?;
    if version != TOKEN_WIRE_VERSION {
        return Err(TokenWireError::decode(format!(
            "unsupported cursor token wire version {version}"
        )));
    }

    let actual_variant = cursor.read_u8()?;
    if actual_variant != expected_variant {
        return Err(TokenWireError::decode(format!(
            "cursor token variant mismatch: expected {expected_variant}, found {actual_variant}"
        )));
    }

    Ok(())
}

///
/// PRIMITIVE WRITE HELPERS
///

fn checked_len_u32(len: usize) -> Result<u32, TokenWireError> {
    u32::try_from(len)
        .map_err(|_| TokenWireError::encode("cursor token payload exceeds u32 length"))
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i128(out: &mut Vec<u8>, value: i128) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_len_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(bytes.len())?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), TokenWireError> {
    write_len_prefixed_bytes(out, value.as_bytes())
}

///
/// DIRECTION AND ANCHOR HELPERS
///

fn write_direction(out: &mut Vec<u8>, direction: Direction) {
    out.push(match direction {
        Direction::Asc => DIRECTION_ASC,
        Direction::Desc => DIRECTION_DESC,
    });
}

fn read_direction(cursor: &mut ByteCursor<'_>) -> Result<Direction, TokenWireError> {
    match cursor.read_u8()? {
        DIRECTION_ASC => Ok(Direction::Asc),
        DIRECTION_DESC => Ok(Direction::Desc),
        other => Err(TokenWireError::decode(format!(
            "unsupported cursor direction tag {other}"
        ))),
    }
}

fn write_optional_anchor(
    out: &mut Vec<u8>,
    anchor: Option<&IndexRangeCursorAnchor>,
) -> Result<(), TokenWireError> {
    match anchor {
        Some(anchor) => {
            out.push(1);
            write_len_prefixed_bytes(out, anchor.last_raw_key())?;
        }
        None => out.push(0),
    }

    Ok(())
}

fn read_optional_anchor(
    cursor: &mut ByteCursor<'_>,
) -> Result<Option<IndexRangeCursorAnchor>, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(IndexRangeCursorAnchor::new(
            cursor.read_len_prefixed_bytes()?.to_vec(),
        ))),
        other => Err(TokenWireError::decode(format!(
            "unsupported cursor anchor presence tag {other}"
        ))),
    }
}

///
/// CURSOR BOUNDARY HELPERS
///

fn write_cursor_boundary(
    out: &mut Vec<u8>,
    boundary: &CursorBoundary,
) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(boundary.slots.len())?);

    for slot in &boundary.slots {
        match slot {
            CursorBoundarySlot::Missing => out.push(SLOT_MISSING),
            CursorBoundarySlot::Present(value) => {
                out.push(SLOT_PRESENT);
                write_value(out, value)?;
            }
        }
    }

    Ok(())
}

fn read_cursor_boundary(cursor: &mut ByteCursor<'_>) -> Result<CursorBoundary, TokenWireError> {
    let slot_count = usize::try_from(cursor.read_u32()?)
        .map_err(|_| TokenWireError::decode("cursor boundary slot count does not fit usize"))?;
    let mut slots = Vec::with_capacity(slot_count);

    for _ in 0..slot_count {
        match cursor.read_u8()? {
            SLOT_MISSING => slots.push(CursorBoundarySlot::Missing),
            SLOT_PRESENT => slots.push(CursorBoundarySlot::Present(read_value(cursor)?)),
            other => {
                return Err(TokenWireError::decode(format!(
                    "unsupported cursor boundary slot tag {other}"
                )));
            }
        }
    }

    Ok(CursorBoundary { slots })
}

///
/// VALUE VECTOR HELPERS
///

fn write_value_slice(out: &mut Vec<u8>, values: &[Value]) -> Result<(), TokenWireError> {
    write_u32(out, checked_len_u32(values.len())?);

    for value in values {
        write_value(out, value)?;
    }

    Ok(())
}

fn read_value_vec(cursor: &mut ByteCursor<'_>) -> Result<Vec<Value>, TokenWireError> {
    let len = usize::try_from(cursor.read_u32()?)
        .map_err(|_| TokenWireError::decode("cursor value count does not fit usize"))?;
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        values.push(read_value(cursor)?);
    }

    Ok(values)
}

///
/// VALUE ENCODE
///

// One recursive dispatcher owns every token-supported `Value` leaf shape so
// cursor-token encoding keeps a single authoritative variant map.
#[expect(clippy::too_many_lines)]
fn write_value(out: &mut Vec<u8>, value: &Value) -> Result<(), TokenWireError> {
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

///
/// VALUE DECODE
///

fn read_value(cursor: &mut ByteCursor<'_>) -> Result<Value, TokenWireError> {
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
