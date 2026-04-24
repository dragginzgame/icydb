//! Module: cursor::token::codec
//! Responsibility: custom binary wire codec for scalar and grouped cursor tokens.
//! Does not own: cursor validation policy or higher-level continuation planning.
//! Boundary: token-owned bounded wire encoding that avoids generic serde
//! serialization on the cursor runtime path.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, CursorBoundarySlot, IndexRangeCursorAnchor,
            token::TokenWireError,
            token::bytes::{ByteCursor, checked_len_u32, write_len_prefixed_bytes, write_u32},
            token::value::{read_value, read_value_vec, write_value, write_value_slice},
        },
        direction::Direction,
    },
    value::Value,
};

pub(in crate::db::cursor) const MAX_CURSOR_TOKEN_BYTES: usize = 8 * 1024;

const TOKEN_VARIANT_SCALAR: u8 = 1;
const TOKEN_VARIANT_GROUPED: u8 = 2;
const TOKEN_WIRE_VERSION: u8 = 1;

const SLOT_MISSING: u8 = 0;
const SLOT_PRESENT: u8 = 1;

const DIRECTION_ASC: u8 = 0;
const DIRECTION_DESC: u8 = 1;

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

    finish_token_encode(out)
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

    finish_token_encode(out)
}

///
/// TOKEN DECODE
///

pub(in crate::db::cursor::token) fn decode_scalar_token(
    bytes: &[u8],
) -> Result<ScalarTokenParts, TokenWireError> {
    let mut cursor = start_token_decode(bytes)?;

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
    let mut cursor = start_token_decode(bytes)?;

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

fn start_token_decode(bytes: &[u8]) -> Result<ByteCursor<'_>, TokenWireError> {
    if bytes.len() > MAX_CURSOR_TOKEN_BYTES {
        return Err(TokenWireError::decode(format!(
            "cursor token exceeds max length: {} bytes (max {MAX_CURSOR_TOKEN_BYTES})",
            bytes.len()
        )));
    }

    Ok(ByteCursor::new(bytes))
}

fn finish_token_encode(bytes: Vec<u8>) -> Result<Vec<u8>, TokenWireError> {
    if bytes.len() > MAX_CURSOR_TOKEN_BYTES {
        return Err(TokenWireError::encode(format!(
            "cursor token exceeds max length: {} bytes (max {MAX_CURSOR_TOKEN_BYTES})",
            bytes.len()
        )));
    }

    Ok(bytes)
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
