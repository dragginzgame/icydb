//! Module: cursor::token::codec
//! Responsibility: custom binary wire codec for scalar and grouped cursor tokens.
//! Does not own: cursor validation policy or higher-level continuation planning.
//! Boundary: token-owned bounded wire encoding that avoids generic serde
//! serialization on the cursor runtime path.

use crate::{
    db::{
        cursor::{
            ContinuationSignature,
            token::TokenWireError,
            token::bytes::{ByteCursor, write_u32},
            token::value::{read_value_vec, write_value_slice},
        },
        direction::Direction,
    },
    value::Value,
};

pub(in crate::db::cursor) const MAX_CURSOR_TOKEN_BYTES: usize = 8 * 1024;

const TOKEN_VARIANT_GROUPED: u8 = 2;
const TOKEN_WIRE_MAGIC: &[u8; 4] = b"ICYQ";
const TOKEN_WIRE_VERSION: u8 = 1;

const DIRECTION_ASC: u8 = 0;
const DIRECTION_DESC: u8 = 1;

///
/// DecodedGroupedTokenPayload
///
/// DecodedGroupedTokenPayload is the decoded wire payload handed from the
/// token codec back to the grouped continuation token domain type.
///

pub(in crate::db::cursor::token) struct DecodedGroupedTokenPayload {
    pub(in crate::db::cursor::token) signature: ContinuationSignature,
    pub(in crate::db::cursor::token) last_group_key: Vec<Value>,
    pub(in crate::db::cursor::token) direction: Direction,
    pub(in crate::db::cursor::token) initial_offset: u32,
}

///
/// TOKEN ENCODE
///

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

pub(in crate::db::cursor::token) fn decode_grouped_token(
    bytes: &[u8],
) -> Result<DecodedGroupedTokenPayload, TokenWireError> {
    let mut cursor = start_token_decode(bytes)?;

    // Phase 1: validate the grouped token envelope and fixed-width header.
    expect_token_variant(&mut cursor, TOKEN_VARIANT_GROUPED)?;
    let signature = ContinuationSignature::from_bytes(cursor.read_array()?);
    let direction = read_direction(&mut cursor)?;
    let initial_offset = cursor.read_u32()?;

    // Phase 2: decode the grouped continuation key payload.
    let last_group_key = read_value_vec(&mut cursor)?;

    cursor.finish()?;

    Ok(DecodedGroupedTokenPayload {
        signature,
        last_group_key,
        direction,
        initial_offset,
    })
}

///
/// TOKEN HEADER
///

const fn start_token_decode(bytes: &[u8]) -> Result<ByteCursor<'_>, TokenWireError> {
    if bytes.len() > MAX_CURSOR_TOKEN_BYTES {
        return Err(TokenWireError::decode());
    }

    Ok(ByteCursor::new(bytes))
}

fn finish_token_encode(bytes: Vec<u8>) -> Result<Vec<u8>, TokenWireError> {
    if bytes.len() > MAX_CURSOR_TOKEN_BYTES {
        return Err(TokenWireError::encode());
    }

    Ok(bytes)
}

fn write_token_header(out: &mut Vec<u8>, variant: u8) {
    out.extend_from_slice(TOKEN_WIRE_MAGIC);
    out.push(TOKEN_WIRE_VERSION);
    out.push(variant);
}

fn expect_token_variant(
    cursor: &mut ByteCursor<'_>,
    expected_variant: u8,
) -> Result<(), TokenWireError> {
    let magic: [u8; TOKEN_WIRE_MAGIC.len()] = cursor.read_array()?;
    if &magic != TOKEN_WIRE_MAGIC {
        return Err(TokenWireError::decode());
    }

    let version = cursor.read_u8()?;
    if version != TOKEN_WIRE_VERSION {
        return Err(TokenWireError::decode());
    }

    let actual_variant = cursor.read_u8()?;
    if actual_variant != expected_variant {
        return Err(TokenWireError::decode());
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
        _ => Err(TokenWireError::decode()),
    }
}
