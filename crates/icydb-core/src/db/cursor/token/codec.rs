//! Module: cursor::token::codec
//! Responsibility: custom binary wire codec for scalar and grouped cursor tokens.
//! Does not own: cursor validation policy or higher-level continuation planning.
//! Boundary: token-owned bounded wire encoding that avoids generic serde
//! serialization on the cursor runtime path.

use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, CursorBoundarySlot,
            token::bytes::{
                ByteCursor, checked_len_u32, write_len_prefixed_bytes, write_string, write_u32,
                write_u64,
            },
            token::scalar::{
                MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES, MAX_SCALAR_CURSOR_ORDER_TERMS,
            },
            token::value::{read_value, read_value_vec, write_value, write_value_slice},
            token::{
                ScalarOrderTermContract, ScalarPageMode, ScalarPageToken, ScalarPageTokenAuthority,
                ScalarPageTokenProgress, ScalarPageTokenWindow, TokenWireError,
            },
        },
        direction::Direction,
        index::IndexId,
        query::plan::{CardinalityTiebreakRoutePin, OrderDirection},
    },
    types::EntityTag,
    value::Value,
};
use sha2::{Digest, Sha256};

pub(in crate::db::cursor) const MAX_CURSOR_TOKEN_BYTES: usize = 8 * 1024;

const TOKEN_VARIANT_GROUPED: u8 = 2;
const TOKEN_VARIANT_SCALAR: u8 = 1;
const TOKEN_WIRE_MAGIC: &[u8; 4] = b"ICYQ";
const TOKEN_WIRE_VERSION: u8 = 1;
const SCALAR_TOKEN_WIRE_VERSION: u8 = 1;
const SCALAR_TOKEN_MAC_BYTES: usize = 32;
const SCALAR_TOKEN_HEADER_BYTES: usize = 10;

const PAGE_MODE_LIVE: u8 = 0;
const PAGE_MODE_EXHAUSTIVE: u8 = 1;
const ORDER_DIRECTION_ASC: u8 = 0;
const ORDER_DIRECTION_DESC: u8 = 1;
const NULL_COMPARISON_CANONICAL_V1: u8 = 1;
const SCALAR_ROUTE_PIN_LAYOUT_CURRENT: u8 = 0xA6;
const SLOT_MISSING: u8 = 0;
const SLOT_PRESENT: u8 = 1;

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

pub(in crate::db::cursor::token) fn encode_scalar_token(
    token: &ScalarPageToken,
    mac_key: &[u8; 32],
) -> Result<Vec<u8>, TokenWireError> {
    if token.order_terms().len() > MAX_SCALAR_CURSOR_ORDER_TERMS {
        return Err(TokenWireError::encode());
    }

    let mut payload = Vec::new();
    write_scalar_payload(&mut payload, token)?;
    let payload_len = checked_len_u32(payload.len())?;
    let mut out = Vec::with_capacity(
        SCALAR_TOKEN_HEADER_BYTES
            .saturating_add(payload.len())
            .saturating_add(SCALAR_TOKEN_MAC_BYTES),
    );
    out.extend_from_slice(TOKEN_WIRE_MAGIC);
    out.push(SCALAR_TOKEN_WIRE_VERSION);
    out.push(TOKEN_VARIANT_SCALAR);
    write_u32(&mut out, payload_len);
    out.extend_from_slice(payload.as_slice());
    let mac = hmac_sha256(mac_key, out.as_slice());
    out.extend_from_slice(&mac);

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

pub(in crate::db::cursor::token) fn decode_scalar_token(
    bytes: &[u8],
    mac_key: &[u8; 32],
) -> Result<ScalarPageToken, TokenWireError> {
    if bytes.len() > MAX_CURSOR_TOKEN_BYTES
        || bytes.len() < SCALAR_TOKEN_HEADER_BYTES + SCALAR_TOKEN_MAC_BYTES
    {
        return Err(TokenWireError::decode());
    }

    let mut framing = ByteCursor::new(bytes);
    if framing.read_array::<4>()? != *TOKEN_WIRE_MAGIC
        || framing.read_u8()? != SCALAR_TOKEN_WIRE_VERSION
        || framing.read_u8()? != TOKEN_VARIANT_SCALAR
    {
        return Err(TokenWireError::decode());
    }
    let payload_len = usize::try_from(framing.read_u32()?).map_err(|_| TokenWireError::decode())?;
    let authenticated_len = SCALAR_TOKEN_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(TokenWireError::decode)?;
    let total_len = authenticated_len
        .checked_add(SCALAR_TOKEN_MAC_BYTES)
        .ok_or_else(TokenWireError::decode)?;
    if total_len != bytes.len() {
        return Err(TokenWireError::decode());
    }
    let authenticated = bytes
        .get(..authenticated_len)
        .ok_or_else(TokenWireError::decode)?;
    let supplied_mac = bytes
        .get(authenticated_len..)
        .ok_or_else(TokenWireError::decode)?;
    let expected_mac = hmac_sha256(mac_key, authenticated);
    if !constant_time_eq(supplied_mac, &expected_mac) {
        return Err(TokenWireError::decode());
    }

    let payload = bytes
        .get(SCALAR_TOKEN_HEADER_BYTES..authenticated_len)
        .ok_or_else(TokenWireError::decode)?;
    read_scalar_payload(ByteCursor::new(payload))
}

fn write_scalar_payload(out: &mut Vec<u8>, token: &ScalarPageToken) -> Result<(), TokenWireError> {
    out.push(match token.mode() {
        ScalarPageMode::Live => PAGE_MODE_LIVE,
        ScalarPageMode::Exhaustive => PAGE_MODE_EXHAUSTIVE,
    });
    out.extend_from_slice(&token.signature().into_bytes());
    let authority = token.authority();
    out.extend_from_slice(&authority.database_incarnation());
    write_u64(out, authority.accepted_root_revision());
    out.push(authority.accepted_root_fingerprint_method());
    out.extend_from_slice(&authority.accepted_root_fingerprint());
    out.extend_from_slice(&authority.accepted_entity_fingerprint());
    write_u64(out, authority.entity_tag().value());
    out.push(SCALAR_ROUTE_PIN_LAYOUT_CURRENT);
    write_optional_cardinality_route_pin(out, token.route_pin(), authority.entity_tag())?;

    let window = token.window();
    write_u32(out, window.initial_offset());
    write_optional_u32(out, window.total_limit());
    write_u64(out, window.envelope_profile_identity());

    write_u32(out, checked_len_u32(token.order_terms().len())?);
    for term in token.order_terms() {
        write_string(out, term.identity())?;
        out.push(match term.direction() {
            OrderDirection::Asc => ORDER_DIRECTION_ASC,
            OrderDirection::Desc => ORDER_DIRECTION_DESC,
        });
        out.push(NULL_COMPARISON_CANONICAL_V1);
    }

    let progress = token.progress();
    write_optional_boundary(out, progress.last_emitted_logical())?;
    write_optional_bytes(out, progress.last_consumed_physical())?;
    write_optional_bytes(out, progress.unconsumed_lookahead())?;
    write_u64(out, progress.matching_rows_skipped());
    write_u64(out, progress.rows_emitted());
    Ok(())
}

fn read_scalar_payload(mut cursor: ByteCursor<'_>) -> Result<ScalarPageToken, TokenWireError> {
    let mode = match cursor.read_u8()? {
        PAGE_MODE_LIVE => ScalarPageMode::Live,
        PAGE_MODE_EXHAUSTIVE => ScalarPageMode::Exhaustive,
        _ => return Err(TokenWireError::decode()),
    };
    let signature = ContinuationSignature::from_bytes(cursor.read_array()?);
    let database_incarnation = cursor.read_array()?;
    let accepted_root_revision = cursor.read_u64()?;
    let accepted_root_fingerprint_method = cursor.read_u8()?;
    let accepted_root_fingerprint = cursor.read_array()?;
    let accepted_entity_fingerprint = cursor.read_array()?;
    let entity_tag = EntityTag::new(cursor.read_u64()?);
    let authority = ScalarPageTokenAuthority::new(
        database_incarnation,
        accepted_root_revision,
        accepted_root_fingerprint_method,
        accepted_root_fingerprint,
        accepted_entity_fingerprint,
        entity_tag,
    );
    if cursor.read_u8()? != SCALAR_ROUTE_PIN_LAYOUT_CURRENT {
        return Err(TokenWireError::decode());
    }
    let route_pin = read_optional_cardinality_route_pin(&mut cursor, entity_tag)?;

    let initial_offset = cursor.read_u32()?;
    let total_limit = read_optional_u32(&mut cursor)?;
    let envelope_profile_identity = cursor.read_u64()?;
    let window = ScalarPageTokenWindow::new(initial_offset, total_limit, envelope_profile_identity);

    let term_count = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
    if term_count > MAX_SCALAR_CURSOR_ORDER_TERMS || term_count > cursor.remaining() / 3 {
        return Err(TokenWireError::decode());
    }
    let mut order_terms = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        let identity = cursor.read_string()?;
        let direction = match cursor.read_u8()? {
            ORDER_DIRECTION_ASC => OrderDirection::Asc,
            ORDER_DIRECTION_DESC => OrderDirection::Desc,
            _ => return Err(TokenWireError::decode()),
        };
        if cursor.read_u8()? != NULL_COMPARISON_CANONICAL_V1 {
            return Err(TokenWireError::decode());
        }
        order_terms.push(ScalarOrderTermContract::new(identity, direction));
    }

    let boundary_start = cursor.remaining();
    let last_emitted_logical = read_optional_boundary(&mut cursor)?;
    if boundary_start.saturating_sub(cursor.remaining()) > MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES
    {
        return Err(TokenWireError::decode());
    }
    let last_consumed_physical = read_optional_bytes(&mut cursor)?;
    let unconsumed_lookahead = read_optional_bytes(&mut cursor)?;
    let matching_rows_skipped = cursor.read_u64()?;
    let rows_emitted = cursor.read_u64()?;
    cursor.finish()?;

    Ok(ScalarPageToken::new(
        mode,
        signature,
        authority,
        route_pin,
        window,
        order_terms,
        ScalarPageTokenProgress::new(
            last_emitted_logical,
            last_consumed_physical,
            unconsumed_lookahead,
            matching_rows_skipped,
            rows_emitted,
        ),
    ))
}

fn write_optional_cardinality_route_pin(
    out: &mut Vec<u8>,
    route_pin: Option<CardinalityTiebreakRoutePin>,
    entity_tag: EntityTag,
) -> Result<(), TokenWireError> {
    let Some(route_pin) = route_pin else {
        out.push(0);
        return Ok(());
    };
    if route_pin.index_id().entity_tag() != entity_tag {
        return Err(TokenWireError::encode());
    }
    out.push(1);
    out.extend_from_slice(&route_pin.index_id().to_bytes());
    out.push(route_pin.family().wire_tag());
    out.push(route_pin.consumed_prefix_arity());
    Ok(())
}

fn read_optional_cardinality_route_pin(
    cursor: &mut ByteCursor<'_>,
    entity_tag: EntityTag,
) -> Result<Option<CardinalityTiebreakRoutePin>, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => {
            let index_id = IndexId::from_bytes(cursor.read_exact(IndexId::STORED_SIZE_USIZE)?)
                .ok_or_else(TokenWireError::decode)?;
            let family =
                crate::db::query::plan::CardinalityTiebreakFamily::from_wire_tag(cursor.read_u8()?)
                    .ok_or_else(TokenWireError::decode)?;
            let prefix_arity = usize::from(cursor.read_u8()?);
            if index_id.entity_tag() != entity_tag {
                return Err(TokenWireError::decode());
            }
            CardinalityTiebreakRoutePin::new(index_id, family, prefix_arity)
                .map(Some)
                .ok_or_else(TokenWireError::decode)
        }
        _ => Err(TokenWireError::decode()),
    }
}

fn write_optional_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            out.push(1);
            write_u32(out, value);
        }
        None => out.push(0),
    }
}

fn read_optional_u32(cursor: &mut ByteCursor<'_>) -> Result<Option<u32>, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => cursor.read_u32().map(Some),
        _ => Err(TokenWireError::decode()),
    }
}

fn write_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), TokenWireError> {
    match value {
        Some(value) => {
            out.push(1);
            write_len_prefixed_bytes(out, value)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn read_optional_bytes(cursor: &mut ByteCursor<'_>) -> Result<Option<Vec<u8>>, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => cursor
            .read_len_prefixed_bytes()
            .map(<[u8]>::to_vec)
            .map(Some),
        _ => Err(TokenWireError::decode()),
    }
}

fn write_optional_boundary(
    out: &mut Vec<u8>,
    boundary: Option<&CursorBoundary>,
) -> Result<(), TokenWireError> {
    let Some(boundary) = boundary else {
        out.push(0);
        return Ok(());
    };
    out.push(1);
    write_u32(out, checked_len_u32(boundary.slots.len())?);
    let start = out.len();
    for slot in &boundary.slots {
        match slot {
            CursorBoundarySlot::Missing => out.push(SLOT_MISSING),
            CursorBoundarySlot::Present(value) => {
                out.push(SLOT_PRESENT);
                write_value(out, value)?;
            }
        }
    }
    if out.len().saturating_sub(start) > MAX_SCALAR_CURSOR_LOGICAL_BOUNDARY_BYTES {
        return Err(TokenWireError::encode());
    }
    Ok(())
}

fn read_optional_boundary(
    cursor: &mut ByteCursor<'_>,
) -> Result<Option<CursorBoundary>, TokenWireError> {
    match cursor.read_u8()? {
        0 => Ok(None),
        1 => {
            let len = usize::try_from(cursor.read_u32()?).map_err(|_| TokenWireError::decode())?;
            if len > MAX_SCALAR_CURSOR_ORDER_TERMS || len > cursor.remaining() {
                return Err(TokenWireError::decode());
            }
            let mut slots = Vec::with_capacity(len);
            for _ in 0..len {
                slots.push(match cursor.read_u8()? {
                    SLOT_MISSING => CursorBoundarySlot::Missing,
                    SLOT_PRESENT => CursorBoundarySlot::Present(read_value(cursor)?),
                    _ => return Err(TokenWireError::decode()),
                });
            }
            Ok(Some(CursorBoundary { slots }))
        }
        _ => Err(TokenWireError::decode()),
    }
}

fn hmac_sha256(key: &[u8; 32], message: &[u8]) -> [u8; 32] {
    const BLOCK_BYTES: usize = 64;
    let mut inner_key = [0x36_u8; BLOCK_BYTES];
    let mut outer_key = [0x5c_u8; BLOCK_BYTES];
    for (index, byte) in key.iter().copied().enumerate() {
        inner_key[index] ^= byte;
        outer_key[index] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_key);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_key);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::plan::CardinalityTiebreakFamily;

    const ROUTE_LAYOUT_OFFSET: usize = SCALAR_TOKEN_HEADER_BYTES
        + 1
        + 32
        + 16
        + std::mem::size_of::<u64>()
        + 1
        + 32
        + 16
        + std::mem::size_of::<u64>();
    const ROUTE_INDEX_OFFSET: usize = ROUTE_LAYOUT_OFFSET + 2;
    const ROUTE_FAMILY_OFFSET: usize = ROUTE_INDEX_OFFSET + IndexId::STORED_SIZE_USIZE;
    const ROUTE_ARITY_OFFSET: usize = ROUTE_FAMILY_OFFSET + 1;

    fn pinned_token() -> ScalarPageToken {
        ScalarPageToken::new(
            ScalarPageMode::Live,
            ContinuationSignature::from_bytes([0x11; 32]),
            ScalarPageTokenAuthority::new(
                [0x22; 16],
                7,
                1,
                [0x33; 32],
                [0x44; 16],
                EntityTag::new(9),
            ),
            CardinalityTiebreakRoutePin::new(
                IndexId::new_with_generation(EntityTag::new(9), 3, 4),
                CardinalityTiebreakFamily::BranchSet,
                2,
            ),
            ScalarPageTokenWindow::new(0, Some(2_048), 0x55),
            vec![ScalarOrderTermContract::new("id", OrderDirection::Asc)],
            ScalarPageTokenProgress::new(None, None, None, 0, 2),
        )
    }

    fn resign(mut authenticated: Vec<u8>, key: &[u8; 32]) -> Vec<u8> {
        let payload_len = u32::try_from(authenticated.len() - SCALAR_TOKEN_HEADER_BYTES)
            .expect("test payload length should fit");
        authenticated[6..SCALAR_TOKEN_HEADER_BYTES].copy_from_slice(&payload_len.to_be_bytes());
        let mac = hmac_sha256(key, authenticated.as_slice());
        authenticated.extend_from_slice(&mac);
        authenticated
    }

    fn authenticated_without_mac(key: &[u8; 32]) -> Vec<u8> {
        let encoded = pinned_token()
            .encode(key)
            .expect("current pinned token should encode");
        encoded[..encoded.len() - SCALAR_TOKEN_MAC_BYTES].to_vec()
    }

    #[test]
    fn authenticated_predecessor_scalar_layout_is_a_hard_cut() {
        let key = [0x66; 32];
        let mut predecessor = authenticated_without_mac(&key);
        predecessor.drain(ROUTE_LAYOUT_OFFSET..=ROUTE_ARITY_OFFSET);
        let predecessor = resign(predecessor, &key);

        assert!(ScalarPageToken::decode(predecessor.as_slice(), &key).is_err());
    }

    #[test]
    fn authenticated_route_pin_corruption_fails_closed() {
        let key = [0x66; 32];
        let mut foreign_entity = authenticated_without_mac(&key);
        foreign_entity[ROUTE_INDEX_OFFSET + std::mem::size_of::<u64>() - 1] ^= 1;
        assert!(ScalarPageToken::decode(resign(foreign_entity, &key).as_slice(), &key).is_err(),);

        let mut unknown_family = authenticated_without_mac(&key);
        unknown_family[ROUTE_FAMILY_OFFSET] = u8::MAX;
        assert!(ScalarPageToken::decode(resign(unknown_family, &key).as_slice(), &key).is_err(),);

        let mut zero_arity = authenticated_without_mac(&key);
        zero_arity[ROUTE_ARITY_OFFSET] = 0;
        assert!(ScalarPageToken::decode(resign(zero_arity, &key).as_slice(), &key).is_err());
    }
}
