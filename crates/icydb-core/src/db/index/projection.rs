//! Canonical index-component projection capability and value recovery.
//!
//! Planning consumes accepted-kind capability before traversal; execution uses
//! the matching decoder and preserves typed payload corruption at this owner.

use crate::{
    db::{index::decode_canonical_index_int64_component, schema::AcceptedFieldKind},
    error::InternalError,
    types::Ulid,
    value::{Value, ValueTag},
};

const COVERING_BOOL_PAYLOAD_LEN: usize = 1;
const COVERING_U64_PAYLOAD_LEN: usize = 8;
const COVERING_ULID_PAYLOAD_LEN: usize = 16;
const COVERING_TEXT_ESCAPE_PREFIX: u8 = 0x00;
const COVERING_TEXT_TERMINATOR: u8 = 0x00;
const COVERING_TEXT_ESCAPED_ZERO: u8 = 0xFF;

/// Return whether an accepted query-value kind can be recovered from the
/// existing canonical index-component projection codec. Primary-key values
/// and prefix-bound constants do not need this codec.
#[must_use]
pub(in crate::db) fn index_component_projection_supported(kind: &AcceptedFieldKind) -> bool {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            index_component_projection_supported(key_kind)
        }
        _ => matches!(
            kind,
            AcceptedFieldKind::Bool
                | AcceptedFieldKind::Int8
                | AcceptedFieldKind::Int16
                | AcceptedFieldKind::Int32
                | AcceptedFieldKind::Int64
                | AcceptedFieldKind::Nat8
                | AcceptedFieldKind::Nat16
                | AcceptedFieldKind::Nat32
                | AcceptedFieldKind::Nat64
                | AcceptedFieldKind::Text { .. }
                | AcceptedFieldKind::Ulid
                | AcceptedFieldKind::Unit
        ),
    }
}

// Decode one canonical covering-index component payload into one runtime
// `Value`. Returning `Ok(None)` keeps unsupported component kinds fail-closed
// at the caller boundary instead of guessing a lossy decode here.
pub(in crate::db) fn decode_covering_projection_component(
    component: &[u8],
) -> Result<Option<Value>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::bytes_covering_component_payload_empty());
    };

    if tag == ValueTag::Bool.to_u8() {
        return decode_covering_bool(payload);
    }
    if tag == ValueTag::Int64.to_u8() {
        return decode_canonical_index_int64_component(component)
            .map(Value::Int64)
            .map(Some);
    }
    if tag == ValueTag::Nat64.to_u8() {
        return decode_covering_u64(payload);
    }
    if tag == ValueTag::Text.to_u8() {
        return decode_covering_text(payload);
    }
    if tag == ValueTag::Ulid.to_u8() {
        return decode_covering_ulid(payload);
    }
    if tag == ValueTag::Unit.to_u8() {
        return Ok(Some(Value::Unit));
    }

    Ok(None)
}

fn decode_covering_bool(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some(value) = payload.first() else {
        return Err(InternalError::bytes_covering_bool_payload_truncated());
    };
    if payload.len() != COVERING_BOOL_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    match *value {
        0 => Ok(Some(Value::Bool(false))),
        1 => Ok(Some(Value::Bool(true))),
        _ => Err(InternalError::bytes_covering_bool_payload_invalid_value()),
    }
}

fn decode_covering_u64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_U64_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    let mut bytes = [0u8; COVERING_U64_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Nat64(u64::from_be_bytes(bytes))))
}

fn decode_covering_text(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    // Fast-path the common ordered-text encoding shape: raw UTF-8 bytes with
    // no embedded zeroes followed by the canonical `[0, 0]` terminator.
    if payload.len() >= 2
        && payload.ends_with(&[COVERING_TEXT_TERMINATOR, COVERING_TEXT_TERMINATOR])
        && !payload[..payload.len().saturating_sub(2)].contains(&COVERING_TEXT_ESCAPE_PREFIX)
    {
        let text = String::from_utf8(payload[..payload.len().saturating_sub(2)].to_vec())
            .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

        return Ok(Some(Value::Text(text)));
    }

    let mut bytes = Vec::new();
    let mut i = 0usize;

    while i < payload.len() {
        let byte = payload[i];
        if byte != COVERING_TEXT_ESCAPE_PREFIX {
            bytes.push(byte);
            i = i.saturating_add(1);
            continue;
        }

        let Some(next) = payload.get(i.saturating_add(1)).copied() else {
            return Err(InternalError::bytes_covering_text_payload_invalid_terminator());
        };
        match next {
            COVERING_TEXT_TERMINATOR => {
                i = i.saturating_add(2);
                if i != payload.len() {
                    return Err(InternalError::bytes_covering_text_payload_trailing_bytes());
                }

                let text = String::from_utf8(bytes)
                    .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

                return Ok(Some(Value::Text(text)));
            }
            COVERING_TEXT_ESCAPED_ZERO => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::bytes_covering_text_payload_invalid_escape_byte());
            }
        }
    }

    Err(InternalError::bytes_covering_text_payload_missing_terminator())
}

fn decode_covering_ulid(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_ULID_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    let mut bytes = [0u8; COVERING_ULID_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Ulid(Ulid::from_bytes(bytes))))
}

#[cfg(test)]
mod tests;
