//! Module: data::structural_field::storage_key
//! Responsibility: direct persisted-field decoding into canonical `StorageKey` forms.
//! Does not own: generic runtime `Value` decode, composite `ByKind` recursion, or low-level CBOR walking.
//! Boundary: relation and index integrity paths call into this module when they need keys without rebuilding `Value`.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::cbor::walk_cbor_array_items;
use crate::db::data::structural_field::cbor::{
    decode_text_scalar_bytes, parse_tagged_cbor_head, skip_cbor_value,
};
use crate::db::data::structural_field::leaf::{
    decode_account_payload, decode_principal_payload, decode_subaccount_payload,
    decode_timestamp_payload,
};
use crate::{model::field::FieldKind, types::Ulid, value::StorageKey};

///
/// RelationKeyDecodeState
///
/// RelationKeyDecodeState carries the output buffer plus the relation key kind
/// while the shared CBOR array walker visits list/set relation items.
///
type RelationKeyDecodeState = (Vec<StorageKey>, FieldKind);

/// Decode one strong-relation field payload directly into target storage keys.
///
/// This keeps delete validation and reverse-index maintenance on structural
/// key forms without first rebuilding a runtime `Value` or `Value::List`.
pub(in crate::db) fn decode_relation_target_storage_keys_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => Ok(decode_optional_relation_storage_key_bytes(
            raw_bytes, *key_kind,
        )?
        .into_iter()
        .collect()),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            decode_relation_storage_key_list_bytes(raw_bytes, **key_kind)
        }
        other => Err(FieldDecodeError::new(format!(
            "invalid strong relation field kind during structural key decode: {other:?}"
        ))),
    }
}

/// Decode one storage-key-compatible field payload directly into its canonical
/// `StorageKey` form.
pub(in crate::db) fn decode_storage_key_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    match kind {
        FieldKind::Account => decode_account_storage_key_bytes(raw_bytes),
        FieldKind::Int => decode_int_storage_key_bytes(raw_bytes),
        FieldKind::Principal => decode_principal_storage_key_bytes(raw_bytes),
        FieldKind::Relation { key_kind, .. } => {
            decode_storage_key_field_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Subaccount => decode_subaccount_storage_key_bytes(raw_bytes),
        FieldKind::Timestamp => decode_timestamp_storage_key_bytes(raw_bytes),
        FieldKind::Uint => decode_uint_storage_key_bytes(raw_bytes),
        FieldKind::Ulid => decode_ulid_storage_key_bytes(raw_bytes),
        FieldKind::Unit => decode_unit_storage_key_bytes(raw_bytes),
        other => Err(FieldDecodeError::new(format!(
            "unsupported storage-key field kind during structural key decode: {other:?}"
        ))),
    }
}

// Decode one singular relation payload, treating explicit null as "no target".
fn decode_optional_relation_storage_key_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Option<StorageKey>, FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after relation field",
        ));
    }
    if major == 7 && argument == 22 {
        return Ok(None);
    }

    decode_relation_storage_key_scalar_bytes(raw_bytes, key_kind).map(Some)
}

// Decode one list/set relation payload into canonical storage keys while
// preserving current null-item semantics.
fn decode_relation_storage_key_list_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    let Some((major, argument, _cursor)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };
    if major == 7 && argument == 22 {
        return Ok(Vec::new());
    }
    if major != 4 {
        return Err(FieldDecodeError::new(
            "expected CBOR array for list/set field",
        ));
    }

    let mut state = (Vec::new(), key_kind);
    walk_cbor_array_items(
        raw_bytes,
        "expected CBOR array for list/set field",
        "typed CBOR: trailing bytes after list/set field",
        (&raw mut state).cast(),
        push_relation_storage_key_item,
    )?;

    Ok(state.0)
}

// Decode one relation-compatible scalar field payload into its storage-key form.
fn decode_relation_storage_key_scalar_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    decode_storage_key_field_bytes(raw_bytes, key_kind)
}

// Push one relation-key list item into the decoded target-key buffer.
//
// Safety:
// `context` must be a valid `RelationKeyDecodeState`.
fn push_relation_storage_key_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<RelationKeyDecodeState>() };
    if let Some(value) = decode_optional_relation_storage_key_bytes(item_bytes, state.1)? {
        state.0.push(value);
    }

    Ok(())
}

// Decode one account relation-key payload without routing through typed serde.
fn decode_account_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    decode_account_payload(raw_bytes).map(StorageKey::Account)
}

// Decode one timestamp relation-key payload without routing through typed serde.
fn decode_timestamp_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    decode_timestamp_payload(raw_bytes).map(StorageKey::Timestamp)
}

// Decode one principal relation-key payload without routing through typed serde.
fn decode_principal_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    decode_principal_payload(raw_bytes).map(StorageKey::Principal)
}

// Decode one subaccount relation-key payload without routing through typed serde.
fn decode_subaccount_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    decode_subaccount_payload(raw_bytes).map(StorageKey::Subaccount)
}

// Decode one ULID relation-key payload directly from its persisted CBOR text form.
fn decode_ulid_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((major, argument, payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated ulid payload"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after ulid payload",
        ));
    }
    if major != 3 {
        return Err(FieldDecodeError::new(
            "typed CBOR: invalid type, expected a text string",
        ));
    }

    Ulid::from_str(decode_text_scalar_bytes(
        raw_bytes,
        argument,
        payload_start,
    )?)
    .map(StorageKey::Ulid)
    .map_err(|_| FieldDecodeError::new("typed CBOR: invalid ulid string"))
}

// Decode one unit relation-key payload without routing through typed serde.
pub(super) fn decode_unit_storage_key_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated unit payload"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after unit payload",
        ));
    }
    if major != 7 || argument != 22 {
        return Err(FieldDecodeError::new(
            "typed CBOR: expected null for unit payload",
        ));
    }

    Ok(StorageKey::Unit)
}

// Decode one signed storage-key-compatible integer payload directly from CBOR.
fn decode_int_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after relation field",
        ));
    }

    let value = match major {
        0 => i64::try_from(argument).map_err(|_| {
            FieldDecodeError::new(format!(
                "typed CBOR: integer {argument} out of range for i64",
            ))
        })?,
        1 => {
            let signed = i64::try_from(argument).map_err(|_| {
                FieldDecodeError::new(format!(
                    "typed CBOR: integer -{} out of range for i64",
                    argument.saturating_add(1),
                ))
            })?;
            signed
                .checked_neg()
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| {
                    FieldDecodeError::new(format!(
                        "typed CBOR: integer -{} out of range for i64",
                        argument.saturating_add(1),
                    ))
                })?
        }
        _ => {
            return Err(FieldDecodeError::new(
                "typed CBOR: invalid type, expected an integer",
            ));
        }
    };

    Ok(StorageKey::Int(value))
}

// Decode one unsigned storage-key-compatible integer payload directly from CBOR.
fn decode_uint_storage_key_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((major, argument, _payload_start)) = parse_tagged_cbor_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new("typed CBOR: truncated CBOR value"));
    };
    let end = skip_cbor_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "typed CBOR: trailing bytes after relation field",
        ));
    }
    if major != 0 {
        return Err(FieldDecodeError::new(
            "typed CBOR: invalid type, expected an integer",
        ));
    }

    Ok(StorageKey::Uint(argument))
}
