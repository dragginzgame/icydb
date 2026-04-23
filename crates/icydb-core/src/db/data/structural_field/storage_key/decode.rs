use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_LIST, TAG_NULL, parse_binary_head as parse_structural_binary_head,
            skip_binary_value as skip_structural_binary_value,
            walk_binary_list_items as walk_structural_binary_list_items,
        },
        storage_key::{
            RelationKeyDecodeState,
            scalar::{
                decode_account_storage_key_binary_bytes, decode_int_storage_key_binary_bytes,
                decode_principal_storage_key_binary_bytes,
                decode_subaccount_storage_key_binary_bytes,
                decode_timestamp_storage_key_binary_bytes, decode_uint_storage_key_binary_bytes,
                decode_ulid_storage_key_binary_bytes, decode_unit_storage_key_binary_bytes,
            },
            supports_storage_key_binary_kind,
        },
    },
    model::field::FieldKind,
    value::{StorageKey, Value},
};

/// Decode one strong-relation field payload from Structural Binary v1 directly
/// into target storage keys.
pub(in crate::db) fn decode_relation_target_storage_keys_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => Ok(
            decode_optional_relation_storage_key_binary_bytes(raw_bytes, *key_kind)?
                .into_iter()
                .collect(),
        ),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            decode_relation_storage_key_binary_list_bytes(raw_bytes, **key_kind)
        }
        other => Err(FieldDecodeError::new(format!(
            "invalid strong relation field kind during structural binary key decode: {other:?}"
        ))),
    }
}

/// Decode one storage-key-compatible Structural Binary v1 field payload
/// directly into its canonical `StorageKey` form.
pub(in crate::db) fn decode_storage_key_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    match kind {
        FieldKind::Account => decode_account_storage_key_binary_bytes(raw_bytes),
        FieldKind::Int => decode_int_storage_key_binary_bytes(raw_bytes),
        FieldKind::Principal => decode_principal_storage_key_binary_bytes(raw_bytes),
        FieldKind::Relation { key_kind, .. } => {
            decode_storage_key_field_binary_bytes(raw_bytes, *key_kind)
        }
        FieldKind::Subaccount => decode_subaccount_storage_key_binary_bytes(raw_bytes),
        FieldKind::Timestamp => decode_timestamp_storage_key_binary_bytes(raw_bytes),
        FieldKind::Uint => decode_uint_storage_key_binary_bytes(raw_bytes),
        FieldKind::Ulid => decode_ulid_storage_key_binary_bytes(raw_bytes),
        FieldKind::Unit => decode_unit_storage_key_binary_bytes(raw_bytes),
        other => Err(FieldDecodeError::new(format!(
            "unsupported storage-key field kind during structural binary key decode: {other:?}"
        ))),
    }
}

/// Decode one optional storage-key-compatible Structural Binary v1 field
/// payload directly into its canonical `StorageKey` form.
pub(in crate::db) fn decode_optional_storage_key_field_binary_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<StorageKey>, FieldDecodeError> {
    if binary_payload_is_null(raw_bytes)? {
        return Ok(None);
    }

    decode_storage_key_field_binary_bytes(raw_bytes, kind).map(Some)
}

/// Decode one Structural Binary v1 storage-key-compatible field payload
/// directly into its semantic runtime value.
pub(in crate::db) fn decode_storage_key_binary_value_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Option<Value>, FieldDecodeError> {
    if !supports_storage_key_binary_kind(kind) {
        return Ok(None);
    }

    let value = match kind {
        FieldKind::Relation { key_kind, .. } => {
            decode_optional_relation_storage_key_binary_bytes(raw_bytes, *key_kind)?
                .map_or(Value::Null, |key| key.as_value())
        }
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => Value::List(
            decode_relation_storage_key_binary_list_bytes(raw_bytes, **key_kind)?
                .into_iter()
                .map(|key| key.as_value())
                .collect(),
        ),
        _ if binary_payload_is_null(raw_bytes)? => Value::Null,
        _ => decode_storage_key_field_binary_bytes(raw_bytes, kind)?.as_value(),
    };

    Ok(Some(value))
}

/// Validate one Structural Binary v1 storage-key-compatible field payload
/// without routing through the generic structural value lane.
pub(in crate::db) fn validate_storage_key_binary_value_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_storage_key_binary_kind(kind) {
        return Ok(false);
    }

    decode_storage_key_binary_value_bytes(raw_bytes, kind)?;

    Ok(true)
}

// Return whether one Structural Binary v1 payload is the explicit null form.
fn binary_payload_is_null(raw_bytes: &[u8]) -> Result<bool, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after field payload",
        ));
    }

    Ok(tag == TAG_NULL)
}

// Decode one singular relation payload from Structural Binary v1, treating
// explicit null as "no target".
fn decode_optional_relation_storage_key_binary_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Option<StorageKey>, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag == TAG_NULL {
        return Ok(None);
    }

    decode_relation_storage_key_binary_scalar_bytes(raw_bytes, key_kind).map(Some)
}

// Decode one list/set relation payload from Structural Binary v1 into
// canonical storage keys while preserving current null-item semantics.
fn decode_relation_storage_key_binary_list_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated binary value",
        ));
    };
    if tag == TAG_NULL {
        return Ok(Vec::new());
    }
    if tag != TAG_LIST {
        return Err(FieldDecodeError::new(
            "expected Structural Binary list for relation field",
        ));
    }

    let mut state = (Vec::new(), key_kind);
    walk_structural_binary_list_items(
        raw_bytes,
        "expected Structural Binary list for relation field",
        "structural binary: trailing bytes after relation field",
        (&raw mut state).cast(),
        push_relation_storage_key_binary_item,
    )?;

    Ok(state.0)
}

// Decode one relation-compatible scalar field payload from Structural Binary
// v1 into its storage-key form.
fn decode_relation_storage_key_binary_scalar_bytes(
    raw_bytes: &[u8],
    key_kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    decode_storage_key_field_binary_bytes(raw_bytes, key_kind)
}

// Push one Structural Binary relation-key list item into the decoded
// target-key buffer.
//
// Safety:
// `context` must be a valid `RelationKeyDecodeState`.
fn push_relation_storage_key_binary_item(
    item_bytes: &[u8],
    context: *mut (),
) -> Result<(), FieldDecodeError> {
    let state = unsafe { &mut *context.cast::<RelationKeyDecodeState>() };
    if let Some(value) = decode_optional_relation_storage_key_binary_bytes(item_bytes, state.1)? {
        state.0.push(value);
    }

    Ok(())
}
