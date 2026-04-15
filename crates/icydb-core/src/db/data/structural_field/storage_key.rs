//! Module: data::structural_field::storage_key
//! Responsibility: direct persisted-field decoding into canonical `StorageKey` forms.
//! Does not own: generic runtime `Value` decode, composite `ByKind` recursion, or low-level structural binary walking.
//! Boundary: relation and index integrity paths call into this module when they need keys without rebuilding `Value`.

use crate::db::data::structural_field::FieldDecodeError;
use crate::db::data::structural_field::binary::{
    TAG_BYTES, TAG_INT64, TAG_LIST, TAG_NULL, TAG_UINT64, TAG_UNIT,
    parse_binary_head as parse_structural_binary_head, payload_bytes as binary_payload_bytes,
    push_binary_bytes, push_binary_int64, push_binary_list_len, push_binary_null,
    push_binary_uint64, push_binary_unit, skip_binary_value as skip_structural_binary_value,
    walk_binary_list_items as walk_structural_binary_list_items,
};
use crate::{
    error::InternalError,
    model::field::FieldKind,
    types::Ulid,
    value::{StorageKey, Value},
};

///
/// RelationKeyDecodeState
///
/// RelationKeyDecodeState carries the output buffer plus the relation key kind
/// while the Structural Binary v1 list walker visits relation items.
///
type RelationKeyDecodeState = (Vec<StorageKey>, FieldKind);

/// Return whether this field kind is owned by the Structural Binary v1
/// storage-key lane.
pub(in crate::db) const fn supports_storage_key_binary_kind(kind: FieldKind) -> bool {
    match kind {
        FieldKind::Account
        | FieldKind::Int
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Ulid
        | FieldKind::Unit => true,
        FieldKind::Relation { key_kind, .. } => supports_storage_key_binary_kind(*key_kind),
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            supports_storage_key_binary_kind(**key_kind)
        }
        _ => false,
    }
}

/// Decode one strong-relation field payload directly into target storage keys.
///
/// This keeps delete validation and reverse-index maintenance on structural
/// key forms without first rebuilding a runtime `Value` or `Value::List`.
pub(in crate::db) fn decode_relation_target_storage_keys_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Vec<StorageKey>, FieldDecodeError> {
    decode_relation_target_storage_keys_binary_bytes(raw_bytes, kind)
}

/// Decode one storage-key-compatible field payload directly into its canonical
/// `StorageKey` form.
pub(in crate::db) fn decode_storage_key_field_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<StorageKey, FieldDecodeError> {
    decode_storage_key_field_binary_bytes(raw_bytes, kind)
}

/// Decode one strong-relation field payload from Structural Binary v1 directly
/// into target storage keys.
#[cfg_attr(not(test), allow(dead_code))]
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
#[cfg_attr(not(test), allow(dead_code))]
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

/// Encode strong-relation target keys into the owner-local Structural Binary
/// v1 storage-key lane.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn encode_relation_target_storage_keys_binary_bytes(
    keys: &[StorageKey],
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_relation_target_storage_keys_binary_into(&mut encoded, keys, kind, field_name)?;

    Ok(encoded)
}

/// Encode one canonical `StorageKey` into the owner-local Structural Binary v1
/// storage-key lane.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn encode_storage_key_field_binary_bytes(
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    encode_storage_key_field_binary_into(&mut encoded, key, kind, field_name)?;

    Ok(encoded)
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

    let _ = decode_storage_key_binary_value_bytes(raw_bytes, kind)?;
    Ok(true)
}

/// Encode one storage-key-compatible runtime value through the owner-local
/// Structural Binary v1 lane.
pub(in crate::db) fn encode_storage_key_binary_value_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Option<Vec<u8>>, InternalError> {
    if !supports_storage_key_binary_kind(kind) {
        return Ok(None);
    }

    let encoded = match kind {
        FieldKind::Relation { .. } => {
            let keys = match value {
                Value::Null => Vec::new(),
                value => vec![StorageKey::try_from_value(value).map_err(|err| {
                    InternalError::persisted_row_field_encode_failed(field_name, err)
                })?],
            };
            encode_relation_target_storage_keys_binary_bytes(&keys, kind, field_name)?
        }
        FieldKind::List(FieldKind::Relation { .. })
        | FieldKind::Set(FieldKind::Relation { .. }) => {
            let Value::List(items) = value else {
                return Err(InternalError::persisted_row_field_encode_failed(
                    field_name,
                    format!("field kind {kind:?} does not accept runtime value {value:?}"),
                ));
            };
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                keys.push(StorageKey::try_from_value(item).map_err(|err| {
                    InternalError::persisted_row_field_encode_failed(field_name, err)
                })?);
            }
            encode_relation_target_storage_keys_binary_bytes(&keys, kind, field_name)?
        }
        _ if matches!(value, Value::Null) => {
            let mut encoded = Vec::new();
            push_binary_null(&mut encoded);
            encoded
        }
        _ => encode_storage_key_field_binary_bytes(
            StorageKey::try_from_value(value)
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))?,
            kind,
            field_name,
        )?,
    };

    Ok(Some(encoded))
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

// Encode one strong-relation field into the storage-key Structural Binary v1
// lane without routing through runtime `Value`.
fn encode_relation_target_storage_keys_binary_into(
    out: &mut Vec<u8>,
    keys: &[StorageKey],
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match kind {
        FieldKind::Relation { key_kind, .. } => match keys {
            [] => {
                push_binary_null(out);
                Ok(())
            }
            [key] => encode_storage_key_field_binary_into(out, *key, *key_kind, field_name),
            _ => Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                "singular relation field received more than one target key",
            )),
        },
        FieldKind::List(FieldKind::Relation { key_kind, .. })
        | FieldKind::Set(FieldKind::Relation { key_kind, .. }) => {
            push_binary_list_len(out, keys.len());
            for key in keys {
                encode_storage_key_field_binary_into(out, *key, **key_kind, field_name)?;
            }

            Ok(())
        }
        other => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!(
                "invalid strong relation field kind during structural binary encode: {other:?}"
            ),
        )),
    }
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

// Encode one storage-key-compatible field into the owner-local Structural
// Binary v1 storage-key lane.
fn encode_storage_key_field_binary_into(
    out: &mut Vec<u8>,
    key: StorageKey,
    kind: FieldKind,
    field_name: &str,
) -> Result<(), InternalError> {
    match (kind, key) {
        (FieldKind::Account, StorageKey::Account(value)) => {
            push_binary_list_len(out, 2);
            push_binary_bytes(out, value.owner().as_slice());
            match value.subaccount() {
                Some(subaccount) => push_binary_bytes(out, subaccount.as_slice()),
                None => push_binary_null(out),
            }
            Ok(())
        }
        (FieldKind::Int, StorageKey::Int(value)) => {
            push_binary_int64(out, value);
            Ok(())
        }
        (FieldKind::Principal, StorageKey::Principal(value)) => {
            push_binary_bytes(out, value.as_slice());
            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, key) => {
            encode_storage_key_field_binary_into(out, key, *key_kind, field_name)
        }
        (FieldKind::Subaccount, StorageKey::Subaccount(value)) => {
            push_binary_bytes(out, value.as_slice());
            Ok(())
        }
        (FieldKind::Timestamp, StorageKey::Timestamp(value)) => {
            push_binary_int64(out, value.as_millis());
            Ok(())
        }
        (FieldKind::Uint, StorageKey::Uint(value)) => {
            push_binary_uint64(out, value);
            Ok(())
        }
        (FieldKind::Ulid, StorageKey::Ulid(value)) => {
            push_binary_bytes(out, &value.to_bytes());
            Ok(())
        }
        (FieldKind::Unit, StorageKey::Unit) => {
            push_binary_unit(out);
            Ok(())
        }
        (other, key) => Err(InternalError::persisted_row_field_encode_failed(
            field_name,
            format!("field kind {other:?} does not accept storage key {key:?}"),
        )),
    }
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

// Decode one account relation-key payload from Structural Binary v1 without
// routing through generic value decode.
fn decode_account_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated account payload",
        ));
    };
    if tag != TAG_LIST || len != 2 {
        return Err(FieldDecodeError::new(
            "structural binary: expected two-item account payload",
        ));
    }

    let owner_start = payload_start;
    let owner_end = skip_structural_binary_value(raw_bytes, owner_start)?;
    let sub_start = owner_end;
    let sub_end = skip_structural_binary_value(raw_bytes, sub_start)?;
    if sub_end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after account payload",
        ));
    }

    let StorageKey::Principal(owner) =
        decode_principal_storage_key_binary_bytes(&raw_bytes[owner_start..owner_end])?
    else {
        unreachable!("principal key decode must return a principal");
    };
    let subaccount = if let Some((tag, _len, _payload_start)) =
        parse_structural_binary_head(&raw_bytes[sub_start..sub_end], 0)?
    {
        if tag == TAG_NULL {
            None
        } else {
            match decode_subaccount_storage_key_binary_bytes(&raw_bytes[sub_start..sub_end])? {
                StorageKey::Subaccount(value) => Some(value),
                _ => unreachable!("subaccount key decode must return a subaccount"),
            }
        }
    } else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated subaccount payload",
        ));
    };

    Ok(StorageKey::Account(crate::types::Account::from_parts(
        owner, subaccount,
    )))
}

// Decode one timestamp relation-key payload from Structural Binary v1.
fn decode_timestamp_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated timestamp payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after timestamp payload",
        ));
    }
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 timestamp payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "timestamp")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid timestamp payload"))?;

    Ok(StorageKey::Timestamp(crate::types::Timestamp::from_millis(
        i64::from_be_bytes(payload),
    )))
}

// Decode one principal relation-key payload from Structural Binary v1.
fn decode_principal_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated principal payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after principal payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes principal payload",
        ));
    }

    crate::types::Principal::try_from_bytes(binary_payload_bytes(
        raw_bytes,
        len,
        payload_start,
        "principal",
    )?)
    .map(StorageKey::Principal)
    .map_err(|_| FieldDecodeError::new("structural binary: invalid principal payload"))
}

// Decode one subaccount relation-key payload from Structural Binary v1.
fn decode_subaccount_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated subaccount payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after subaccount payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes subaccount payload",
        ));
    }
    let payload = binary_payload_bytes(raw_bytes, len, payload_start, "subaccount")?;
    let bytes: [u8; 32] = payload
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid subaccount payload"))?;

    Ok(StorageKey::Subaccount(
        crate::types::Subaccount::from_array(bytes),
    ))
}

// Decode one ULID relation-key payload directly from its fixed-width Structural
// Binary bytes form.
fn decode_ulid_storage_key_binary_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated ulid payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after ulid payload",
        ));
    }
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new(
            "structural binary: expected bytes ulid payload",
        ));
    }

    Ulid::try_from_bytes(binary_payload_bytes(raw_bytes, len, payload_start, "ulid")?)
        .map(StorageKey::Ulid)
        .map_err(|_| FieldDecodeError::new("structural binary: invalid ulid payload"))
}

// Decode one unit relation-key payload from Structural Binary v1.
fn decode_unit_storage_key_binary_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, _len, _payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated unit payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after unit payload",
        ));
    }
    if tag != TAG_UNIT {
        return Err(FieldDecodeError::new(
            "structural binary: expected unit payload",
        ));
    }

    Ok(StorageKey::Unit)
}

// Decode one signed storage-key-compatible integer payload from Structural
// Binary v1.
fn decode_int_storage_key_binary_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_INT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected i64 integer payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid i64 payload"))?;

    Ok(StorageKey::Int(i64::from_be_bytes(payload)))
}

// Decode one unsigned storage-key-compatible integer payload from Structural
// Binary v1.
fn decode_uint_storage_key_binary_bytes(raw_bytes: &[u8]) -> Result<StorageKey, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated integer payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_UINT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected u64 integer payload",
        ));
    }
    let payload: [u8; 8] = binary_payload_bytes(raw_bytes, len, payload_start, "integer")?
        .try_into()
        .map_err(|_| FieldDecodeError::new("structural binary: invalid u64 payload"))?;

    Ok(StorageKey::Uint(u64::from_be_bytes(payload)))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        decode_relation_target_storage_keys_binary_bytes, decode_storage_key_binary_value_bytes,
        decode_storage_key_field_binary_bytes, encode_relation_target_storage_keys_binary_bytes,
        encode_storage_key_binary_value_bytes, encode_storage_key_field_binary_bytes,
        validate_storage_key_binary_value_bytes,
    };
    use crate::{
        model::field::{FieldKind, RelationStrength},
        types::{Account, EntityTag, Principal, Subaccount, Timestamp, Ulid},
        value::{StorageKey, Value},
    };

    static RELATION_ULID_KEY_KIND: FieldKind = FieldKind::Ulid;
    static STRONG_RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "RelationTargetEntity",
        target_entity_name: "RelationTargetEntity",
        target_entity_tag: EntityTag::new(7),
        target_store_path: "RelationTargetStore",
        key_kind: &RELATION_ULID_KEY_KIND,
        strength: RelationStrength::Strong,
    };
    static STRONG_RELATION_LIST_KIND: FieldKind = FieldKind::List(&STRONG_RELATION_KIND);

    const TAG_NULL: u8 = 0x00;
    const TAG_UNIT: u8 = 0x01;
    const TAG_UINT64: u8 = 0x10;
    const TAG_INT64: u8 = 0x11;
    const TAG_TEXT: u8 = 0x12;
    const TAG_BYTES: u8 = 0x13;
    const TAG_LIST: u8 = 0x20;

    fn encode_null() -> Vec<u8> {
        vec![TAG_NULL]
    }

    fn encode_unit() -> Vec<u8> {
        vec![TAG_UNIT]
    }

    fn encode_uint64(value: u64) -> Vec<u8> {
        let mut out = vec![TAG_UINT64];
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    fn encode_int64(value: i64) -> Vec<u8> {
        let mut out = vec![TAG_INT64];
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    fn encode_text(value: &str) -> Vec<u8> {
        let mut out = vec![TAG_TEXT];
        out.extend_from_slice(
            &u32::try_from(value.len())
                .expect("text len fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn encode_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = vec![TAG_BYTES];
        out.extend_from_slice(
            &u32::try_from(value.len())
                .expect("byte len fits u32")
                .to_be_bytes(),
        );
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn storage_key_binary_roundtrips_all_supported_scalar_kinds() {
        let account = Account::from_parts(Principal::dummy(3), Some(Subaccount::from([3_u8; 32])));
        let timestamp = Timestamp::from_millis(1_710_013_530_123);
        let ulid = Ulid::from_u128(77);
        let cases = vec![
            (
                FieldKind::Account,
                StorageKey::Account(account),
                Value::Account(account),
            ),
            (FieldKind::Int, StorageKey::Int(-9), Value::Int(-9)),
            (
                FieldKind::Principal,
                StorageKey::Principal(Principal::dummy(5)),
                Value::Principal(Principal::dummy(5)),
            ),
            (
                FieldKind::Subaccount,
                StorageKey::Subaccount(Subaccount::from([8_u8; 32])),
                Value::Subaccount(Subaccount::from([8_u8; 32])),
            ),
            (
                FieldKind::Timestamp,
                StorageKey::Timestamp(timestamp),
                Value::Timestamp(timestamp),
            ),
            (FieldKind::Uint, StorageKey::Uint(42), Value::Uint(42)),
            (FieldKind::Ulid, StorageKey::Ulid(ulid), Value::Ulid(ulid)),
            (FieldKind::Unit, StorageKey::Unit, Value::Unit),
        ];

        for (kind, key, value) in cases {
            let encoded = encode_storage_key_field_binary_bytes(key, kind, "field")
                .expect("storage-key payload should encode");
            let decoded_key = decode_storage_key_field_binary_bytes(encoded.as_slice(), kind)
                .expect("storage-key payload should decode");
            let decoded_value = decode_storage_key_binary_value_bytes(encoded.as_slice(), kind)
                .expect("storage-key value decode should succeed")
                .expect("supported kind should stay on the storage-key lane");

            assert!(
                validate_storage_key_binary_value_bytes(encoded.as_slice(), kind)
                    .expect("storage-key payload should validate"),
                "supported storage-key kind should validate as storage-key-owned"
            );
            assert_eq!(decoded_key, key, "decoded key mismatch for {kind:?}");
            assert_eq!(decoded_value, value, "decoded value mismatch for {kind:?}");
        }
    }

    #[test]
    fn storage_key_binary_roundtrips_relation_payloads() {
        let left = Ulid::from_u128(100);
        let right = Ulid::from_u128(200);
        let single = encode_storage_key_binary_value_bytes(
            STRONG_RELATION_KIND,
            &Value::Ulid(left),
            "relation",
        )
        .expect("single relation should encode")
        .expect("relation kind should stay on storage-key lane");
        let many = encode_storage_key_binary_value_bytes(
            STRONG_RELATION_LIST_KIND,
            &Value::List(vec![Value::Ulid(left), Value::Null, Value::Ulid(right)]),
            "relations",
        )
        .expect("many relation should encode")
        .expect("relation list kind should stay on storage-key lane");

        assert_eq!(
            decode_storage_key_binary_value_bytes(single.as_slice(), STRONG_RELATION_KIND)
                .expect("single relation should decode")
                .expect("single relation should be storage-key-owned"),
            Value::Ulid(left),
        );
        assert_eq!(
            decode_relation_target_storage_keys_binary_bytes(
                single.as_slice(),
                STRONG_RELATION_KIND
            )
            .expect("single relation target keys should decode"),
            vec![StorageKey::Ulid(left)],
        );
        assert_eq!(
            decode_storage_key_binary_value_bytes(many.as_slice(), STRONG_RELATION_LIST_KIND)
                .expect("many relation should decode")
                .expect("relation list should be storage-key-owned"),
            Value::List(vec![Value::Ulid(left), Value::Ulid(right)]),
        );
        assert_eq!(
            decode_relation_target_storage_keys_binary_bytes(
                many.as_slice(),
                STRONG_RELATION_LIST_KIND
            )
            .expect("many relation target keys should decode"),
            vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
        );
    }

    #[test]
    fn storage_key_binary_rejects_malformed_account_payload() {
        let bytes = encode_list(&[encode_bytes(Principal::dummy(1).as_slice())]);

        let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Account);
        let validate =
            validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Account);

        assert!(
            decode.is_err(),
            "malformed account payload must fail decode"
        );
        assert!(
            validate.is_err(),
            "malformed account payload must fail validate"
        );
    }

    #[test]
    fn storage_key_binary_rejects_wrong_tag_for_principal_payload() {
        let bytes = encode_text("aaaaa-aa");

        let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Principal);
        let validate =
            validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Principal);

        assert!(decode.is_err(), "principal text payload must fail decode");
        assert!(
            validate.is_err(),
            "principal text payload must fail validate"
        );
    }

    #[test]
    fn storage_key_binary_rejects_wrong_size_subaccount_payload() {
        let bytes = encode_bytes(&[9_u8; 31]);

        let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Subaccount);
        let validate =
            validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Subaccount);

        assert!(decode.is_err(), "short subaccount payload must fail decode");
        assert!(
            validate.is_err(),
            "short subaccount payload must fail validate"
        );
    }

    #[test]
    fn storage_key_binary_rejects_invalid_timestamp_and_ulid_payload() {
        let bad_timestamp = encode_bytes(&[7_u8; 7]);
        let bad_ulid = encode_bytes(&[9_u8; 15]);

        assert!(
            decode_storage_key_field_binary_bytes(bad_timestamp.as_slice(), FieldKind::Timestamp)
                .is_err(),
            "invalid timestamp payload must fail decode"
        );
        assert!(
            validate_storage_key_binary_value_bytes(bad_timestamp.as_slice(), FieldKind::Timestamp)
                .is_err(),
            "invalid timestamp payload must fail validate"
        );
        assert!(
            decode_storage_key_field_binary_bytes(bad_ulid.as_slice(), FieldKind::Ulid).is_err(),
            "invalid ulid payload must fail decode"
        );
        assert!(
            validate_storage_key_binary_value_bytes(bad_ulid.as_slice(), FieldKind::Ulid).is_err(),
            "invalid ulid payload must fail validate"
        );
    }

    #[test]
    fn storage_key_binary_rejects_non_unit_unit_payload() {
        let bytes = encode_text("unit");
        let decode = decode_storage_key_field_binary_bytes(bytes.as_slice(), FieldKind::Unit);
        let validate = validate_storage_key_binary_value_bytes(bytes.as_slice(), FieldKind::Unit);

        assert!(decode.is_err(), "text unit payload must fail decode");
        assert!(validate.is_err(), "text unit payload must fail validate");
    }

    fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
        let mut out = vec![TAG_LIST];
        out.extend_from_slice(
            &u32::try_from(items.len())
                .expect("item count fits u32")
                .to_be_bytes(),
        );
        for item in items {
            out.extend_from_slice(item);
        }
        out
    }

    #[test]
    fn binary_relation_target_storage_key_decode_handles_single_ulid_and_null() {
        let target = Ulid::from_u128(7);
        let target_bytes = encode_bytes(&target.to_bytes());
        let null_bytes = encode_null();

        let decoded =
            decode_relation_target_storage_keys_binary_bytes(&target_bytes, STRONG_RELATION_KIND)
                .expect("single relation should decode");
        let decoded_null =
            decode_relation_target_storage_keys_binary_bytes(&null_bytes, STRONG_RELATION_KIND)
                .expect("null relation should decode");

        assert_eq!(decoded, vec![StorageKey::Ulid(target)]);
        assert!(
            decoded_null.is_empty(),
            "null relation should yield no targets"
        );
    }

    #[test]
    fn binary_relation_target_storage_key_decode_handles_list_and_skips_null_items() {
        let left = Ulid::from_u128(8);
        let right = Ulid::from_u128(9);
        let bytes = encode_list(&[
            encode_bytes(&left.to_bytes()),
            encode_null(),
            encode_bytes(&right.to_bytes()),
        ]);

        let decoded =
            decode_relation_target_storage_keys_binary_bytes(&bytes, STRONG_RELATION_LIST_KIND)
                .expect("relation list should decode");

        assert_eq!(
            decoded,
            vec![StorageKey::Ulid(left), StorageKey::Ulid(right)],
        );
    }

    #[test]
    fn binary_storage_key_field_decode_handles_supported_kinds() {
        let principal = Principal::dummy(7);
        let subaccount = Subaccount::from_array([7_u8; 32]);
        let account = Account::from_parts(principal, Some(subaccount));
        let timestamp = Timestamp::from_millis(1_710_013_530_000);

        assert_eq!(
            decode_storage_key_field_binary_bytes(&encode_int64(-5), FieldKind::Int)
                .expect("i64 should decode"),
            StorageKey::Int(-5),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(&encode_uint64(7), FieldKind::Uint)
                .expect("u64 should decode"),
            StorageKey::Uint(7),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(
                &encode_bytes(&Ulid::from_u128(11).to_bytes()),
                FieldKind::Ulid
            )
            .expect("ulid should decode"),
            StorageKey::Ulid(Ulid::from_u128(11)),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(
                &encode_bytes(principal.as_slice()),
                FieldKind::Principal
            )
            .expect("principal should decode"),
            StorageKey::Principal(principal),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(
                &encode_bytes(subaccount.as_slice()),
                FieldKind::Subaccount
            )
            .expect("subaccount should decode"),
            StorageKey::Subaccount(subaccount),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(
                &encode_int64(timestamp.as_millis()),
                FieldKind::Timestamp
            )
            .expect("timestamp should decode"),
            StorageKey::Timestamp(timestamp),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(
                &encode_list(&[
                    encode_bytes(principal.as_slice()),
                    encode_bytes(subaccount.as_slice())
                ]),
                FieldKind::Account,
            )
            .expect("account should decode"),
            StorageKey::Account(account),
        );
        assert_eq!(
            decode_storage_key_field_binary_bytes(&encode_unit(), FieldKind::Unit)
                .expect("unit should decode"),
            StorageKey::Unit,
        );
    }

    #[test]
    fn binary_relation_target_storage_key_roundtrips_through_owner_local_encoder() {
        let left = StorageKey::Ulid(Ulid::from_u128(41));
        let right = StorageKey::Ulid(Ulid::from_u128(42));
        let encoded = encode_relation_target_storage_keys_binary_bytes(
            &[left, right],
            STRONG_RELATION_LIST_KIND,
            "targets",
        )
        .expect("relation target bytes should encode");
        let decoded =
            decode_relation_target_storage_keys_binary_bytes(&encoded, STRONG_RELATION_LIST_KIND)
                .expect("relation target bytes should decode");

        assert_eq!(decoded, vec![left, right]);
    }

    #[test]
    fn binary_storage_key_field_roundtrips_through_owner_local_encoder() {
        let timestamp = Timestamp::from_millis(1_700_000_000_123);
        let account = Account::from_parts(
            Principal::from_slice(&[0xAB, 0xCD]),
            Some(Subaccount::from_array([7; 32])),
        );

        let cases = [
            (StorageKey::Int(-5), FieldKind::Int),
            (StorageKey::Uint(7), FieldKind::Uint),
            (StorageKey::Ulid(Ulid::from_u128(17)), FieldKind::Ulid),
            (
                StorageKey::Principal(Principal::from_slice(&[0xAA, 0xBB])),
                FieldKind::Principal,
            ),
            (
                StorageKey::Subaccount(Subaccount::from_array([9; 32])),
                FieldKind::Subaccount,
            ),
            (StorageKey::Timestamp(timestamp), FieldKind::Timestamp),
            (StorageKey::Account(account), FieldKind::Account),
            (StorageKey::Unit, FieldKind::Unit),
        ];

        for (key, kind) in cases {
            let encoded = encode_storage_key_field_binary_bytes(key, kind, "field")
                .expect("storage-key field bytes should encode");
            let decoded = decode_storage_key_field_binary_bytes(&encoded, kind)
                .expect("storage-key field bytes should decode");

            assert_eq!(decoded, key);
        }
    }
}
