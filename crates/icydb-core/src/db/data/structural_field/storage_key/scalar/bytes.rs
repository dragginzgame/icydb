use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, parse_binary_head as parse_structural_binary_head,
            payload_bytes as binary_payload_bytes,
            skip_binary_value as skip_structural_binary_value,
        },
        typed::{
            decode_principal_payload_bytes, decode_subaccount_payload_bytes,
            decode_ulid_payload_bytes,
        },
    },
    value::StorageKey,
};

// Decode one principal relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_principal_storage_key_binary_bytes(
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

    decode_principal_payload_bytes(binary_payload_bytes(
        raw_bytes,
        len,
        payload_start,
        "principal",
    )?)
    .map(StorageKey::Principal)
}

// Decode one subaccount relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_subaccount_storage_key_binary_bytes(
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
    decode_subaccount_payload_bytes(binary_payload_bytes(
        raw_bytes,
        len,
        payload_start,
        "subaccount",
    )?)
    .map(StorageKey::Subaccount)
}

// Decode one ULID relation-key payload directly from its fixed-width Structural
// Binary bytes form.
pub(in crate::db::data::structural_field::storage_key) fn decode_ulid_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
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

    decode_ulid_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start, "ulid")?)
        .map(StorageKey::Ulid)
}
