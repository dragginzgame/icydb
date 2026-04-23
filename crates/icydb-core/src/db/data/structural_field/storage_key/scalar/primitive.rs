use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_INT64, TAG_UINT64, TAG_UNIT, parse_binary_head as parse_structural_binary_head,
            payload_bytes as binary_payload_bytes,
            skip_binary_value as skip_structural_binary_value,
        },
        typed::decode_timestamp_payload_millis,
    },
    value::StorageKey,
};

// Decode one timestamp relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_timestamp_storage_key_binary_bytes(
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

    Ok(StorageKey::Timestamp(decode_timestamp_payload_millis(
        i64::from_be_bytes(payload),
    )))
}

// Decode one unit relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::storage_key) fn decode_unit_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
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
pub(in crate::db::data::structural_field::storage_key) fn decode_int_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
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
pub(in crate::db::data::structural_field::storage_key) fn decode_uint_storage_key_binary_bytes(
    raw_bytes: &[u8],
) -> Result<StorageKey, FieldDecodeError> {
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
