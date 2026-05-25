//! Module: data::structural_field::primary_key_component::scalar::primitive
//! Responsibility: primitive primary-key-component scalar decode for unit, integers, and timestamp.
//! Does not own: generic scalar dispatch, relation traversal, or row decode.
//! Boundary: decodes primitive primary-key-component payloads after callers select this scalar lane.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, TAG_INT64, TAG_NAT64, TAG_UNIT,
            parse_binary_head as parse_structural_binary_head,
            payload_bytes as binary_payload_bytes,
            skip_binary_value as skip_structural_binary_value,
        },
        primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
        typed::{
            decode_int128_payload_bytes, decode_nat128_payload_bytes,
            decode_timestamp_payload_millis,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
};

// Decode one timestamp relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_timestamp_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
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
    Ok(PrimaryKeyComponent::Timestamp(
        decode_timestamp_payload_millis(decode_i64_payload_bytes(
            binary_payload_bytes(raw_bytes, len, payload_start, "timestamp")?,
            "timestamp",
        )?),
    ))
}

// Decode one unit relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_unit_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
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

    Ok(PrimaryKeyComponent::Unit)
}

// Decode one signed primary-key-component integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_int_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
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
    Ok(PrimaryKeyComponent::Int64(decode_i64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, "integer")?,
        "i64",
    )?))
}

// Decode one signed 128-bit primary-key-component integer payload from
// Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_int128_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated int128 payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_BYTES || len != 16 {
        return Err(FieldDecodeError::new(
            "structural binary: expected 16-byte int128 payload",
        ));
    }
    Ok(PrimaryKeyComponent::Int128(decode_int128_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, "int128")?,
    )?))
}

// Decode one unsigned primary-key-component integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_nat_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
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
    if tag != TAG_NAT64 || len != 8 {
        return Err(FieldDecodeError::new(
            "structural binary: expected u64 integer payload",
        ));
    }
    Ok(PrimaryKeyComponent::Nat64(decode_u64_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, "integer")?,
        "u64",
    )?))
}

// Decode one unsigned 128-bit primary-key-component integer payload from
// Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_nat128_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    let Some((tag, len, payload_start)) = parse_structural_binary_head(raw_bytes, 0)? else {
        return Err(FieldDecodeError::new(
            "structural binary: truncated nat128 payload",
        ));
    };
    let end = skip_structural_binary_value(raw_bytes, 0)?;
    if end != raw_bytes.len() {
        return Err(FieldDecodeError::new(
            "structural binary: trailing bytes after relation field",
        ));
    }
    if tag != TAG_BYTES || len != 16 {
        return Err(FieldDecodeError::new(
            "structural binary: expected 16-byte nat128 payload",
        ));
    }
    Ok(PrimaryKeyComponent::Nat128(decode_nat128_payload_bytes(
        binary_payload_bytes(raw_bytes, len, payload_start, "nat128")?,
    )?))
}
