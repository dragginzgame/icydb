//! Module: data::structural_field::primary_key_component::scalar::primitive
//! Responsibility: primitive primary-key-component scalar decode for unit, integers, and timestamp.
//! Does not own: generic scalar dispatch, relation traversal, or row decode.
//! Boundary: decodes primitive primary-key-component payloads after callers select this scalar lane.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{CompleteBinaryValue, TAG_BYTES, TAG_INT64, TAG_NAT64, TAG_UNIT},
        primitive::{decode_i64_payload_bytes, decode_u64_payload_bytes},
        typed::{
            decode_int128_payload_bytes, decode_nat128_payload_bytes,
            decode_timestamp_payload_millis,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
    types::U256,
};

// Decode one timestamp relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_timestamp_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_INT64 || root.len() != 8 {
        return Err(FieldDecodeError::new());
    }
    Ok(PrimaryKeyComponent::Timestamp(
        decode_timestamp_payload_millis(decode_i64_payload_bytes(root.scalar_payload()?)?),
    ))
}

// Decode one unit relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) const fn decode_unit_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_UNIT {
        return Err(FieldDecodeError::new());
    }

    Ok(PrimaryKeyComponent::Unit)
}

// Decode one signed primary-key-component integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_int_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_INT64 || root.len() != 8 {
        return Err(FieldDecodeError::new());
    }
    Ok(PrimaryKeyComponent::Int64(decode_i64_payload_bytes(
        root.scalar_payload()?,
    )?))
}

// Decode one signed 128-bit primary-key-component integer payload from
// Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_int128_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES || root.len() != 16 {
        return Err(FieldDecodeError::new());
    }
    Ok(PrimaryKeyComponent::Int128(decode_int128_payload_bytes(
        root.scalar_payload()?,
    )?))
}

// Decode one unsigned primary-key-component integer payload from Structural
// Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_nat_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_NAT64 || root.len() != 8 {
        return Err(FieldDecodeError::new());
    }
    Ok(PrimaryKeyComponent::Nat64(decode_u64_payload_bytes(
        root.scalar_payload()?,
    )?))
}

// Decode one unsigned 128-bit primary-key-component integer payload from
// Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_nat128_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES || root.len() != 16 {
        return Err(FieldDecodeError::new());
    }
    Ok(PrimaryKeyComponent::Nat128(decode_nat128_payload_bytes(
        root.scalar_payload()?,
    )?))
}

// Decode one unsigned 256-bit primary-key component from fixed bytes.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_u256_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES || root.len() != 32 {
        return Err(FieldDecodeError::new());
    }
    let bytes = root
        .scalar_payload()?
        .try_into()
        .map_err(|_| FieldDecodeError::new())?;
    Ok(PrimaryKeyComponent::U256(U256::from_be_bytes(bytes)))
}
