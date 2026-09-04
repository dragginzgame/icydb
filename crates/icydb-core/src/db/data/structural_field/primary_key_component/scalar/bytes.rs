//! Module: data::structural_field::primary_key_component::scalar::bytes
//! Responsibility: byte-backed primary-key-component scalar decode for principal, subaccount, and ULID.
//! Does not own: generic scalar dispatch, relation traversal, or row decode.
//! Boundary: decodes byte-backed primary-key-component payloads after callers select this scalar lane.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{
            TAG_BYTES, parse_complete_binary_value as parse_complete_structural_binary_value,
            payload_bytes as binary_payload_bytes,
        },
        typed::{
            decode_principal_payload_bytes, decode_subaccount_payload_bytes,
            decode_ulid_payload_bytes,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
};

// Decode one principal relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_principal_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_structural_binary_value(raw_bytes)?;
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    decode_principal_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
        .map(PrimaryKeyComponent::Principal)
}

// Decode one subaccount relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_subaccount_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_structural_binary_value(raw_bytes)?;
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }
    decode_subaccount_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
        .map(PrimaryKeyComponent::Subaccount)
}

// Decode one ULID relation-key payload directly from its fixed-width Structural
// Binary bytes form.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_ulid_primary_key_component_binary_bytes(
    raw_bytes: &[u8],
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    let (tag, len, payload_start) = parse_complete_structural_binary_value(raw_bytes)?;
    if tag != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    decode_ulid_payload_bytes(binary_payload_bytes(raw_bytes, len, payload_start)?)
        .map(PrimaryKeyComponent::Ulid)
}
