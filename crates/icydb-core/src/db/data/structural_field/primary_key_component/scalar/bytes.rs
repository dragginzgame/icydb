//! Module: data::structural_field::primary_key_component::scalar::bytes
//! Responsibility: byte-backed primary-key-component scalar decode for principal, subaccount, and ULID.
//! Does not own: generic scalar dispatch, relation traversal, or row decode.
//! Boundary: decodes byte-backed primary-key-component payloads after callers select this scalar lane.

use crate::{
    db::data::structural_field::{
        FieldDecodeError,
        binary::{CompleteBinaryValue, TAG_BYTES},
        typed::{
            decode_principal_payload_bytes, decode_subaccount_payload_bytes,
            decode_ulid_payload_bytes,
        },
    },
    db::key_taxonomy::PrimaryKeyComponent,
};

// Decode one principal relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_principal_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    decode_principal_payload_bytes(root.scalar_payload()?).map(PrimaryKeyComponent::Principal)
}

// Decode one subaccount relation-key payload from Structural Binary v1.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_subaccount_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }
    decode_subaccount_payload_bytes(root.scalar_payload()?).map(PrimaryKeyComponent::Subaccount)
}

// Decode one ULID relation-key payload directly from its fixed-width Structural
// Binary bytes form.
pub(in crate::db::data::structural_field::primary_key_component) fn decode_ulid_primary_key_component(
    root: &CompleteBinaryValue<'_>,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    if root.tag() != TAG_BYTES {
        return Err(FieldDecodeError::new());
    }

    decode_ulid_payload_bytes(root.scalar_payload()?).map(PrimaryKeyComponent::Ulid)
}
