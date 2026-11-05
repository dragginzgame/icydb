//! Test-only helpers for the parent module.

use super::*;

/// Decode one primary-key-component Structural Binary v1 field payload
/// directly into its canonical `PrimaryKeyComponent` form.
pub(in crate::db) fn decode_primary_key_component_field_binary_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<PrimaryKeyComponent, FieldDecodeError> {
    decode_primary_key_component_field(&CompleteBinaryValue::parse(raw_bytes)?, kind)
}

/// Validate one Structural Binary v1 primary-key-component field payload
/// without routing through the generic structural value lane.
pub(in crate::db) fn validate_primary_key_component_binary_value_bytes(
    raw_bytes: &[u8],
    kind: &AcceptedFieldKind,
) -> Result<bool, FieldDecodeError> {
    if !supports_primary_key_component_binary_kind(kind) {
        return Ok(false);
    }

    decode_primary_key_component_binary_value_bytes(raw_bytes, kind)?;

    Ok(true)
}
