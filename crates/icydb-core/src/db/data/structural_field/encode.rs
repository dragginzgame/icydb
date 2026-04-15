//! Module: data::structural_field::encode
//! Responsibility: owner-local `ByKind` persisted field encoding.
//! Does not own: row layout orchestration, generic serde wire surfaces, or
//! externally tagged `Value` storage envelopes.
//! Boundary: persisted-row writers call into this file when they need the raw
//! bytes for one semantic field kind without routing through generic serde.

use crate::db::data::structural_field::{
    composite::encode_composite_field_binary_bytes, leaf::encode_leaf_field_binary_bytes,
    scalar::encode_scalar_fast_path_binary_bytes,
};
use crate::{error::InternalError, model::field::FieldKind, value::Value};

/// Encode one `ByKind` field payload into the canonical structural field
/// format expected by the current field decoder.
pub(in crate::db) fn encode_structural_field_by_kind_bytes(
    kind: FieldKind,
    value: &Value,
    field_name: &str,
) -> Result<Vec<u8>, InternalError> {
    // Keep the root entrypoint as a thin owner router: scalar first, then
    // fixed-shape leaves, then the recursive composite authority.
    if let Some(encoded) = encode_scalar_fast_path_binary_bytes(kind, value, field_name)? {
        return Ok(encoded);
    }
    if let Some(encoded) = encode_leaf_field_binary_bytes(kind, value, field_name)? {
        return Ok(encoded);
    }

    if matches!(
        kind,
        FieldKind::Enum { .. }
            | FieldKind::List(_)
            | FieldKind::Map { .. }
            | FieldKind::Relation { .. }
            | FieldKind::Set(_)
    ) {
        return encode_composite_field_binary_bytes(kind, value, field_name);
    }

    Err(InternalError::persisted_row_field_encode_failed(
        field_name,
        format!("unsupported structural field kind during encode: {kind:?}"),
    ))
}
