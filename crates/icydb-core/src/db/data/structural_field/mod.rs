//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod accepted;
mod binary;
mod composite;
mod encode;
mod leaf;
mod primary_key_component;
mod primitive;
mod scalar;
mod typed;
mod value_storage;

use crate::{model::field::FieldKind, value::Value};

use composite::{decode_composite_field_by_kind_bytes, validate_composite_field_by_kind_bytes};
use leaf::decode_leaf_field_by_kind_bytes;
use scalar::decode_scalar_fast_path_bytes;

pub(in crate::db) use accepted::{
    accepted_kind_supports_primary_key_component_binary,
    decode_structural_field_by_accepted_kind_bytes, encode_structural_field_by_accepted_kind_bytes,
    validate_structural_field_by_accepted_kind_bytes,
};
pub(in crate::db) use encode::encode_structural_field_by_kind_bytes;
pub(in crate::db) use primary_key_component::decode_accepted_relation_target_primary_key_components_bytes;
pub(in crate::db) use value_storage::{
    ValueStorageView, decode_canonical_value_storage_bytes, decode_structural_value_storage_bytes,
    encode_canonical_value_storage_bytes, encode_structural_value_storage_null_bytes,
    validate_structural_value_storage_bytes, value_storage_bytes_are_null,
};

///
/// FieldDecodeError
///
/// FieldDecodeError captures one persisted-field structural decode
/// failure.
/// It keeps structural decode diagnostics local to the field boundary so row
/// and relation callers can map them into taxonomy-correct higher-level errors.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct FieldDecodeError;

impl FieldDecodeError {
    // Build one compact structural field-decode failure marker. Detailed
    // corruption taxonomy is added by row/store boundaries.
    pub(in crate::db) const fn new() -> Self {
        Self
    }
}

/// Decode one encoded persisted field payload strictly by semantic field kind.
pub(in crate::db) fn decode_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<Value, FieldDecodeError> {
    // Keep byte-backed `ByKind` leaves off the generic `ValueWire` bridge
    // whenever their persisted shape is fixed or already owned by the leaf
    // type.
    if let Some(value) = decode_scalar_fast_path_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    // Keep the root entrypoint as a thin lane router: scalar fast path above,
    // then non-recursive leaves, then the recursive composite authority.
    if let Some(value) = decode_leaf_field_by_kind_bytes(raw_bytes, kind)? {
        return Ok(value);
    }

    decode_composite_field_by_kind_bytes(raw_bytes, kind)
}

/// Validate one encoded persisted field payload strictly by semantic field
/// kind without eagerly building the final runtime `Value`.
pub(in crate::db) fn validate_structural_field_by_kind_bytes(
    raw_bytes: &[u8],
    kind: FieldKind,
) -> Result<(), FieldDecodeError> {
    // Keep the validate-only entrypoint aligned with the existing decode lane
    // ordering so row-open validation and later materialization still share one
    // field-contract authority.
    if decode_scalar_fast_path_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    if decode_leaf_field_by_kind_bytes(raw_bytes, kind)?.is_some() {
        return Ok(());
    }

    validate_composite_field_by_kind_bytes(raw_bytes, kind)
}
