//! Module: data::structural_field
//! Responsibility: canonical persisted-field structural decode helpers.
//! Does not own: row layout planning, typed entity reconstruction, or query semantics.
//! Boundary: runtime paths use this module when they need one persisted field decoded without `E`.

mod accepted;
mod binary;
mod leaf;
mod primary_key_component;
mod primitive;
mod scalar;
mod typed;
mod value_storage;

pub(in crate::db) use accepted::{
    accepted_kind_supports_primary_key_component_binary,
    decode_structural_field_by_accepted_kind_bytes, encode_structural_field_by_accepted_kind_bytes,
    validate_structural_field_by_accepted_kind_bytes,
};
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
