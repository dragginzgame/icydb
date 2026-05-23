//! Module: data::structural_field::value_storage::decode
//! Responsibility: decode-side wiring for structural value-storage materialization and borrowed views.
//! Does not own: value-storage encoding, field-kind routing, or row reconstruction.
//! Boundary: chooses between skip-validated borrowed traversal and runtime `Value` materialization.

mod cursor;
mod scalar;
mod value;
mod view;

use crate::db::data::structural_field::{
    FieldDecodeError, value_storage::skip::skip_value_storage_binary_value,
};

pub(in crate::db) use value::{
    decode_account, decode_decimal, decode_enum, decode_int, decode_int128, decode_nat,
    decode_nat128, decode_structural_value_storage_blob_bytes,
    decode_structural_value_storage_bool_bytes, decode_structural_value_storage_bytes,
    decode_structural_value_storage_date_bytes, decode_structural_value_storage_duration_bytes,
    decode_structural_value_storage_float32_bytes, decode_structural_value_storage_float64_bytes,
    decode_structural_value_storage_i64_bytes, decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_value_storage_list_item_slices, decode_value_storage_map_entry_slices,
    decode_value_storage_text, validate_structural_value_storage_bytes,
    value_storage_bytes_are_null,
};
pub(in crate::db) use view::ValueStorageView;

///
/// ValueStorageSlice
///
/// Bounded structural value-storage bytes that have already been proven to
/// contain exactly one valid value envelope. Decode entrypoints accept this
/// wrapper so only skip traversal can authorize top-level materialization.
///

struct ValueStorageSlice<'a> {
    bytes: &'a [u8],
}

impl<'a> ValueStorageSlice<'a> {
    /// Validate raw bytes as exactly one structural value-storage envelope.
    fn from_raw(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let end = skip_value_storage_binary_value(raw, 0)?;
        if end != raw.len() {
            return Err(FieldDecodeError::new(
                "structural binary: trailing bytes after value payload",
            ));
        }

        Ok(Self { bytes: raw })
    }

    /// Build a bounded slice from bytes already produced by value-storage skip.
    ///
    /// Callers must only use this when `bytes` came from a cursor range whose
    /// end was returned by `skip_value_storage_binary_value`.
    const fn from_skip_bounded_unchecked(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Return the bounded bytes after skip traversal has established ownership.
    #[inline]
    const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}
