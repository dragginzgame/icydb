//! Decode-side materialization for the structural value-storage owner.
//!
//! Two traversal models intentionally coexist here. Skip-based traversal is the
//! authoritative boundary detector for borrowed-slice helpers and local tagged
//! payload extraction: it validates the structural shape, finds the exact byte
//! boundary, and only then lets callers inspect the bounded slice. Decode-based
//! traversal is used when this module materializes runtime `Value` trees; those
//! paths advance a cursor while decoding and may assume any slice handed to a
//! nested decoder is already bounded by the owning traversal step.
//!
//! The distinction is important for maintenance: skip owns structural
//! validation and boundary discovery, while decode owns `Value` construction.
//! New callers should pick the model that matches their ownership needs rather
//! than mixing borrowed boundary detection with runtime materialization.

mod cursor;
mod scalar;
mod value;
mod view;

use crate::db::data::structural_field::{
    FieldDecodeError, value_storage::skip::skip_value_storage_binary_value,
};

pub(in crate::db) use value::{
    decode_account, decode_decimal, decode_enum, decode_int, decode_int128, decode_list_item,
    decode_map_entry, decode_nat, decode_nat128, decode_structural_value_storage_blob_bytes,
    decode_structural_value_storage_bool_bytes, decode_structural_value_storage_bytes,
    decode_structural_value_storage_date_bytes, decode_structural_value_storage_duration_bytes,
    decode_structural_value_storage_float32_bytes, decode_structural_value_storage_float64_bytes,
    decode_structural_value_storage_i64_bytes, decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_text, structural_value_storage_bytes_are_null, validate_structural_value_storage_bytes,
};
pub(in crate::db) use view::ValueStorageView;

#[cfg(test)]
pub(super) use value::{
    decode_structural_value_storage_binary_bytes, validate_structural_value_storage_binary_bytes,
};

///
/// ValueStorageSlice
///
/// Bounded structural value-storage bytes that have already been proven to
/// contain exactly one valid value envelope. Decode entrypoints accept this
/// wrapper so only skip traversal can authorize top-level materialization.
///

pub(crate) struct ValueStorageSlice<'a> {
    bytes: &'a [u8],
}

impl<'a> ValueStorageSlice<'a> {
    /// Validate raw bytes as exactly one structural value-storage envelope.
    pub(crate) fn from_raw(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
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
    pub(in crate::db::data::structural_field::value_storage::decode) const fn from_bounded_unchecked(
        bytes: &'a [u8],
    ) -> Self {
        Self { bytes }
    }

    /// Return the bounded bytes after skip traversal has established ownership.
    #[inline]
    pub(crate) const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}
