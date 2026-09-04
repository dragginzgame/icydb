//! Module: data::structural_field::value_storage::decode
//! Responsibility: decode-side wiring for structural value-storage materialization and borrowed views.
//! Does not own: value-storage encoding, field-kind routing, or row reconstruction.
//! Boundary: chooses between skip-validated borrowed traversal and runtime `Value` materialization.

mod cursor;
mod scalar;
mod value;
mod view;

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        CompleteBinaryValue, TAG_BYTES, TAG_INT64, TAG_NAT64, TAG_TEXT,
        payload_bytes as binary_payload_bytes,
    },
    value_storage::skip::skip_value_storage_binary_value,
};

pub(in crate::db) use value::{
    decode_structural_value_storage_bytes, validate_structural_value_storage_bytes,
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
    scalar_len: u32,
}

impl<'a> ValueStorageSlice<'a> {
    /// Validate raw bytes as exactly one structural value-storage envelope.
    #[inline]
    fn from_raw(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let tag = *raw.first().ok_or_else(FieldDecodeError::new)?;
        if value_storage_tag_has_payload(tag) {
            Ok(Self::from_scalar_root(CompleteBinaryValue::parse(raw)?))
        } else {
            if skip_value_storage_binary_value(raw, 0)? != raw.len() {
                return Err(FieldDecodeError::new());
            }

            Ok(Self::other(raw))
        }
    }

    /// Build a slice from bytes already bounded by value-storage skip.
    ///
    /// Callers must only use this when `bytes` came from a cursor range whose
    /// end was returned by `skip_value_storage_binary_value`.
    #[inline]
    fn from_skip_bounded(bytes: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let tag = *bytes.first().ok_or_else(FieldDecodeError::new)?;
        if value_storage_tag_has_payload(tag) {
            Ok(Self::from_scalar_root(
                CompleteBinaryValue::from_skip_bounded(bytes)?,
            ))
        } else {
            Ok(Self::other(bytes))
        }
    }

    #[inline]
    const fn from_scalar_root(root: CompleteBinaryValue<'a>) -> Self {
        Self {
            bytes: root.bytes(),
            scalar_len: root.len(),
        }
    }

    #[inline]
    const fn other(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            scalar_len: 0,
        }
    }

    /// Return the bounded bytes after skip traversal has established ownership.
    #[inline]
    const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Borrow one retained generic scalar payload with its expected shape.
    #[inline]
    fn scalar_payload(
        &self,
        expected_tag: u8,
        expected_len: Option<u32>,
    ) -> Result<&'a [u8], FieldDecodeError> {
        if self.bytes[0] != expected_tag || expected_len.is_some_and(|len| self.scalar_len != len) {
            return Err(FieldDecodeError::new());
        }

        let payload_offset = match expected_tag {
            TAG_INT64 | TAG_NAT64 => 1,
            TAG_TEXT | TAG_BYTES => 5,
            _ => return Err(FieldDecodeError::new()),
        };

        binary_payload_bytes(self.bytes, self.scalar_len, payload_offset)
    }
}

// Return whether a generic scalar's parsed payload facts are consumed after
// validation. Zero-payload scalars, containers, and owner-local extension tags
// retain their existing value-storage walkers.
const fn value_storage_tag_has_payload(tag: u8) -> bool {
    matches!(tag, TAG_INT64 | TAG_NAT64 | TAG_TEXT | TAG_BYTES)
}
