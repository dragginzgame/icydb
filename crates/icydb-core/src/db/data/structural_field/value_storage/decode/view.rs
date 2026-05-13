//! Borrowed, non-materializing access to validated value-storage bytes.
//!
//! This module is the execution-facing half of value-storage traversal. It
//! lets callers walk nested collection payloads as bounded byte slices after
//! skip validation has established that the root contains exactly one value.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_FALSE, TAG_INT64, TAG_NAT64, TAG_NULL, TAG_TEXT, TAG_TRUE},
    value_storage::{
        decode::{
            ValueStorageSlice,
            scalar::{
                decode_binary_i64_scalar, decode_binary_text_payload_bytes_if_text,
                decode_binary_text_scalar, decode_binary_u64_scalar,
            },
        },
        walk::visit_value_storage_map_entries,
    },
};

///
/// ValueStorageView
///
/// Borrowed view over structural value-storage bytes. Raw execution callers
/// build it through skip validation, and nested map lookups reuse the walker
/// boundary without materializing runtime `Value` entries.
///

pub(in crate::db) struct ValueStorageView<'a> {
    bytes: &'a [u8],
}

impl<'a> ValueStorageView<'a> {
    /// Validate raw bytes as one value-storage envelope and expose a view.
    pub(in crate::db) fn from_raw_validated(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let slice = ValueStorageSlice::from_raw(raw)?;

        Ok(Self {
            bytes: slice.as_bytes(),
        })
    }

    /// Wrap bytes whose exact boundary was already returned by skip traversal.
    const fn from_skip_bounded_unchecked(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Return the bytes covered by this view.
    #[inline]
    pub(in crate::db) const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Return the value-storage tag without decoding the payload.
    #[inline]
    const fn tag(&self) -> u8 {
        self.bytes[0]
    }

    /// Return whether this view contains a generic null value.
    #[inline]
    pub(in crate::db) const fn is_null(&self) -> bool {
        self.tag() == TAG_NULL
    }

    /// Return whether this view contains a generic bool value.
    #[inline]
    pub(in crate::db) const fn is_bool(&self) -> bool {
        matches!(self.tag(), TAG_FALSE | TAG_TRUE)
    }

    /// Return whether this view contains a generic i64 value.
    #[inline]
    pub(in crate::db) const fn is_i64(&self) -> bool {
        self.tag() == TAG_INT64
    }

    /// Return whether this view contains a generic u64 value.
    #[inline]
    pub(in crate::db) const fn is_u64(&self) -> bool {
        self.tag() == TAG_NAT64
    }

    /// Return whether this view contains a generic text value.
    #[inline]
    pub(in crate::db) const fn is_text(&self) -> bool {
        self.tag() == TAG_TEXT
    }

    /// Decode one bool directly from the bounded value-storage slice.
    pub(in crate::db) fn as_bool(&self) -> Result<bool, FieldDecodeError> {
        match self.tag() {
            TAG_FALSE => Ok(false),
            TAG_TRUE => Ok(true),
            _ => Err(FieldDecodeError::new(
                "structural binary: expected bool payload",
            )),
        }
    }

    /// Decode one i64 directly from the bounded value-storage slice.
    pub(in crate::db) fn as_i64(&self) -> Result<i64, FieldDecodeError> {
        decode_binary_i64_scalar(self.as_bytes())
    }

    /// Decode one u64 directly from the bounded value-storage slice.
    pub(in crate::db) fn as_u64(&self) -> Result<u64, FieldDecodeError> {
        decode_binary_u64_scalar(self.as_bytes())
    }

    /// Decode one borrowed string directly from the bounded value-storage slice.
    pub(in crate::db) fn as_text(&self) -> Result<&'a str, FieldDecodeError> {
        decode_binary_text_scalar(self.as_bytes())
    }

    /// Return the value slice for one text-keyed map entry using byte equality.
    pub(in crate::db) fn map_text_key_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Self>, FieldDecodeError> {
        let mut found = None;

        // Segment bytes are compiled once from Rust `String`s. Comparing them
        // against borrowed text payload bytes avoids per-row UTF-8 decoding
        // while preserving map-entry boundary validation in the walker.
        visit_value_storage_map_entries(
            self.as_bytes(),
            "structural binary: expected value map payload",
            "structural binary: trailing bytes after value map payload",
            |_| (),
            |(), entry_key, entry_value| {
                if found.is_some() {
                    return Ok(());
                }

                if decode_binary_text_payload_bytes_if_text(entry_key)?
                    .is_some_and(|found| found == key)
                {
                    found = Some(Self::from_skip_bounded_unchecked(entry_value));
                }

                Ok(())
            },
        )?;

        Ok(found)
    }
}
