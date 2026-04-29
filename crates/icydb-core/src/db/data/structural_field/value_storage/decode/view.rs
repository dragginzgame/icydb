//! Borrowed, non-materializing access to validated value-storage bytes.
//!
//! This module is the execution-facing half of value-storage traversal. It
//! lets callers walk nested collection payloads as bounded byte slices after
//! skip validation has established that the root contains exactly one value.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_FALSE, TAG_INT64, TAG_NULL, TAG_TEXT, TAG_TRUE, TAG_UINT64},
    value_storage::{
        decode::{
            ValueStorageSlice,
            scalar::{
                decode_binary_i64_scalar, decode_binary_text_payload_bytes_if_text,
                decode_binary_text_scalar, decode_binary_u64_scalar,
            },
        },
        walk::{visit_value_storage_list_items, visit_value_storage_map_entries},
    },
};

///
/// ValueStorageView
///
/// Borrowed view over structural value-storage bytes. Raw execution callers
/// build it through skip validation, while internal collection splitters use it
/// as a typed wrapper around walkers that validate before yielding slices.
///

pub(in crate::db) struct ValueStorageView<'a> {
    bytes: &'a [u8],
}

impl<'a> ValueStorageView<'a> {
    /// Validate raw bytes as one value-storage envelope and expose a view.
    pub(in crate::db) fn from_raw(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let slice = ValueStorageSlice::from_raw(raw)?;

        Ok(Self {
            bytes: slice.as_bytes(),
        })
    }

    /// Wrap bytes that a collection visitor will validate before yielding.
    pub(in crate::db::data::structural_field::value_storage::decode) const fn from_collection_walker_input(
        bytes: &'a [u8],
    ) -> Self {
        Self { bytes }
    }

    /// Wrap bytes whose exact boundary was already returned by skip traversal.
    #[allow(
        dead_code,
        reason = "nested borrowed view access is staged before query AST integration"
    )]
    pub(in crate::db) const fn from_bounded_unchecked(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Return the bytes covered by this view.
    #[inline]
    pub(in crate::db) const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Return the value-storage tag without decoding the payload.
    #[inline]
    pub(in crate::db) const fn tag(&self) -> u8 {
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
        self.tag() == TAG_UINT64
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

    /// Visit list item slices without materializing the list as `Value::List`.
    pub(in crate::db) fn visit_list_items(
        &self,
        mut visit: impl FnMut(&'a [u8]) -> Result<(), FieldDecodeError>,
    ) -> Result<(), FieldDecodeError> {
        // The underlying walker validates each nested item boundary with skip
        // and yields the exact borrowed byte range for the item.
        visit_value_storage_list_items(
            self.as_bytes(),
            "structural binary: expected value list payload",
            "structural binary: trailing bytes after value list payload",
            |_| (),
            |(), item| visit(item),
        )
    }

    /// Visit map key/value slices without materializing `Value::Map` entries.
    pub(in crate::db) fn visit_map_entries(
        &self,
        mut visit: impl FnMut(&'a [u8], &'a [u8]) -> Result<(), FieldDecodeError>,
    ) -> Result<(), FieldDecodeError> {
        // Map traversal keeps key and value boundaries independent so callers
        // can evaluate either side without staging decoded entry pairs.
        visit_value_storage_map_entries(
            self.as_bytes(),
            "structural binary: expected value map payload",
            "structural binary: trailing bytes after value map payload",
            |_| (),
            |(), key, value| visit(key, value),
        )
    }

    /// Return the value slice for one text-keyed map entry without materializing the map.
    #[allow(
        dead_code,
        reason = "nested borrowed view access is staged before query AST integration"
    )]
    pub(in crate::db) fn map_text_key(&self, key: &str) -> Result<Option<Self>, FieldDecodeError> {
        let mut found = None;

        // Each key/value slice yielded here is already bounded by the map
        // walker. Building child views from those slices must therefore avoid
        // re-running root validation.
        self.visit_map_entries(|entry_key, entry_value| {
            if found.is_some() {
                return Ok(());
            }

            let key_view = Self::from_bounded_unchecked(entry_key);
            if key_view.is_text() && key_view.as_text()? == key {
                found = Some(Self::from_bounded_unchecked(entry_value));
            }

            Ok(())
        })?;

        Ok(found)
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
        self.visit_map_entries(|entry_key, entry_value| {
            if found.is_some() {
                return Ok(());
            }

            if decode_binary_text_payload_bytes_if_text(entry_key)?
                .is_some_and(|found| found == key)
            {
                found = Some(Self::from_bounded_unchecked(entry_value));
            }

            Ok(())
        })?;

        Ok(found)
    }
}
