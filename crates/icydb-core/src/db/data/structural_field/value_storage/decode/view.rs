//! Module: data::structural_field::value_storage::decode::view
//! Responsibility: borrowed non-materializing access to validated value-storage bytes.
//! Does not own: runtime `Value` construction, encode policy, or field-kind routing.
//! Boundary: exposes typed borrowed access only after skip validation proves one bounded root value.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_NAT64, TAG_NULL, TAG_TEXT, TAG_TRUE},
    value_storage::decode::{
        ValueStorageSlice,
        scalar::{
            decode_binary_blob_scalar, decode_binary_i64_scalar, decode_binary_text_scalar,
            decode_binary_u64_scalar,
        },
    },
};
use crate::db::data::structural_field::{
    binary::{TAG_MAP, parse_binary_head},
    value_storage::{
        canonical::skip_canonical_value, decode::scalar::decode_binary_text_payload_bytes_if_text,
        skip::skip_value_storage_binary_value,
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
    slice: ValueStorageSlice<'a>,
}

impl<'a> ValueStorageView<'a> {
    /// Validate raw bytes as one value-storage envelope and expose a view.
    pub(in crate::db) fn from_raw_validated(raw: &'a [u8]) -> Result<Self, FieldDecodeError> {
        let slice = ValueStorageSlice::from_raw(raw)?;

        Ok(Self { slice })
    }

    /// Validate a complete raw map while selecting its first matching member.
    /// A complete null root has no member; every sibling still passes canonical
    /// traversal at its original depth before any selected view is returned.
    pub(in crate::db) fn from_raw_map_text_key(
        raw: &'a [u8],
        key: &[u8],
    ) -> Result<Option<Self>, FieldDecodeError> {
        if raw == [TAG_NULL] {
            return Ok(None);
        }
        let Some((TAG_MAP, len, mut cursor)) = parse_binary_head(raw, 0)? else {
            return Err(FieldDecodeError::new());
        };
        // Keep the canonical map's child-count admission before traversal.
        len.checked_mul(2).ok_or_else(FieldDecodeError::new)?;
        let mut selected = None;
        for _ in 0..len {
            let key_start = cursor;
            cursor = skip_canonical_value(raw, cursor, 1)?;
            let value_start = cursor;
            cursor = skip_canonical_value(raw, cursor, 1)?;
            if selected.is_none()
                && decode_binary_text_payload_bytes_if_text(&raw[key_start..value_start])?
                    .is_some_and(|found| found == key)
            {
                selected = Some(&raw[value_start..cursor]);
            }
        }
        if cursor != raw.len() {
            return Err(FieldDecodeError::new());
        }

        selected.map(Self::from_skip_bounded).transpose()
    }

    /// Wrap bytes whose exact boundary was already returned by skip traversal.
    fn from_skip_bounded(bytes: &'a [u8]) -> Result<Self, FieldDecodeError> {
        Ok(Self {
            slice: ValueStorageSlice::from_skip_bounded(bytes)?,
        })
    }

    /// Return the bytes covered by this view.
    #[inline]
    pub(in crate::db) const fn as_bytes(&self) -> &'a [u8] {
        self.slice.as_bytes()
    }

    /// Return the value-storage tag without decoding the payload.
    #[inline]
    const fn tag(&self) -> u8 {
        self.slice.as_bytes()[0]
    }

    /// Return whether this view contains a generic null value.
    #[inline]
    pub(in crate::db) const fn is_null(&self) -> bool {
        self.tag() == TAG_NULL
    }

    /// Return whether this view contains a generic bool value.
    #[inline]
    pub(in crate::db::data) const fn is_bool(&self) -> bool {
        matches!(self.tag(), TAG_FALSE | TAG_TRUE)
    }

    /// Return whether this view contains a generic i64 value.
    #[inline]
    pub(in crate::db::data) const fn is_i64(&self) -> bool {
        self.tag() == TAG_INT64
    }

    /// Return whether this view contains a generic u64 value.
    #[inline]
    pub(in crate::db::data) const fn is_u64(&self) -> bool {
        self.tag() == TAG_NAT64
    }

    /// Return whether this view contains a generic text value.
    #[inline]
    pub(in crate::db::data) const fn is_text(&self) -> bool {
        self.tag() == TAG_TEXT
    }

    /// Return whether this view contains a generic blob value.
    #[inline]
    pub(in crate::db::data) const fn is_blob(&self) -> bool {
        self.tag() == TAG_BYTES
    }

    /// Decode one bool directly from the bounded value-storage slice.
    pub(in crate::db::data) const fn as_bool(&self) -> Result<bool, FieldDecodeError> {
        match self.tag() {
            TAG_FALSE => Ok(false),
            TAG_TRUE => Ok(true),
            _ => Err(FieldDecodeError::new()),
        }
    }

    /// Decode one i64 directly from the bounded value-storage slice.
    pub(in crate::db::data) fn as_i64(&self) -> Result<i64, FieldDecodeError> {
        decode_binary_i64_scalar(&self.slice)
    }

    /// Decode one u64 directly from the bounded value-storage slice.
    pub(in crate::db::data) fn as_u64(&self) -> Result<u64, FieldDecodeError> {
        decode_binary_u64_scalar(&self.slice)
    }

    /// Decode one borrowed string directly from the bounded value-storage slice.
    pub(in crate::db::data) fn as_text(&self) -> Result<&'a str, FieldDecodeError> {
        decode_binary_text_scalar(&self.slice)
    }

    /// Decode one borrowed blob directly from the bounded value-storage slice.
    pub(in crate::db::data) fn as_blob(&self) -> Result<&'a [u8], FieldDecodeError> {
        decode_binary_blob_scalar(&self.slice)
    }

    /// Return the value slice for one text-keyed map entry using byte equality.
    pub(in crate::db) fn map_text_key_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Self>, FieldDecodeError> {
        // Segment bytes are compiled once from Rust `String`s. Comparing them
        // against borrowed text payload bytes avoids per-row UTF-8 decoding
        // while preserving map-entry boundary validation. The view already
        // proves the entire envelope valid, including every later sibling, so
        // finding the first match does not require scanning the suffix again.
        let raw_bytes = self.as_bytes();
        let Some((tag, len, payload_start)) = parse_binary_head(raw_bytes, 0)? else {
            return Err(FieldDecodeError::new());
        };
        if tag != TAG_MAP {
            return Err(FieldDecodeError::new());
        }

        let mut cursor = payload_start;
        for _ in 0..len {
            let key_start = cursor;
            cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;
            let value_start = cursor;
            cursor = skip_value_storage_binary_value(raw_bytes, cursor)?;

            if decode_binary_text_payload_bytes_if_text(&raw_bytes[key_start..value_start])?
                .is_some_and(|found| found == key)
            {
                return Self::from_skip_bounded(&raw_bytes[value_start..cursor]).map(Some);
            }
        }

        Ok(None)
    }
}
