//! Module: data::structural_field::value_storage::skip
//! Responsibility: borrowed traversal through the canonical value-storage owner.
//! Does not own: recursive grammar, runtime construction, or field-kind routing.
//! Boundary: scalar widths stay local; canonical traversal owns every child value.

use crate::db::data::structural_field::{
    FieldDecodeError,
    binary::{
        TAG_BYTES, TAG_FALSE, TAG_INT64, TAG_NAT64, TAG_NULL, TAG_TEXT, TAG_TRUE, TAG_UNIT,
        skip_binary_value,
    },
    value_storage::{
        canonical::skip_canonical_value,
        tags::{fixed_value_storage_payload_len, is_nested_value_storage_tag},
    },
};

// All borrowed callers share canonical enum frames and accepted value depth.
pub(super) fn skip_value_storage_binary_value(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    skip_canonical_value(raw_bytes, offset, 0)
}

// Scalar payload frames do not add a recursive value level. This leaf helper
// cannot recurse into collections or enums; those belong to canonical traversal.
pub(super) fn skip_value_storage_scalar(
    raw_bytes: &[u8],
    offset: usize,
) -> Result<usize, FieldDecodeError> {
    let tag = *raw_bytes.get(offset).ok_or_else(FieldDecodeError::new)?;
    if let Some(len) = fixed_value_storage_payload_len(tag) {
        let end = offset
            .checked_add(1 + len)
            .ok_or_else(FieldDecodeError::new)?;
        raw_bytes
            .get(offset..end)
            .ok_or_else(FieldDecodeError::new)?;
        return Ok(end);
    }
    match tag {
        TAG_NULL | TAG_UNIT | TAG_FALSE | TAG_TRUE | TAG_INT64 | TAG_NAT64 | TAG_TEXT
        | TAG_BYTES => skip_binary_value(raw_bytes, offset),
        other if is_nested_value_storage_tag(other) => skip_binary_value(raw_bytes, offset + 1),
        _ => Err(FieldDecodeError::new()),
    }
}
