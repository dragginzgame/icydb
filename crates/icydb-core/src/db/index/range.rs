//! Module: index::range
//! Responsibility: semantic-to-raw range lowering for index key traversal.
//! Does not own: continuation token verification or index-store scanning.
//! Boundary: planner/cursor paths call this module to build raw bounds.

use crate::{
    db::index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey},
    model::index::IndexModel,
    value::Value,
};
use std::ops::Bound;

///
/// IndexRangeBoundEncodeError
///
/// Reason a logical `IndexRange` bound shape could not be translated into
/// canonical raw index-key bounds.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexRangeBoundEncodeError {
    Prefix,
    Lower,
    Upper,
}

impl IndexRangeBoundEncodeError {
    #[must_use]
    pub(in crate::db) const fn validated_spec_not_indexable_reason(self) -> &'static str {
        match self {
            Self::Prefix => "validated index-range prefix is not indexable",
            Self::Lower => "validated index-range lower bound is not indexable",
            Self::Upper => "validated index-range upper bound is not indexable",
        }
    }

    #[must_use]
    pub(in crate::db) const fn cursor_anchor_not_indexable_reason(self) -> &'static str {
        match self {
            Self::Prefix => "index-range continuation anchor prefix is not indexable",
            Self::Lower => "index-range cursor lower continuation bound is not indexable",
            Self::Upper => "index-range cursor upper continuation bound is not indexable",
        }
    }
}

///
/// raw_keys_for_encoded_prefix
///
/// Build canonical raw start/end keys for an encoded prefix in the user namespace.
///

#[must_use]
pub(in crate::db) fn raw_keys_for_encoded_prefix(
    index_id: &IndexId,
    index: &IndexModel,
    prefix: &[EncodedValue],
) -> (RawIndexKey, RawIndexKey) {
    raw_keys_for_encoded_prefix_with_kind(
        index_id,
        IndexKeyKind::User,
        index.fields().len(),
        prefix,
    )
}

///
/// raw_keys_for_encoded_prefix_with_kind
///
/// Build canonical raw start/end keys for an encoded prefix in the requested key namespace.
///

#[must_use]
pub(in crate::db) fn raw_keys_for_encoded_prefix_with_kind(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[EncodedValue],
) -> (RawIndexKey, RawIndexKey) {
    raw_keys_for_component_prefix_with_kind(index_id, key_kind, index_len, prefix)
}

/// Build canonical raw start/end keys for any pre-encoded prefix bytes in the
/// requested key namespace.
#[must_use]
pub(in crate::db) fn raw_keys_for_component_prefix_with_kind<C: AsRef<[u8]>>(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[C],
) -> (RawIndexKey, RawIndexKey) {
    let (start, end) = IndexKey::bounds_for_prefix_with_kind(index_id, key_kind, index_len, prefix);

    (start.to_raw(), end.to_raw())
}

///
/// raw_bounds_for_encoded_index_component_range
///
/// Build raw key-space bounds from pre-encoded index components.
///

pub(in crate::db) fn raw_bounds_for_encoded_index_component_range(
    index_id: &IndexId,
    index: &IndexModel,
    prefix: &[EncodedValue],
    lower: &Bound<EncodedValue>,
    upper: &Bound<EncodedValue>,
) -> (Bound<RawIndexKey>, Bound<RawIndexKey>) {
    let lower_component = encoded_component_bound(lower);
    let upper_component = encoded_component_bound(upper);
    let (start, end) = IndexKey::bounds_for_prefix_component_range(
        index_id,
        index.fields().len(),
        prefix,
        &lower_component,
        &upper_component,
    );

    (raw_index_key_bound(start), raw_index_key_bound(end))
}

///
/// raw_bounds_for_semantic_index_component_range
///
/// Build raw key-space bounds from semantic index components.
/// This is the semantic-to-physical lowering boundary for index-range access.
///

pub(in crate::db) fn raw_bounds_for_semantic_index_component_range(
    index_id: &IndexId,
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    // Phase 1: encode semantic values into canonical index-component bytes.
    let encoded_prefix =
        EncodedValue::try_encode_all(prefix).map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
    let encoded_lower = encode_semantic_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let encoded_upper = encode_semantic_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;

    // Phase 2: lower encoded bounds to canonical raw index-key bounds.
    Ok(raw_bounds_for_encoded_index_component_range(
        index_id,
        index,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
    ))
}

/// Return the smallest strict lexical successor prefix, or `None` when the
/// input is already at the terminal Unicode scalar boundary.
pub(in crate::db) fn next_text_prefix(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    for index in (0..chars.len()).rev() {
        let Some(next_char) = next_unicode_scalar(chars[index]) else {
            continue;
        };
        chars.truncate(index);
        chars.push(next_char);
        return Some(chars.into_iter().collect());
    }

    None
}

const fn encoded_component_bound(bound: &Bound<EncodedValue>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value.encoded()),
        Bound::Excluded(value) => Bound::Excluded(value.encoded()),
    }
}

fn encode_semantic_component_bound(
    bound: &Bound<Value>,
    kind: IndexRangeBoundEncodeError,
) -> Result<Bound<EncodedValue>, IndexRangeBoundEncodeError> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => EncodedValue::try_from_ref(value)
            .map(Bound::Included)
            .map_err(|_| kind),
        Bound::Excluded(value) => EncodedValue::try_from_ref(value)
            .map(Bound::Excluded)
            .map_err(|_| kind),
    }
}

fn raw_index_key_bound(bound: Bound<IndexKey>) -> Bound<RawIndexKey> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(key.to_raw()),
        Bound::Excluded(key) => Bound::Excluded(key.to_raw()),
    }
}

fn next_unicode_scalar(value: char) -> Option<char> {
    if value == char::MAX {
        return None;
    }

    let mut next = u32::from(value).saturating_add(1);
    if (0xD800..=0xDFFF).contains(&next) {
        next = 0xE000;
    }

    char::from_u32(next)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::IndexRangeBoundEncodeError;

    #[test]
    fn index_range_bound_encode_error_owns_validated_spec_reason_text() {
        assert_eq!(
            IndexRangeBoundEncodeError::Prefix.validated_spec_not_indexable_reason(),
            "validated index-range prefix is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Lower.validated_spec_not_indexable_reason(),
            "validated index-range lower bound is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Upper.validated_spec_not_indexable_reason(),
            "validated index-range upper bound is not indexable",
        );
    }

    #[test]
    fn index_range_bound_encode_error_owns_cursor_anchor_reason_text() {
        assert_eq!(
            IndexRangeBoundEncodeError::Prefix.cursor_anchor_not_indexable_reason(),
            "index-range continuation anchor prefix is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Lower.cursor_anchor_not_indexable_reason(),
            "index-range cursor lower continuation bound is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Upper.cursor_anchor_not_indexable_reason(),
            "index-range cursor upper continuation bound is not indexable",
        );
    }
}
