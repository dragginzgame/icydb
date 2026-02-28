//! Module: index::range
//! Responsibility: semantic-to-raw range lowering for index key traversal.
//! Does not own: continuation token verification or index-store scanning.
//! Boundary: planner/cursor paths call this module to build raw bounds.

use crate::{
    db::index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey},
    model::index::IndexModel,
    traits::EntityKind,
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

///
/// raw_keys_for_encoded_prefix
///
/// Build canonical raw start/end keys for an encoded prefix in the user namespace.
///

#[must_use]
pub(in crate::db) fn raw_keys_for_encoded_prefix<E: EntityKind>(
    index: &IndexModel,
    prefix: &[EncodedValue],
) -> (RawIndexKey, RawIndexKey) {
    let index_id = IndexId::new::<E>(index);
    raw_keys_for_encoded_prefix_with_kind(&index_id, IndexKeyKind::User, index.fields.len(), prefix)
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
    let (start, end) = IndexKey::bounds_for_prefix_with_kind(index_id, key_kind, index_len, prefix);

    (start.to_raw(), end.to_raw())
}

///
/// raw_bounds_for_encoded_index_component_range
///
/// Build raw key-space bounds from pre-encoded index components.
///

pub(in crate::db) fn raw_bounds_for_encoded_index_component_range<E: EntityKind>(
    index: &IndexModel,
    prefix: &[EncodedValue],
    lower: &Bound<EncodedValue>,
    upper: &Bound<EncodedValue>,
) -> (Bound<RawIndexKey>, Bound<RawIndexKey>) {
    let index_id = IndexId::new::<E>(index);

    let lower_component = encoded_component_bound(lower);
    let upper_component = encoded_component_bound(upper);
    let (start, end) = IndexKey::bounds_for_prefix_component_range(
        &index_id,
        index.fields.len(),
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

pub(in crate::db) fn raw_bounds_for_semantic_index_component_range<E: EntityKind>(
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
    Ok(raw_bounds_for_encoded_index_component_range::<E>(
        index,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
    ))
}

///
/// envelope_is_empty
///
/// Validate whether raw index-key bounds encode an empty traversal envelope.
///

#[must_use]
pub(in crate::db) fn envelope_is_empty(
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    // Unbounded envelopes are never empty by construction.
    let (Some(lower_key), Some(upper_key)) = (bound_key_ref(lower), bound_key_ref(upper)) else {
        return false;
    };

    if lower_key < upper_key {
        return false;
    }
    if lower_key > upper_key {
        return true;
    }

    !matches!(lower, Bound::Included(_)) || !matches!(upper, Bound::Included(_))
}

const fn bound_key_ref(bound: &Bound<RawIndexKey>) -> Option<&RawIndexKey> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{db::index::RawIndexKey, traits::Storable};
    use std::{borrow::Cow, ops::Bound};

    use super::envelope_is_empty;

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn envelope_emptiness_identifies_empty_equal_exclusive_bounds() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x10));

        assert!(envelope_is_empty(&lower, &upper));
    }
}
