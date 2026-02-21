use crate::{
    db::{
        index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey},
        query::plan::KeyEnvelope,
    },
    model::index::IndexModel,
    traits::EntityKind,
};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

///
/// Direction
///
/// Execution-time traversal direction for range continuation behavior.
///

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Direction {
    #[default]
    Asc,
    Desc,
}

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
/// map_bound_encode_error
///
/// Map a bound-encode variant to the caller-provided reason string for that
/// bound position. Callers keep ownership of their error class and boundary.
///

#[must_use]
pub(in crate::db) const fn map_bound_encode_error(
    err: IndexRangeBoundEncodeError,
    prefix_reason: &'static str,
    lower_reason: &'static str,
    upper_reason: &'static str,
) -> &'static str {
    match err {
        IndexRangeBoundEncodeError::Prefix => prefix_reason,
        IndexRangeBoundEncodeError::Lower => lower_reason,
        IndexRangeBoundEncodeError::Upper => upper_reason,
    }
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
/// resume_bounds
///
/// Rewrite raw continuation bounds based on direction and anchor.
/// This is the single authority for index-range continuation bound rewriting.
///

#[must_use]
pub(in crate::db) fn resume_bounds(
    direction: Direction,
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
    anchor: &RawIndexKey,
) -> (Bound<RawIndexKey>, Bound<RawIndexKey>) {
    KeyEnvelope::new(direction, lower, upper)
        .apply_anchor(anchor)
        .into_bounds()
}

///
/// anchor_within_envelope
///
/// Validate that a continuation anchor stays within the original raw-key envelope.
/// Envelope containment remains direction-agnostic over the same raw bounds.
///

#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db) fn anchor_within_envelope(
    direction: Direction,
    anchor: &RawIndexKey,
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    KeyEnvelope::new(direction, lower.clone(), upper.clone()).contains(anchor)
}

///
/// continuation_advanced
///
/// Validate strict monotonic advancement relative to the continuation anchor.
///

#[must_use]
pub(in crate::db) fn continuation_advanced(
    direction: Direction,
    candidate: &RawIndexKey,
    anchor: &RawIndexKey,
) -> bool {
    KeyEnvelope::new(direction, Bound::Unbounded, Bound::Unbounded)
        .continuation_advanced(candidate, anchor)
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
    KeyEnvelope::new(Direction::Asc, lower.clone(), upper.clone()).is_empty()
}

const fn encoded_component_bound(bound: &Bound<EncodedValue>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value.encoded()),
        Bound::Excluded(value) => Bound::Excluded(value.encoded()),
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

    use super::{Direction, anchor_within_envelope, continuation_advanced, resume_bounds};

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
    }

    #[test]
    fn resume_bounds_asc_rewrites_lower_only() {
        let lower = Bound::Included(raw_key(0x01));
        let upper = Bound::Excluded(raw_key(0x09));
        let anchor = raw_key(0x05);

        let (next_lower, next_upper) = resume_bounds(Direction::Asc, lower, upper.clone(), &anchor);

        assert_eq!(next_lower, Bound::Excluded(anchor));
        assert_eq!(next_upper, upper);
    }

    #[test]
    fn resume_bounds_directional_rewrite_symmetry_table() {
        let lower = Bound::Included(raw_key(0x01));
        let upper = Bound::Included(raw_key(0x09));
        let anchor = raw_key(0x05);

        let (asc_lower, asc_upper) =
            resume_bounds(Direction::Asc, lower.clone(), upper.clone(), &anchor);
        assert_eq!(asc_lower, Bound::Excluded(anchor.clone()));
        assert_eq!(asc_upper, upper);

        let (desc_lower, desc_upper) =
            resume_bounds(Direction::Desc, lower.clone(), upper, &anchor);
        assert_eq!(desc_lower, lower);
        assert_eq!(desc_upper, Bound::Excluded(anchor));
    }

    #[test]
    fn anchor_within_envelope_is_bidirectionally_contained_for_current_model() {
        let lower = Bound::Included(raw_key(0x10));
        let upper = Bound::Excluded(raw_key(0x20));
        let inside = raw_key(0x18);
        let below = raw_key(0x0F);
        let at_excluded_upper = raw_key(0x20);

        assert_eq!(
            anchor_within_envelope(Direction::Asc, &inside, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &inside, &lower, &upper),
            "ASC and DESC envelope containment must match for equivalent bounds",
        );
        assert_eq!(
            anchor_within_envelope(Direction::Asc, &below, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &below, &lower, &upper),
            "ASC and DESC envelope containment must match for below-lower anchors",
        );
        assert_eq!(
            anchor_within_envelope(Direction::Asc, &at_excluded_upper, &lower, &upper),
            anchor_within_envelope(Direction::Desc, &at_excluded_upper, &lower, &upper),
            "ASC and DESC envelope containment must match for upper-boundary anchors",
        );
    }

    #[test]
    fn continuation_advanced_is_directional() {
        let anchor = raw_key(0x10);
        let asc_candidate = raw_key(0x11);
        let desc_candidate = raw_key(0x0F);

        assert!(continuation_advanced(
            Direction::Asc,
            &asc_candidate,
            &anchor
        ));
        assert!(!continuation_advanced(
            Direction::Asc,
            &desc_candidate,
            &anchor
        ));

        assert!(continuation_advanced(
            Direction::Desc,
            &desc_candidate,
            &anchor
        ));
        assert!(!continuation_advanced(
            Direction::Desc,
            &asc_candidate,
            &anchor
        ));
    }
}
