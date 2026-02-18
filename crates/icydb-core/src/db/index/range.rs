use crate::{
    db::index::{IndexId, IndexKey, RawIndexKey, encode_canonical_index_component},
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

///
/// Direction
///
/// Execution-time traversal direction for range continuation behavior.
/// DESC is structurally represented but currently follows ASC traversal semantics.
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
/// raw_bounds_for_index_component_range
///
/// Build raw key-space bounds for one ranged component after an equality prefix.
/// This is the canonical path shared by execution and cursor-anchor validation.
///

pub(in crate::db) fn raw_bounds_for_index_component_range<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    let index_id = IndexId::new::<E>(index);

    let mut prefix_components = Vec::with_capacity(prefix.len());
    for value in prefix {
        let component = encode_canonical_index_component(value)
            .map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
        prefix_components.push(component);
    }

    let lower_component = encode_index_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let upper_component = encode_index_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;
    let (start, end) = IndexKey::bounds_for_prefix_component_range(
        &index_id,
        index.fields.len(),
        &prefix_components,
        lower_component,
        upper_component,
    );

    Ok((raw_index_key_bound(start), raw_index_key_bound(end)))
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
    match direction {
        Direction::Asc => (Bound::Excluded(anchor.clone()), upper),
        // Structural containment for DESC is in place; traversal remains ASC for now.
        Direction::Desc => (lower, Bound::Excluded(anchor.clone())),
    }
}

///
/// anchor_within_envelope
///
/// Validate that a continuation anchor stays within the original raw-key envelope.
/// DESC currently reuses ASC envelope semantics until reverse traversal is enabled.
///

#[must_use]
pub(in crate::db) fn anchor_within_envelope(
    direction: Direction,
    anchor: &RawIndexKey,
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    let _ = direction;
    key_within_bounds(anchor, lower, upper)
}

///
/// continuation_advanced
///
/// Validate strict monotonic advancement relative to the continuation anchor.
/// DESC currently follows ASC monotonic checks until reverse traversal is enabled.
///

#[must_use]
pub(in crate::db) fn continuation_advanced(
    direction: Direction,
    candidate: &RawIndexKey,
    anchor: &RawIndexKey,
) -> bool {
    let _ = direction;
    candidate > anchor
}

fn encode_index_component_bound(
    bound: &Bound<Value>,
    kind: IndexRangeBoundEncodeError,
) -> Result<Bound<Vec<u8>>, IndexRangeBoundEncodeError> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_canonical_index_component(value)
            .map(Bound::Included)
            .map_err(|_| kind),
        Bound::Excluded(value) => encode_canonical_index_component(value)
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

fn key_within_bounds(
    key: &RawIndexKey,
    lower: &Bound<RawIndexKey>,
    upper: &Bound<RawIndexKey>,
) -> bool {
    let lower_ok = match lower {
        Bound::Unbounded => true,
        Bound::Included(boundary) => key >= boundary,
        Bound::Excluded(boundary) => key > boundary,
    };
    let upper_ok = match upper {
        Bound::Unbounded => true,
        Bound::Included(boundary) => key <= boundary,
        Bound::Excluded(boundary) => key < boundary,
    };

    lower_ok && upper_ok
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::index::{Direction, RawIndexKey, anchor_within_envelope, resume_bounds},
        traits::Storable,
    };
    use std::{borrow::Cow, ops::Bound};

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
            "ASC and DESC envelope containment must match before DESC traversal is enabled",
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
}
