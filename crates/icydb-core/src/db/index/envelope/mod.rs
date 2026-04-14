//! Module: index::envelope
//! Responsibility: canonical bound-envelope and continuation-envelope helpers for index-domain operations.
//! Does not own: planner continuation policy or token wire formats.
//! Boundary: index-owned key-envelope semantics consumed by cursor/runtime/index layers.

#[cfg(test)]
mod tests;

use crate::{
    db::{direction::Direction, index::RawIndexKey},
    error::InternalError,
};
use std::ops::Bound;

/// key_within_envelope
///
/// Validate that one key is contained by one canonical bound envelope.
/// This centralizes inclusive/exclusive bound semantics under index authority.
#[must_use]
pub(in crate::db) fn key_within_envelope<K: Ord + Clone>(
    key: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    KeyEnvelope::new(lower.clone(), upper.clone()).contains(key)
}

/// Shared directional strict-advancement comparator for continuation checks.
/// `candidate` advances only when it is strictly after `anchor` under direction.
#[must_use]
pub(in crate::db) fn continuation_advanced<K: Ord>(
    direction: Direction,
    candidate: &K,
    anchor: &K,
) -> bool {
    match direction {
        Direction::Asc => candidate > anchor,
        Direction::Desc => candidate < anchor,
    }
}

/// Rewrite continuation bounds while cloning only retained bound edges.
#[must_use]
pub(in crate::db) fn resume_bounds_from_refs<K: Clone + Ord>(
    direction: Direction,
    lower: &Bound<K>,
    upper: &Bound<K>,
    anchor: &K,
) -> (Bound<K>, Bound<K>) {
    #[cfg(debug_assertions)]
    {
        debug_assert!(
            key_within_envelope(anchor, lower, upper),
            "cursor anchor escaped envelope",
        );

        let bounds_key_ordered = match (lower, upper) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (
                Bound::Included(lower_key) | Bound::Excluded(lower_key),
                Bound::Included(upper_key) | Bound::Excluded(upper_key),
            ) => lower_key <= upper_key,
        };
        debug_assert!(
            bounds_key_ordered,
            "index envelope bounds must remain ordered before continuation rewrite",
        );
    }

    match direction {
        Direction::Asc => (Bound::Excluded(anchor.clone()), upper.clone()),
        Direction::Desc => (lower.clone(), Bound::Excluded(anchor.clone())),
    }
}

/// Validate continuation anchor containment against the original index-scan envelope.
pub(in crate::db) fn validate_index_scan_continuation_envelope<K: Ord + Clone>(
    anchor: Option<&K>,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> Result<(), InternalError> {
    if let Some(anchor) = anchor
        && !key_within_envelope(anchor, lower, upper)
    {
        return Err(InternalError::index_scan_continuation_anchor_within_envelope_required());
    }

    Ok(())
}

/// Validate one optional continuation anchor and derive resumed scan bounds.
pub(in crate::db) fn resume_bounds_for_continuation<K: Clone + Ord>(
    direction: Direction,
    anchor: Option<&K>,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> Result<(Bound<K>, Bound<K>), InternalError> {
    // Phase 1: fail closed when the supplied anchor escaped the original scan envelope.
    validate_index_scan_continuation_envelope(anchor, lower, upper)?;

    // Phase 2: rewrite only the bound edge owned by the scan direction.
    Ok(match anchor {
        Some(anchor) => resume_bounds_from_refs(direction, lower, upper, anchor),
        None => (lower.clone(), upper.clone()),
    })
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

/// Validate strict directional continuation advancement for one scan candidate.
pub(in crate::db) fn validate_index_scan_continuation_advancement<K: Ord>(
    direction: Direction,
    anchor: Option<&K>,
    candidate: &K,
) -> Result<(), InternalError> {
    if let Some(anchor) = anchor
        && !continuation_advanced(direction, candidate, anchor)
    {
        return Err(InternalError::index_scan_continuation_advancement_required());
    }

    Ok(())
}

///
/// KeyEnvelope
///
/// Canonical raw-key envelope with inclusive/exclusive bound semantics.
/// This type models containment only; cursor continuation advancement semantics
/// are intentionally owned by `db::cursor`.
///

pub(in crate::db) struct KeyEnvelope<K> {
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord,
{
    pub(in crate::db) const fn new(lower: Bound<K>, upper: Bound<K>) -> Self {
        Self { lower, upper }
    }

    pub(in crate::db) fn contains(&self, key: &K) -> bool {
        // Envelope containment is purely bound-based and direction-agnostic.
        let lower_ok = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key >= boundary,
            Bound::Excluded(boundary) => key > boundary,
        };
        let upper_ok = match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(boundary) => key <= boundary,
            Bound::Excluded(boundary) => key < boundary,
        };

        lower_ok && upper_ok
    }
}

const fn bound_key_ref(bound: &Bound<RawIndexKey>) -> Option<&RawIndexKey> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}
