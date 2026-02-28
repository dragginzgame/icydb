//! Module: index::envelope
//! Responsibility: directional continuation advancement and envelope checks.
//! Does not own: range encoding or cursor-token signature policy.
//! Boundary: shared by index range and cursor resume paths.

use crate::db::direction::Direction;
use std::{cmp::Ordering, ops::Bound};

///
/// DirectionComparator
///
/// Direction-aware key comparator used by cursor resume and continuation checks.
/// Keeps strict "after anchor" semantics in one place.
///

struct DirectionComparator {
    direction: Direction,
}

impl DirectionComparator {
    const fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn is_strictly_after<K: Ord>(&self, candidate: &K, anchor: &K) -> bool {
        continuation_advances(self.direction, anchor, candidate)
    }
}

///
/// continuation_advances_from_ordering
///
/// Shared strict continuation predicate from one precomputed ordering.
/// Centralizes the strict "after" rule (`Greater` only) across cursor layers.
///
#[must_use]
pub(in crate::db) const fn continuation_advances_from_ordering(ordering: Ordering) -> bool {
    ordering.is_gt()
}

///
/// continuation_advances
///
/// Shared directional strict-advancement comparator for continuation checks.
/// `candidate` advances only when it is strictly after `anchor` under direction.
///
#[must_use]
pub(in crate::db) fn continuation_advances<K: Ord>(
    direction: Direction,
    anchor: &K,
    candidate: &K,
) -> bool {
    let ordering = match direction {
        Direction::Asc => candidate.cmp(anchor),
        Direction::Desc => anchor.cmp(candidate),
    };

    continuation_advances_from_ordering(ordering)
}

///
/// continuation_advanced
///
/// Validate strict monotonic advancement relative to one continuation anchor.
///
#[must_use]
pub(in crate::db) fn continuation_advanced<K: Ord>(
    direction: Direction,
    candidate: &K,
    anchor: &K,
) -> bool {
    KeyEnvelope::new(direction, Bound::Unbounded, Bound::Unbounded)
        .continuation_advanced(candidate, anchor)
}

///
/// resume_bounds_from_refs
///
/// Rewrite continuation bounds while cloning only retained bound edges.
///
#[must_use]
pub(in crate::db) fn resume_bounds_from_refs<K: Clone>(
    direction: Direction,
    lower: &Bound<K>,
    upper: &Bound<K>,
    anchor: &K,
) -> (Bound<K>, Bound<K>) {
    match direction {
        Direction::Asc => (Bound::Excluded(anchor.clone()), upper.clone()),
        Direction::Desc => (lower.clone(), Bound::Excluded(anchor.clone())),
    }
}

///
/// anchor_within_envelope
///
/// Validate that a continuation anchor remains inside the original envelope.
///
#[must_use]
pub(in crate::db) fn anchor_within_envelope<K: Ord + Clone>(
    direction: Direction,
    anchor: &K,
    lower: &Bound<K>,
    upper: &Bound<K>,
) -> bool {
    KeyEnvelope::new(direction, lower.clone(), upper.clone()).contains(anchor)
}

///
/// KeyEnvelope
///
/// Canonical raw-key envelope with direction-aware continuation semantics.
/// Centralizes anchor rewrite, containment checks, monotonic advancement, and
/// empty-envelope detection for cursor continuation paths.
///

pub(in crate::db) struct KeyEnvelope<K> {
    comparator: DirectionComparator,
    lower: Bound<K>,
    upper: Bound<K>,
}

impl<K> KeyEnvelope<K>
where
    K: Ord,
{
    pub(in crate::db) const fn new(direction: Direction, lower: Bound<K>, upper: Bound<K>) -> Self {
        Self {
            comparator: DirectionComparator::new(direction),
            lower,
            upper,
        }
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

    pub(in crate::db) fn continuation_advanced(&self, candidate: &K, anchor: &K) -> bool {
        self.comparator.is_strictly_after(candidate, anchor)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{direction::Direction, index::RawIndexKey},
        traits::Storable,
    };
    use std::{borrow::Cow, ops::Bound};

    use super::{anchor_within_envelope, continuation_advanced};

    fn raw_key(byte: u8) -> RawIndexKey {
        <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![byte]))
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
