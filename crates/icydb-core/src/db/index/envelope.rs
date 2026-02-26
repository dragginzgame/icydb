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
