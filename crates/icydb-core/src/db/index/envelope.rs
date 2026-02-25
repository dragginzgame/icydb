use crate::db::index::Direction;
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

    fn apply_anchor<K: Clone>(
        &self,
        lower: Bound<K>,
        upper: Bound<K>,
        anchor: &K,
    ) -> (Bound<K>, Bound<K>) {
        match self.direction {
            Direction::Asc => (Bound::Excluded(anchor.clone()), upper),
            Direction::Desc => (lower, Bound::Excluded(anchor.clone())),
        }
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
    K: Ord + Clone,
{
    pub(in crate::db) const fn new(direction: Direction, lower: Bound<K>, upper: Bound<K>) -> Self {
        Self {
            comparator: DirectionComparator::new(direction),
            lower,
            upper,
        }
    }

    // Rewrite the directional continuation edge to strict "after anchor".
    pub(in crate::db) fn apply_anchor(self, anchor: &K) -> Self {
        let (lower, upper) = self.comparator.apply_anchor(self.lower, self.upper, anchor);
        Self {
            comparator: self.comparator,
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

    // Envelope emptiness is defined only by raw lower/upper bound relation.
    // This check is intentionally direction-agnostic.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) fn is_empty_direction_agnostic(&self) -> bool {
        let (Some(lower), Some(upper)) = (
            Self::bound_key_ref(&self.lower),
            Self::bound_key_ref(&self.upper),
        ) else {
            return false;
        };

        if lower < upper {
            return false;
        }
        if lower > upper {
            return true;
        }

        !matches!(&self.lower, Bound::Included(_)) || !matches!(&self.upper, Bound::Included(_))
    }

    pub(in crate::db) fn into_bounds(self) -> (Bound<K>, Bound<K>) {
        (self.lower, self.upper)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    const fn bound_key_ref(bound: &Bound<K>) -> Option<&K> {
        match bound {
            Bound::Included(value) | Bound::Excluded(value) => Some(value),
            Bound::Unbounded => None,
        }
    }
}
