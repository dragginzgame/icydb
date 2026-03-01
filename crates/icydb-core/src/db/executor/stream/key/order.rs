//! Module: executor::stream::key::order
//! Responsibility: comparator policy for ordered key streams.
//! Does not own: stream traversal mechanics or access-path resolution.
//! Boundary: centralizes ASC/DESC comparison behavior for stream combinators.

use crate::db::{
    data::{DataKey, StorageKey},
    direction::Direction,
};
use std::cmp::Ordering;

///
/// KeyOrderComparator
///
/// Comparator wrapper for ordered key stream monotonicity and merge decisions.
/// This keeps stream combinators comparator-driven instead of directly branching
/// on traversal direction at each call site.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyOrderComparator {
    direction: Direction,
}

impl KeyOrderComparator {
    /// Construct comparator policy from traversal direction.
    #[must_use]
    pub(crate) const fn from_direction(direction: Direction) -> Self {
        Self { direction }
    }

    /// Compare two data keys under this comparator direction policy.
    pub(in crate::db::executor) fn compare_data_keys(
        self,
        left: &DataKey,
        right: &DataKey,
    ) -> Ordering {
        match self.direction {
            Direction::Asc => left.cmp(right),
            Direction::Desc => right.cmp(left),
        }
    }

    /// Compare two storage keys under this comparator direction policy.
    pub(in crate::db::executor) fn compare_storage_keys(
        self,
        left: &StorageKey,
        right: &StorageKey,
    ) -> Ordering {
        match self.direction {
            Direction::Asc => left.cmp(right),
            Direction::Desc => right.cmp(left),
        }
    }

    // Return whether `current` violates stream monotonicity after `previous`.
    pub(super) fn violates_monotonicity(self, previous: &StorageKey, current: &StorageKey) -> bool {
        self.compare_storage_keys(previous, current).is_gt()
    }

    // Human-readable direction label for invariant diagnostics.
    pub(super) const fn order_label(self) -> &'static str {
        match self.direction {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
        }
    }
}
