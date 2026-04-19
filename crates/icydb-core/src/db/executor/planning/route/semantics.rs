//! Module: db::executor::planning::route::semantics
//! Responsibility: executor-owned runtime derivation helpers over logical enums.
//! Does not own: planner validation or user-facing logical semantics.
//! Boundary: route/executor runtime interpretation of already-validated plan kinds.

use crate::db::{direction::Direction, query::plan::AggregateKind};

/// Return the canonical extrema traversal direction for this aggregate kind.
#[must_use]
pub(in crate::db::executor) const fn aggregate_extrema_direction(
    kind: AggregateKind,
) -> Option<Direction> {
    kind.extrema_direction()
}

/// Return the canonical non-short-circuit materialized reduction direction.
#[must_use]
pub(in crate::db::executor) const fn aggregate_materialized_fold_direction(
    kind: AggregateKind,
) -> Direction {
    kind.materialized_fold_direction()
}

/// Return true when this kind can use bounded aggregate probe hints.
#[must_use]
pub(in crate::db::executor) const fn aggregate_supports_bounded_probe_hint(
    kind: AggregateKind,
) -> bool {
    kind.supports_bounded_probe_hint()
}

/// Derive a bounded aggregate probe fetch hint for this kind.
#[must_use]
pub(in crate::db::executor) fn aggregate_bounded_probe_fetch_hint(
    kind: AggregateKind,
    direction: Direction,
    offset: usize,
    page_limit: Option<usize>,
) -> Option<usize> {
    kind.bounded_probe_fetch_hint(direction, offset, page_limit)
}
