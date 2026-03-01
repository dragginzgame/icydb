//! Module: db::executor::route::semantics
//! Responsibility: executor-owned runtime derivation helpers over logical enums.
//! Does not own: planner validation or user-facing logical semantics.
//! Boundary: route/executor runtime interpretation of already-validated plan kinds.

use crate::db::{
    direction::Direction,
    query::{
        builder::AggregateExpr,
        plan::{AggregateKind, OrderDirection},
    },
};

/// Convert canonical order direction into execution scan direction.
#[must_use]
pub(in crate::db::executor) const fn direction_from_order(direction: OrderDirection) -> Direction {
    match direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}

/// Convert execution scan direction into canonical order direction.
#[must_use]
pub(in crate::db::executor) const fn order_direction_from_direction(
    direction: Direction,
) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}

/// Return the canonical extrema traversal direction for this aggregate kind.
#[must_use]
pub(in crate::db::executor) const fn aggregate_extrema_direction(
    kind: AggregateKind,
) -> Option<Direction> {
    AggregateExpr::extrema_direction_for_kind(kind)
}

/// Return the canonical non-short-circuit materialized reduction direction.
#[must_use]
pub(in crate::db::executor) const fn aggregate_materialized_fold_direction(
    kind: AggregateKind,
) -> Direction {
    AggregateExpr::materialized_fold_direction_for_kind(kind)
}

/// Return true when this kind can use bounded aggregate probe hints.
#[must_use]
pub(in crate::db::executor) const fn aggregate_supports_bounded_probe_hint(
    kind: AggregateKind,
) -> bool {
    AggregateExpr::supports_bounded_probe_hint_for_kind(kind)
}

/// Derive a bounded aggregate probe fetch hint for this kind.
#[must_use]
pub(in crate::db::executor) fn aggregate_bounded_probe_fetch_hint(
    kind: AggregateKind,
    direction: Direction,
    offset: usize,
    page_limit: Option<usize>,
) -> Option<usize> {
    AggregateExpr::bounded_probe_fetch_hint_for_kind(kind, direction, offset, page_limit)
}
