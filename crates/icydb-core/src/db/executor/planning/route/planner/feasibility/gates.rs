//! Module: executor::planning::route::planner::feasibility::gates
//! Responsibility: route feasibility pre-gates.
//! Does not own: route intent derivation or execution-stage selection.
//! Boundary: exposes pure gate decisions consumed by feasibility derivation.

use crate::db::executor::aggregate::AggregateKind;

/// Return whether index-range limit pushdown may run for grouped state.
#[must_use]
pub(super) const fn index_range_limit_pushdown_allowed_for_grouped(grouped: bool) -> bool {
    !grouped
}

/// Return whether load scan hints may be derived for this route intent.
#[must_use]
pub(super) const fn load_scan_hints_allowed_for_intent(
    kind: Option<AggregateKind>,
    grouped: bool,
) -> bool {
    kind.is_none() && !grouped
}
