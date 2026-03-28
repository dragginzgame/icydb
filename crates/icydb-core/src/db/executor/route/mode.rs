//! Module: db::executor::route::mode
//! Responsibility: route-owned direction/window/continuation-mode derivation helpers.
//! Does not own: access-shape capability decisions or route execution-mode selection.
//! Boundary: pure derivation primitives consumed by route planning.

use crate::db::{
    direction::Direction,
    query::{
        builder::AggregateExpr,
        plan::{AccessPlannedQuery, ExecutionOrderContract},
    },
};

use crate::db::executor::route::{RouteCapabilities, aggregate_extrema_direction};

// Route-owned aggregate non-count streaming gate.
// Field-target extrema uses route capability flags directly; non-target
// terminals use the shared streaming-safe/pushdown/index-range route gates.
pub(in crate::db::executor) fn aggregate_non_count_streaming_allowed(
    aggregate_expr: Option<&AggregateExpr>,
    capabilities: RouteCapabilities,
    secondary_pushdown_eligible: bool,
    index_range_limit_enabled: bool,
) -> bool {
    if let Some(aggregate) = aggregate_expr
        && aggregate.target_field().is_some()
    {
        return match aggregate_extrema_direction(aggregate.kind()) {
            Some(Direction::Asc) => capabilities.field_min_fast_path_eligible,
            Some(Direction::Desc) => capabilities.field_max_fast_path_eligible,
            None => false,
        };
    }

    if capabilities.stream_order_contract_safe || index_range_limit_enabled {
        return true;
    }

    // Secondary-order pushdown alone is not enough for canonical aggregate
    // streaming on non-field terminals. Those shapes share the ordered index
    // access contract with load execution, but stale secondary rows are
    // reconciled correctly only through the canonical materialized row path.
    // Keeping this lane materialized preserves aggregate/load parity for
    // `EXISTS`, `FIRST`, `LAST`, `MIN`, and `MAX` on ordered secondary shapes.
    let _ = secondary_pushdown_eligible;

    false
}

// Route-owned load streaming gate.
// Load execution remains streaming when canonical streaming-safe shapes
// apply or when route enabled index-range limit pushdown.
pub(in crate::db::executor) const fn load_streaming_allowed(
    capabilities: RouteCapabilities,
    index_range_limit_enabled: bool,
) -> bool {
    capabilities.stream_order_contract_safe || index_range_limit_enabled
}

pub(in crate::db::executor::route) fn derive_load_route_direction(
    plan: &AccessPlannedQuery,
) -> Direction {
    ExecutionOrderContract::from_plan(
        plan.grouped_plan().is_some(),
        plan.scalar_plan().order.as_ref(),
    )
    .primary_scan_direction()
}

pub(in crate::db::executor::route) fn derive_aggregate_route_direction(
    plan: &AccessPlannedQuery,
    aggregate: &AggregateExpr,
) -> Direction {
    if aggregate.target_field().is_some() {
        return aggregate_extrema_direction(aggregate.kind())
            .unwrap_or_else(|| derive_load_route_direction(plan));
    }

    derive_load_route_direction(plan)
}
