//! Module: db::executor::planning::route::mode
//! Responsibility: route-owned direction/window/continuation-mode derivation helpers.
//! Does not own: access-shape capability decisions or route execution-mode selection.
//! Boundary: pure derivation primitives consumed by route planning.

use crate::db::{
    direction::Direction,
    query::plan::{AccessPlannedQuery, ExecutionOrderContract},
};

use crate::db::executor::planning::route::{AggregateRouteShape, RouteCapabilities};

// Route-owned aggregate non-count streaming gate.
// Field-target extrema uses route capability flags directly; non-target
// terminals use the shared streaming-safe/pushdown/index-range route gates.
pub(in crate::db::executor) const fn aggregate_non_count_streaming_allowed(
    aggregate_shape: Option<AggregateRouteShape<'_>>,
    capabilities: RouteCapabilities,
    secondary_pushdown_eligible: bool,
    index_range_limit_enabled: bool,
) -> bool {
    if let Some(aggregate) = aggregate_shape
        && aggregate.target_field().is_some()
    {
        return match aggregate.kind().extrema_direction() {
            Some(Direction::Asc) => capabilities.field_min_fast_path_eligible,
            Some(Direction::Desc) => capabilities.field_max_fast_path_eligible,
            None => false,
        };
    }

    if index_range_limit_enabled {
        return true;
    }
    if capabilities
        .load_order_route_contract()
        .allows_streaming_load()
        && !secondary_pushdown_eligible
    {
        return true;
    }

    // Secondary-order pushdown alone is not enough for canonical aggregate
    // streaming on non-field terminals. Those shapes share the ordered index
    // access contract with load execution, but stale secondary rows are
    // reconciled correctly only through the canonical materialized row path.
    // Keeping this lane materialized preserves aggregate/load parity for
    // `EXISTS`, `FIRST`, `LAST`, `MIN`, and `MAX` on ordered secondary shapes.
    false
}

// Route-owned load streaming gate.
// Load execution remains streaming when canonical streaming-safe shapes
// apply or when route enabled index-range limit pushdown.
pub(in crate::db::executor) const fn load_streaming_allowed(
    capabilities: RouteCapabilities,
    index_range_limit_enabled: bool,
) -> bool {
    capabilities
        .load_order_route_contract()
        .allows_streaming_load()
        || index_range_limit_enabled
}

pub(in crate::db::executor::planning::route) fn derive_load_route_direction(
    plan: &AccessPlannedQuery,
) -> Direction {
    ExecutionOrderContract::from_plan(
        plan.grouped_plan().is_some(),
        plan.scalar_plan().order.as_ref(),
    )
    .primary_scan_direction()
}

pub(in crate::db::executor::planning::route) fn derive_aggregate_route_direction(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
) -> Direction {
    if aggregate.target_field().is_some() {
        return aggregate
            .kind()
            .extrema_direction()
            .unwrap_or_else(|| derive_load_route_direction(plan));
    }

    derive_load_route_direction(plan)
}
