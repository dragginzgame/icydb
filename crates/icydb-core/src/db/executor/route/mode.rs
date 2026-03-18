//! Module: db::executor::route::mode
//! Responsibility: route-owned direction/window/continuation-mode derivation helpers.
//! Does not own: access-shape capability decisions or route execution-mode selection.
//! Boundary: pure derivation primitives consumed by route planning.

use crate::{
    db::{
        direction::Direction,
        executor::pipeline::contracts::LoadExecutor,
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, ExecutionOrderContract},
    },
    traits::{EntityKind, EntityValue},
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

    capabilities.stream_order_contract_safe
        || secondary_pushdown_eligible
        || index_range_limit_enabled
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

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) fn derive_load_route_direction(plan: &AccessPlannedQuery) -> Direction {
        ExecutionOrderContract::from_plan(
            plan.grouped_plan().is_some(),
            plan.scalar_plan().order.as_ref(),
        )
        .primary_scan_direction()
    }

    pub(super) fn derive_aggregate_route_direction(
        plan: &AccessPlannedQuery,
        aggregate: &AggregateExpr,
    ) -> Direction {
        // Aggregate direction authority flows from AggregateKind.
        // Field-target extrema derive from `AggregateKind::extrema_direction`;
        // all other cases inherit canonical load ordering direction.
        if aggregate.target_field().is_some() {
            return aggregate_extrema_direction(aggregate.kind())
                .unwrap_or_else(|| Self::derive_load_route_direction(plan));
        }

        Self::derive_load_route_direction(plan)
    }
}
