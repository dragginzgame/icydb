//! Module: db::executor::route::planner::feasibility::grouped_strategy
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::feasibility::grouped_strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction, executor::route::GroupedExecutionStrategy,
    query::plan::GroupedPlanStrategy,
};

// Resolve one route-level grouped strategy from planner-owned grouped admission
// plus the remaining route capability gates.
const fn grouped_execution_strategy_for_plan_strategy(
    plan_strategy: GroupedPlanStrategy,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    ordered_streaming_safe: bool,
) -> GroupedExecutionStrategy {
    let direction_compatible =
        !matches!(direction, Direction::Desc) || desc_physical_reverse_supported;
    let ordered_route_eligible =
        plan_strategy.ordered_group_admitted() && direction_compatible && ordered_streaming_safe;

    if ordered_route_eligible {
        GroupedExecutionStrategy::OrderedMaterialized
    } else {
        GroupedExecutionStrategy::HashMaterialized
    }
}

#[must_use]
pub(super) const fn grouped_execution_strategy_for_runtime(
    plan_strategy: GroupedPlanStrategy,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    ordered_streaming_safe: bool,
) -> GroupedExecutionStrategy {
    grouped_execution_strategy_for_plan_strategy(
        plan_strategy,
        direction,
        desc_physical_reverse_supported,
        ordered_streaming_safe,
    )
}
