//! Module: db::executor::route::planner::feasibility::grouped_strategy
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::feasibility::grouped_strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::route::GroupedExecutionStrategy,
    query::plan::{AccessPlannedQuery, GroupedPlanStrategy},
};

///
/// GroupedOrderedEligibility
///
/// Executor-owned grouped ordered-strategy eligibility matrix.
/// This matrix revalidates planner ordered-group strategies against runtime capability
/// constraints before strategy projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderedEligibility {
    ordered_strategy_selected: bool,
    direction_compatible: bool,
    ordered_streaming_safe: bool,
}

impl GroupedOrderedEligibility {
    const fn is_eligible(self) -> bool {
        self.ordered_strategy_selected && self.direction_compatible && self.ordered_streaming_safe
    }
}

// Derive one grouped ordered-strategy eligibility matrix snapshot.
const fn derive_grouped_ordered_eligibility(
    _plan: &AccessPlannedQuery,
    plan_strategy: GroupedPlanStrategy,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    ordered_streaming_safe: bool,
) -> GroupedOrderedEligibility {
    GroupedOrderedEligibility {
        ordered_strategy_selected: plan_strategy.streaming_admitted(),
        direction_compatible: !matches!(direction, Direction::Desc)
            || desc_physical_reverse_supported,
        ordered_streaming_safe,
    }
}

// Resolve one route-level grouped strategy from one revalidated eligibility matrix.
const fn grouped_execution_strategy_for_plan_strategy(
    grouped_ordered_eligibility: GroupedOrderedEligibility,
) -> GroupedExecutionStrategy {
    if grouped_ordered_eligibility.is_eligible() {
        GroupedExecutionStrategy::OrderedMaterialized
    } else {
        GroupedExecutionStrategy::HashMaterialized
    }
}

#[must_use]
pub(super) const fn grouped_execution_strategy_for_runtime(
    plan: &AccessPlannedQuery,
    plan_strategy: GroupedPlanStrategy,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    ordered_streaming_safe: bool,
) -> GroupedExecutionStrategy {
    let grouped_ordered_eligibility = derive_grouped_ordered_eligibility(
        plan,
        plan_strategy,
        direction,
        desc_physical_reverse_supported,
        ordered_streaming_safe,
    );

    grouped_execution_strategy_for_plan_strategy(grouped_ordered_eligibility)
}
