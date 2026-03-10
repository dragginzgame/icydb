//! Module: db::executor::route::planner::feasibility::grouped_strategy
//! Responsibility: module-local ownership and contracts for db::executor::route::planner::feasibility::grouped_strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::route::GroupedExecutionStrategy,
    query::plan::{AccessPlannedQuery, GroupedPlanStrategyHint},
};

///
/// GroupedOrderedEligibility
///
/// Executor-owned grouped ordered-strategy eligibility matrix.
/// This matrix revalidates planner ordered-group hints against runtime capability
/// constraints before strategy projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderedEligibility {
    ordered_hint: bool,
    direction_compatible: bool,
    stream_order_contract_safe: bool,
}

impl GroupedOrderedEligibility {
    const fn is_eligible(self) -> bool {
        self.ordered_hint && self.direction_compatible && self.stream_order_contract_safe
    }
}

// Derive one grouped ordered-strategy eligibility matrix snapshot.
const fn derive_grouped_ordered_eligibility<K>(
    _plan: &AccessPlannedQuery<K>,
    plan_hint: GroupedPlanStrategyHint,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    stream_order_contract_safe: bool,
) -> GroupedOrderedEligibility {
    GroupedOrderedEligibility {
        ordered_hint: matches!(plan_hint, GroupedPlanStrategyHint::OrderedGroup),
        direction_compatible: !matches!(direction, Direction::Desc)
            || desc_physical_reverse_supported,
        stream_order_contract_safe,
    }
}

// Resolve one route-level grouped strategy from one revalidated eligibility matrix.
const fn grouped_execution_strategy_for_plan_hint(
    grouped_ordered_eligibility: GroupedOrderedEligibility,
) -> GroupedExecutionStrategy {
    if grouped_ordered_eligibility.is_eligible() {
        GroupedExecutionStrategy::OrderedMaterialized
    } else {
        GroupedExecutionStrategy::HashMaterialized
    }
}

#[must_use]
pub(super) const fn grouped_execution_strategy_for_runtime<K>(
    plan: &AccessPlannedQuery<K>,
    plan_hint: GroupedPlanStrategyHint,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    stream_order_contract_safe: bool,
) -> GroupedExecutionStrategy {
    let grouped_ordered_eligibility = derive_grouped_ordered_eligibility(
        plan,
        plan_hint,
        direction,
        desc_physical_reverse_supported,
        stream_order_contract_safe,
    );

    grouped_execution_strategy_for_plan_hint(grouped_ordered_eligibility)
}

#[cfg(test)]
pub(in crate::db::executor) const fn grouped_ordered_runtime_revalidation_flag_count_guard() -> usize
{
    let _ = GroupedOrderedEligibility {
        ordered_hint: false,
        direction_compatible: false,
        stream_order_contract_safe: false,
    };

    3
}
