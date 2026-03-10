//! Module: db::executor::load::grouped_route::metrics
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_route::metrics.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::executor::{
        load::LoadExecutor, plan_metrics::GroupedPlanMetricsStrategy,
        route::GroupedExecutionStrategy,
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Map route-owned grouped strategy labels into grouped plan-metrics labels.
    pub(in crate::db::executor) const fn grouped_plan_metrics_strategy_for_execution_strategy(
        grouped_execution_strategy: GroupedExecutionStrategy,
    ) -> GroupedPlanMetricsStrategy {
        match grouped_execution_strategy {
            GroupedExecutionStrategy::HashMaterialized => {
                GroupedPlanMetricsStrategy::HashMaterialized
            }
            GroupedExecutionStrategy::OrderedMaterialized => {
                GroupedPlanMetricsStrategy::OrderedMaterialized
            }
        }
    }
}
