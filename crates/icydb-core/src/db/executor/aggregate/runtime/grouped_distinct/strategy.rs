//! Module: db::executor::aggregate::runtime::grouped_distinct::strategy
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    executor::aggregate::runtime::grouped_distinct::GlobalDistinctFieldAggregateKind,
    query::plan::GroupedDistinctExecutionStrategy,
};

///
/// GlobalDistinctFieldExecutionSpec
///
/// Data-only execution spec for grouped global DISTINCT field reducers.
/// This spec is resolved from planner-owned grouped DISTINCT strategy and does
/// not execute any runtime behavior.
///

pub(in crate::db::executor) struct GlobalDistinctFieldExecutionSpec<'a> {
    pub(in crate::db::executor) target_field: &'a str,
    pub(in crate::db::executor) aggregate_kind: GlobalDistinctFieldAggregateKind,
}

// Resolve one grouped DISTINCT strategy into one optional global field
// execution spec. This helper is data-only and does not execute any fold path.
pub(in crate::db::executor) const fn global_distinct_field_execution_spec(
    strategy: &GroupedDistinctExecutionStrategy,
) -> Option<GlobalDistinctFieldExecutionSpec<'_>> {
    match strategy {
        GroupedDistinctExecutionStrategy::None => None,
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Count,
            })
        }
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Sum,
            })
        }
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg { target_field } => {
            Some(GlobalDistinctFieldExecutionSpec {
                target_field: target_field.as_str(),
                aggregate_kind: GlobalDistinctFieldAggregateKind::Avg,
            })
        }
    }
}
