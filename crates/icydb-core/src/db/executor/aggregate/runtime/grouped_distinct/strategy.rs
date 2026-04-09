//! Module: db::executor::aggregate::runtime::grouped_distinct::strategy
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::plan::{AggregateKind, GroupedDistinctExecutionStrategy};

///
/// global_distinct_field_target_and_kind
///
/// Resolve the planner-owned grouped DISTINCT strategy into the canonical
/// target field and aggregate kind consumed by the dedicated global
/// field-target runtime path.
///
pub(in crate::db::executor) fn global_distinct_field_target_and_kind(
    strategy: &GroupedDistinctExecutionStrategy,
) -> Option<(&str, AggregateKind)> {
    let target_field = strategy.global_distinct_target_field()?;
    let aggregate_kind = strategy.global_distinct_aggregate_kind()?;

    Some((target_field, aggregate_kind))
}
