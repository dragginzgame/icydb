//! Module: executor::aggregate::runtime::grouped_distinct
//! Responsibility: grouped global DISTINCT field-target runtime handling.
//! Does not own: grouped planning policy or generic grouped fold mechanics.
//! Boundary: grouped DISTINCT special-case helpers used by grouped read execution.

mod aggregate;

use crate::db::{
    GroupedRow,
    query::plan::{AggregateKind, FieldSlot, GroupedDistinctExecutionStrategy},
};

///
/// TESTS
///

#[cfg(test)]
mod tests;

pub(in crate::db::executor) use aggregate::execute_global_distinct_field_aggregate;

///
/// global_distinct_field_target_and_kind
///
/// Resolve the planner-owned grouped DISTINCT strategy into the canonical
/// target field and aggregate kind consumed by the dedicated global
/// field-target runtime path.
///
pub(in crate::db::executor) fn global_distinct_field_target_and_kind(
    strategy: &GroupedDistinctExecutionStrategy,
) -> Option<(&FieldSlot, AggregateKind)> {
    let target_field = strategy.global_distinct_target_slot()?;
    let aggregate_kind = strategy.global_distinct_aggregate_kind()?;

    Some((target_field, aggregate_kind))
}

// Apply grouped pagination semantics to one singleton global DISTINCT grouped
// row using routed grouped pagination window primitives.
pub(in crate::db::executor) fn page_global_distinct_grouped_row(
    row: GroupedRow,
    initial_offset_for_page: usize,
    limit: Option<usize>,
) -> Vec<GroupedRow> {
    if initial_offset_for_page > 0 || limit == Some(0) {
        return Vec::new();
    }

    vec![row]
}
