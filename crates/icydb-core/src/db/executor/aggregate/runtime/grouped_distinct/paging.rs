//! Module: db::executor::aggregate::runtime::grouped_distinct::paging
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::paging.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::GroupedRow;

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
