//! Module: db::executor::load::grouped_distinct::paging
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_distinct::paging.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{GroupedRow, executor::load::LoadExecutor},
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply grouped pagination semantics to the singleton global grouped row.
    pub(in crate::db::executor::load) fn page_global_distinct_grouped_row(
        row: GroupedRow,
        initial_offset_for_page: usize,
        limit: Option<usize>,
    ) -> Vec<GroupedRow> {
        page_global_distinct_grouped_row_for_window(row, initial_offset_for_page, limit)
    }
}

// Apply grouped pagination semantics to one singleton global DISTINCT grouped
// row using routed grouped pagination window primitives.
fn page_global_distinct_grouped_row_for_window(
    row: GroupedRow,
    initial_offset_for_page: usize,
    limit: Option<usize>,
) -> Vec<GroupedRow> {
    if initial_offset_for_page > 0 || limit == Some(0) {
        return Vec::new();
    }

    vec![row]
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn global_distinct_grouped_row_paging_offset_consumes_singleton_row() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

        let paged = page_global_distinct_grouped_row_for_window(row, 1, Some(1));

        assert!(
            paged.is_empty(),
            "grouped singleton rows must be skipped when grouped window offset is non-zero",
        );
    }

    #[test]
    fn global_distinct_grouped_row_paging_zero_limit_consumes_singleton_row() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

        let paged = page_global_distinct_grouped_row_for_window(row, 0, Some(0));

        assert!(
            paged.is_empty(),
            "grouped singleton rows must be skipped when grouped window limit is zero",
        );
    }

    #[test]
    fn global_distinct_grouped_row_paging_emits_singleton_without_offset_or_zero_limit() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);
        let row_unbounded = row.clone();

        let bounded = page_global_distinct_grouped_row_for_window(row, 0, Some(5));
        let unbounded = page_global_distinct_grouped_row_for_window(row_unbounded, 0, None);

        assert_eq!(
            bounded.len(),
            1,
            "grouped singleton rows must be emitted when grouped window keeps at least one row",
        );
        assert_eq!(
            unbounded.len(),
            1,
            "grouped singleton rows must be emitted for unbounded grouped windows",
        );
    }
}
