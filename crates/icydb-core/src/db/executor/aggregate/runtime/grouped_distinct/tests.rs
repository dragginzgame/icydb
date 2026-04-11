//! Module: db::executor::aggregate::runtime::grouped_distinct::tests
//! Covers grouped DISTINCT runtime behavior and paging invariants.
//! Does not own: production aggregate behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        GroupedRow,
        executor::aggregate::runtime::grouped_distinct::{
            global_distinct_field_target_and_kind, page_global_distinct_grouped_row,
        },
        query::plan::{AggregateKind, FieldSlot, GroupedDistinctExecutionStrategy},
    },
    value::Value,
};

#[test]
fn global_distinct_grouped_row_paging_offset_consumes_singleton_row() {
    let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

    let paged = page_global_distinct_grouped_row(row, 1, Some(1));

    assert!(
        paged.is_empty(),
        "grouped singleton rows must be skipped when grouped window offset is non-zero",
    );
}

#[test]
fn global_distinct_grouped_row_paging_zero_limit_consumes_singleton_row() {
    let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

    let paged = page_global_distinct_grouped_row(row, 0, Some(0));

    assert!(
        paged.is_empty(),
        "grouped singleton rows must be skipped when grouped window limit is zero",
    );
}

#[test]
fn global_distinct_grouped_row_paging_emits_singleton_without_offset_or_zero_limit() {
    let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);
    let row_unbounded = row.clone();

    let bounded = page_global_distinct_grouped_row(row, 0, Some(5));
    let unbounded = page_global_distinct_grouped_row(row_unbounded, 0, None);

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

#[test]
fn grouped_distinct_strategy_none_maps_to_no_global_field_spec() {
    let strategy = GroupedDistinctExecutionStrategy::None;

    assert!(
        global_distinct_field_target_and_kind(&strategy).is_none(),
        "grouped distinct None strategy must not resolve to a global field execution spec",
    );
}

#[test]
fn grouped_distinct_count_strategy_maps_to_count_field_spec() {
    let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
        target_field: "rank".to_string(),
        target_slot: FieldSlot::from_parts_for_test(1, "rank"),
    };
    let spec = global_distinct_field_target_and_kind(&strategy)
        .expect("grouped distinct COUNT strategy should resolve");

    assert_eq!(spec.0.field(), "rank");
    assert!(matches!(spec.1, AggregateKind::Count));
}

#[test]
fn grouped_distinct_sum_strategy_maps_to_sum_field_spec() {
    let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
        target_field: "score".to_string(),
        target_slot: FieldSlot::from_parts_for_test(2, "score"),
    };
    let spec = global_distinct_field_target_and_kind(&strategy)
        .expect("grouped distinct SUM strategy should resolve");

    assert_eq!(spec.0.field(), "score");
    assert!(matches!(spec.1, AggregateKind::Sum));
}

#[test]
fn grouped_distinct_avg_strategy_maps_to_avg_field_spec() {
    let strategy = GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg {
        target_field: "score".to_string(),
        target_slot: FieldSlot::from_parts_for_test(2, "score"),
    };
    let spec = global_distinct_field_target_and_kind(&strategy)
        .expect("grouped distinct AVG strategy should resolve");

    assert_eq!(spec.0.field(), "score");
    assert!(matches!(spec.1, AggregateKind::Avg));
}
