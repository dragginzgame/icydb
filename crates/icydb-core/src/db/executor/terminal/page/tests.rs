use super::*;
use crate::db::{
    cursor::{CursorBoundary, CursorBoundarySlot},
    executor::terminal::page::{
        metrics::{
            record_direct_data_row_path_hit, record_direct_filtered_data_row_path_hit,
            record_kernel_data_row_path_hit, record_kernel_full_row_retained_path_hit,
            record_kernel_slots_only_path_hit,
        },
        post_access::{apply_load_cursor_and_pagination_window, compact_kernel_rows_in_place},
    },
    query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderField, ResolvedOrderValueSource},
};

fn kernel_row_u64(value: u64) -> KernelRow {
    KernelRow::new_slot_only(RetainedSlotRow::new(1, vec![(0, Value::Nat(value))]))
}

fn direct_field_order(slot: usize) -> ResolvedOrder {
    ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::direct_field(slot),
        OrderDirection::Asc,
    )])
}

#[test]
fn retained_slot_row_slot_ref_and_take_slot_use_indexed_lookup() {
    let mut row = RetainedSlotRow::new(
        8,
        vec![
            (1, Value::Text("alpha".to_string())),
            (5, Value::Nat(7)),
            (3, Value::Bool(true)),
        ],
    );

    assert_eq!(row.slot_ref(5), Some(&Value::Nat(7)));
    assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
    assert_eq!(row.slot_ref(1), None);
    assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    assert_eq!(row.take_slot(5), Some(Value::Nat(7)));
    assert_eq!(row.slot_ref(5), None);
    assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
}

#[test]
fn retained_slot_row_sparse_constructor_preserves_dense_overwrite_semantics() {
    let row = RetainedSlotRow::new(
        4,
        vec![
            (3, Value::Bool(false)),
            (1, Value::Text("first".to_string())),
            (7, Value::Nat(99)),
            (1, Value::Text("last".to_string())),
        ],
    );

    assert_eq!(
        row.into_dense_slots(),
        vec![
            None,
            Some(Value::Text("last".to_string())),
            None,
            Some(Value::Bool(false)),
        ]
    );
}

#[test]
fn retained_slot_row_indexed_layout_uses_shared_slot_lookup() {
    let layout = RetainedSlotLayout::compile(8, vec![1, 3, 5]);
    let mut row = RetainedSlotRow::from_indexed_values(
        &layout,
        vec![
            Some(Value::Text("alpha".to_string())),
            Some(Value::Bool(true)),
            Some(Value::Nat(7)),
        ],
    );

    assert_eq!(row.slot_ref(5), Some(&Value::Nat(7)));
    assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
    assert_eq!(row.slot_ref(1), None);
    assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    assert_eq!(row.into_dense_slots()[5], Some(Value::Nat(7)));
}

#[test]
fn residual_filter_scan_mode_fails_closed_by_row_capability() {
    assert_eq!(
        ResidualFilterScanMode::from_plan_and_layout(false, None, None),
        ResidualFilterScanMode::Absent
    );
    assert_eq!(
        ResidualFilterScanMode::from_plan_and_layout(true, None, None),
        ResidualFilterScanMode::DeferredPostAccess
    );
    assert_eq!(
        ResidualFilterScanMode::from_plan_and_layout(
            true,
            Some(&RetainedSlotLayout::compile(2, vec![0])),
            None,
        ),
        ResidualFilterScanMode::AppliedDuringScan
    );
}

#[test]
fn scalar_materialization_lane_metrics_capture_direct_and_kernel_paths() {
    let ((), metrics) = with_scalar_materialization_lane_metrics(|| {
        record_direct_data_row_path_hit();
        record_direct_filtered_data_row_path_hit();
        record_kernel_data_row_path_hit();
        record_kernel_full_row_retained_path_hit();
        record_kernel_slots_only_path_hit();
    });

    assert_eq!(
        metrics.direct_data_row_path_hits, 1,
        "direct data-row lane should increment once",
    );
    assert_eq!(
        metrics.direct_filtered_data_row_path_hits, 1,
        "direct filtered data-row lane should increment once",
    );
    assert_eq!(
        metrics.kernel_data_row_path_hits, 1,
        "kernel data-row lane should increment once",
    );
    assert_eq!(
        metrics.kernel_full_row_retained_path_hits, 1,
        "kernel retained full-row lane should increment once",
    );
    assert_eq!(
        metrics.kernel_slots_only_path_hits, 1,
        "kernel slot-only lane should increment once",
    );
}

#[test]
fn load_cursor_and_pagination_window_compacts_in_one_pass() {
    let resolved_order = direct_field_order(0);
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Nat(2))],
    };
    let mut rows = vec![
        kernel_row_u64(1),
        kernel_row_u64(2),
        kernel_row_u64(3),
        kernel_row_u64(4),
        kernel_row_u64(5),
    ];

    let rows_after_cursor = apply_load_cursor_and_pagination_window(
        &mut rows,
        Some((&resolved_order, &boundary)),
        1,
        Some(2),
    );

    assert_eq!(rows_after_cursor, 3);
    assert_eq!(
        rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
        vec![Some(Value::Nat(4)), Some(Value::Nat(5))]
    );
}

#[test]
fn load_pagination_window_without_cursor_skips_offset_then_limits() {
    let mut rows = vec![
        kernel_row_u64(10),
        kernel_row_u64(20),
        kernel_row_u64(30),
        kernel_row_u64(40),
    ];

    let rows_after_cursor = apply_load_cursor_and_pagination_window(&mut rows, None, 2, Some(1));

    assert_eq!(rows_after_cursor, 4);
    assert_eq!(
        rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
        vec![Some(Value::Nat(30))]
    );
}

#[test]
fn compact_kernel_rows_in_place_preserves_kept_order() {
    let mut rows = vec![
        kernel_row_u64(1),
        kernel_row_u64(2),
        kernel_row_u64(3),
        kernel_row_u64(4),
    ];

    let kept = compact_kernel_rows_in_place(
        &mut rows,
        |row| matches!(row.slot(0), Some(Value::Nat(value)) if value % 2 == 0),
    );

    assert_eq!(kept, 2);
    assert_eq!(
        rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
        vec![Some(Value::Nat(2)), Some(Value::Nat(4))]
    );
}
