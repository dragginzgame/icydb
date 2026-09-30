use super::*;
use crate::db::{
    cursor::{CursorBoundary, CursorBoundarySlot},
    executor::terminal::page::post_access::apply_load_cursor_and_pagination_window,
    query::plan::{OrderDirection, ResolvedOrder, ResolvedOrderField, ResolvedOrderValueSource},
};

fn kernel_row_u64(value: u64) -> KernelRow {
    let layout = RetainedSlotLayout::compile(1, vec![0]);
    KernelRow::new_slot_only(RetainedSlotRow::from_indexed_values(
        &layout,
        vec![Some(Value::Nat64(value))],
    ))
}

fn direct_field_order(slot: usize) -> ResolvedOrder {
    ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::direct_field(slot),
        OrderDirection::Asc,
    )])
}

#[test]
fn retained_slot_layout_preserves_only_nondefault_decode_modes() {
    use RetainedSlotValueMode::{Normal, ScalarOctetLength};

    for modes in [vec![], vec![Normal, Normal]] {
        let layout = RetainedSlotLayout::compile_with_value_modes(4, vec![3, 1], modes);
        assert!(!layout.has_value_mode_overrides());
        assert_eq!(layout.override_value_modes(), None);
        assert_eq!(layout.required_slots(), &[3, 1]);
        assert_eq!(layout.value_index_for_slot(3), Some(0));
        assert_eq!(layout.value_index_for_slot(1), Some(1));
        assert_eq!(layout.value_index_for_slot(0), None);
    }

    let modes = vec![Normal, ScalarOctetLength];
    let layout = RetainedSlotLayout::compile_with_value_modes(4, vec![3, 1], modes.clone());
    assert!(layout.has_value_mode_overrides());
    assert_eq!(layout.override_value_modes(), Some(modes.as_slice()));
    assert_eq!(layout.value_index_for_slot(3), Some(0));
    assert_eq!(layout.value_index_for_slot(1), Some(1));

    let empty = RetainedSlotLayout::compile(0, vec![]);
    assert!(!empty.has_value_mode_overrides());
    assert_eq!(empty.override_value_modes(), None);
}

#[test]
fn retained_slot_row_indexed_layout_uses_shared_slot_lookup() {
    let layout = RetainedSlotLayout::compile(8, vec![1, 3, 5]);
    let mut row = RetainedSlotRow::from_indexed_values(
        &layout,
        vec![
            Some(Value::Text("alpha".to_string())),
            Some(Value::Bool(true)),
            Some(Value::Nat64(7)),
        ],
    );

    assert_eq!(row.slot_ref(5), Some(&Value::Nat64(7)));
    assert_eq!(row.take_slot(1), Some(Value::Text("alpha".to_string())));
    assert_eq!(row.slot_ref(1), None);
    assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    assert_eq!(row.take_slot(5), Some(Value::Nat64(7)));
    assert_eq!(row.take_slot(5), None);
    assert_eq!(row.slot_ref(5), None);
    assert_eq!(row.slot_ref(3), Some(&Value::Bool(true)));
    assert_eq!(row.slot_ref(0), None);
    assert_eq!(row.slot_ref(8), None);
    assert_eq!(row.take_slot(8), None);
}

#[test]
fn load_cursor_and_pagination_window_compacts_in_one_pass() {
    let resolved_order = direct_field_order(0);
    let boundary = CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Nat64(2))],
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
    )
    .expect("valid cursor boundary should apply");

    assert_eq!(rows_after_cursor, 3);
    assert_eq!(
        rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
        vec![Some(Value::Nat64(4)), Some(Value::Nat64(5))]
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

    let rows_after_cursor = apply_load_cursor_and_pagination_window(&mut rows, None, 2, Some(1))
        .expect("pagination without cursor should apply");

    assert_eq!(rows_after_cursor, 4);
    assert_eq!(
        rows.into_iter().map(|row| row.slot(0)).collect::<Vec<_>>(),
        vec![Some(Value::Nat64(30))]
    );
}
