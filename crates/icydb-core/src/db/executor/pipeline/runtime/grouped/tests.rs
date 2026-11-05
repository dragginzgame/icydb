use crate::{
    db::executor::{
        pipeline::runtime::RowView,
        terminal::{RetainedSlotLayout, RetainedSlotRow},
    },
    value::Value,
};

#[test]
fn retained_row_view_resolves_noncontiguous_slots() {
    let row_view = RowView::new(vec![
        None,
        Some(Value::Nat64(7)),
        None,
        None,
        Some(Value::Text("group".to_string())),
        None,
    ]);

    assert_eq!(row_view.slot_value_ref(1), Some(&Value::Nat64(7)));
    assert_eq!(
        row_view.slot_value_ref(4),
        Some(&Value::Text("group".to_string()))
    );
    assert_eq!(row_view.slot_value_ref(0), None);
    assert_eq!(row_view.slot_value_ref(5), None);
    assert_eq!(row_view.slot_value_ref(6), None);
    assert_eq!(
        row_view.into_required_slot_value(4).unwrap(),
        Value::Text("group".to_string())
    );
}

#[test]
fn single_slot_row_view_resolves_only_its_declared_slot() {
    let row_view = RowView::from_single_value(4, Value::Text("group".to_string()));

    assert_eq!(
        row_view.slot_value_ref(4),
        Some(&Value::Text("group".to_string()))
    );
    assert_eq!(row_view.slot_value_ref(1), None);
}

#[test]
fn retained_row_view_slot_reads_are_repeatable_borrows() {
    let layout = RetainedSlotLayout::compile(5, vec![1, 4]);
    let retained = RetainedSlotRow::from_indexed_values(
        &layout,
        vec![
            Some(Value::Nat64(7)),
            Some(Value::Text("group".to_string())),
        ],
    );
    let row_view = RowView::from_retained_slots(retained);

    assert_eq!(
        row_view.slot_value_ref(1),
        Some(&Value::Nat64(7)),
        "first retained slot read should borrow the decoded value",
    );
    assert_eq!(
        row_view.slot_value_ref(1),
        Some(&Value::Nat64(7)),
        "second retained slot read must see the same decoded value",
    );
    assert_eq!(
        row_view.slot_value_ref(4),
        Some(&Value::Text("group".to_string())),
        "reading another retained slot must not invalidate earlier slots",
    );
}
