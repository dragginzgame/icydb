use super::*;

#[test]
fn grouped_count_field_skips_null_slot_values() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Count,
            Direction::Asc,
            false,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped COUNT(field) test fixture should construct admitted grouped state");
    let group = text_group_key("alpha");
    let non_null_row = RowView::new(vec![Some(Value::Nat(7))]);
    let null_row = RowView::new(vec![Some(Value::Null)]);

    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(1),
            Some(&non_null_row),
            &mut execution_context,
        )
        .expect("non-null grouped COUNT(field) row should apply");
    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(2),
            Some(&null_row),
            &mut execution_context,
        )
        .expect("null grouped COUNT(field) row should apply as no-op");

    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(
        count_rows(finalized.as_slice()),
        vec![(Value::Text("alpha".to_string()), 1)],
        "grouped COUNT(field) should skip null slot values while preserving per-group output",
    );
}

#[test]
fn grouped_sum_field_skips_null_slot_values_and_accumulates_numeric_rows() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Sum,
            Direction::Asc,
            false,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped SUM(field) test fixture should construct admitted grouped state");
    let group = text_group_key("alpha");
    let numeric_row = RowView::new(vec![Some(Value::Nat(7))]);
    let second_numeric_row = RowView::new(vec![Some(Value::Nat(9))]);
    let null_row = RowView::new(vec![Some(Value::Null)]);

    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(1),
            Some(&numeric_row),
            &mut execution_context,
        )
        .expect("first numeric grouped SUM(field) row should apply");
    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(2),
            Some(&null_row),
            &mut execution_context,
        )
        .expect("null grouped SUM(field) row should apply as no-op");
    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(3),
            Some(&second_numeric_row),
            &mut execution_context,
        )
        .expect("second numeric grouped SUM(field) row should apply");

    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(
        value_rows(finalized.as_slice()),
        vec![(
            Value::Text("alpha".to_string()),
            Value::Decimal(Decimal::from(16_u64)),
        )],
        "grouped SUM(field) should skip null slot values while accumulating numeric rows per group",
    );
}

#[test]
fn grouped_avg_field_skips_null_slot_values_and_accumulates_numeric_rows() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Avg,
            Direction::Asc,
            false,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped AVG(field) test fixture should construct admitted grouped state");
    let group = text_group_key("alpha");
    let numeric_row = RowView::new(vec![Some(Value::Nat(6))]);
    let second_numeric_row = RowView::new(vec![Some(Value::Nat(12))]);
    let null_row = RowView::new(vec![Some(Value::Null)]);

    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(1),
            Some(&numeric_row),
            &mut execution_context,
        )
        .expect("first numeric grouped AVG(field) row should apply");
    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(2),
            Some(&null_row),
            &mut execution_context,
        )
        .expect("null grouped AVG(field) row should apply as no-op");
    grouped
        .apply_borrowed_with_row_view(
            &group,
            &data_key(3),
            Some(&second_numeric_row),
            &mut execution_context,
        )
        .expect("second numeric grouped AVG(field) row should apply");

    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(
        value_rows(finalized.as_slice()),
        vec![(
            Value::Text("alpha".to_string()),
            Value::Decimal(Decimal::from(9_u64)),
        )],
        "grouped AVG(field) should skip null slot values while averaging numeric rows per group",
    );
}

#[test]
fn aggregate_expr_builders_preserve_kind_and_target_field() {
    let terminal = count();
    assert_eq!(terminal.kind(), AggregateKind::Count);
    assert_eq!(terminal.target_field(), None);

    let field_target = min_by("rank");
    assert_eq!(field_target.kind(), AggregateKind::Min);
    assert_eq!(field_target.target_field(), Some("rank"));
}

#[test]
fn grouped_id_terminals_finalize_to_structural_primary_key_values() {
    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Min, Direction::Asc, &[3, 7, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(3))],
        "grouped MIN should finalize to the primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Max, Direction::Desc, &[9, 7, 3], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(9))],
        "grouped MAX should finalize to the primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::First, Direction::Asc, &[7, 3, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(7))],
        "grouped FIRST should finalize to the first seen primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Last, Direction::Asc, &[7, 3, 9], false,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(9))],
        "grouped LAST should finalize to the last seen primary-key value with structural output",
    );
}

#[test]
fn grouped_min_max_key_path_early_break_matrix_is_directional() {
    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Min, Direction::Asc, &[3, 7, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(3))],
        "MIN over ascending input may stop after the first key",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Min, Direction::Desc, &[9, 7, 3], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(3))],
        "MIN over descending input must keep scanning instead of stopping early",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Max, Direction::Desc, &[9, 7, 3], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(9))],
        "MAX over descending input may stop after the first key",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Max, Direction::Asc, &[3, 7, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Nat(9))],
        "MAX over ascending input must keep scanning instead of stopping early",
    );
}
