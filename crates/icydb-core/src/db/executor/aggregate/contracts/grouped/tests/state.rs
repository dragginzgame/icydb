use super::*;

#[test]
fn grouped_aggregate_state_reuses_per_group_state_and_counts_rows() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    grouped
        .apply_borrowed(
            &text_group_key("alpha"),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first grouped row should apply");
    grouped
        .apply_borrowed(
            &text_group_key("alpha"),
            &data_key(2),
            &mut execution_context,
        )
        .expect("second grouped row for same group should apply");
    grouped
        .apply_borrowed(
            &text_group_key("beta"),
            &data_key(3),
            &mut execution_context,
        )
        .expect("third grouped row for second group should apply");

    assert_eq!(execution_context.budget().groups(), 2);
    assert_eq!(execution_context.budget().aggregate_states(), 2);
    assert!(
        execution_context.budget().estimated_bytes() > 0,
        "grouped budget should account for inserted group state bytes",
    );

    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(finalized.len(), 2, "two groups should finalize");
    assert_eq!(
        count_rows(finalized.as_slice()),
        vec![
            (Value::Text("alpha".to_string()), 2),
            (Value::Text("beta".to_string()), 1),
        ],
        "grouped count finalization should preserve per-group row counts",
    );
}

#[test]
fn grouped_aggregate_state_borrowed_row_probe_reuses_existing_group_without_owned_key() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);
    let group_fields = vec![FieldSlot::from_parts_for_test(0, "group")];
    let alpha_row = RowView::new(vec![Some(Value::Text("alpha".to_string()))]);
    let alpha_hash = crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
        "alpha".to_string(),
    )])
    .expect("alpha key")
    .hash();

    let mut inserted_group_key = None;
    grouped
        .apply_with_borrowed_group_probe(
            &data_key(1),
            &alpha_row,
            &group_fields,
            Some(alpha_hash),
            &mut inserted_group_key,
            &mut execution_context,
        )
        .expect("borrowed-probe insert should apply");
    assert!(
        inserted_group_key.is_some(),
        "new groups still materialize one owned canonical key on insert",
    );

    let mut reused_group_key = None;
    grouped
        .apply_with_borrowed_group_probe(
            &data_key(2),
            &alpha_row,
            &group_fields,
            Some(alpha_hash),
            &mut reused_group_key,
            &mut execution_context,
        )
        .expect("borrowed-probe reuse should apply");
    assert!(
        reused_group_key.is_none(),
        "existing-group borrowed probe should not materialize an owned key",
    );

    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(
        count_rows(finalized.as_slice()),
        vec![(Value::List(vec![Value::Text("alpha".to_string())]), 2)],
        "borrowed grouped probe should still preserve grouped count outputs",
    );
}

#[test]
fn grouped_aggregate_state_borrowed_row_probe_handles_hash_collisions() {
    with_test_hash_override([0xCD; 16], || {
        let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
        let mut grouped =
            execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);
        let group_fields = vec![FieldSlot::from_parts_for_test(0, "group")];
        let alpha_row = RowView::new(vec![Some(Value::Text("alpha".to_string()))]);
        let beta_row = RowView::new(vec![Some(Value::Text("beta".to_string()))]);
        let colliding_hash =
            crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                "alpha".to_string(),
            )])
            .expect("colliding hash")
            .hash();

        let mut alpha_group_key = None;
        grouped
            .apply_with_borrowed_group_probe(
                &data_key(1),
                &alpha_row,
                &group_fields,
                Some(colliding_hash),
                &mut alpha_group_key,
                &mut execution_context,
            )
            .expect("alpha insert should apply");
        let mut beta_group_key = None;
        grouped
            .apply_with_borrowed_group_probe(
                &data_key(2),
                &beta_row,
                &group_fields,
                Some(colliding_hash),
                &mut beta_group_key,
                &mut execution_context,
            )
            .expect("beta insert should apply");

        let mut reused_group_key = None;
        grouped
            .apply_with_borrowed_group_probe(
                &data_key(3),
                &alpha_row,
                &group_fields,
                Some(colliding_hash),
                &mut reused_group_key,
                &mut execution_context,
            )
            .expect("alpha reuse should apply");
        assert!(
            reused_group_key.is_none(),
            "hash-collision borrowed lookup should still find the matching canonical group without materializing a new key",
        );

        let finalized = into_value_pairs(grouped.finalize());
        assert_eq!(
            count_rows(finalized.as_slice()),
            vec![
                (Value::List(vec![Value::Text("alpha".to_string())]), 2),
                (Value::List(vec![Value::Text("beta".to_string())]), 1),
            ],
            "same-hash grouped rows must remain distinct under canonical borrowed-group equality",
        );
    });
}
