use super::*;

#[test]
fn grouped_aggregate_state_finalization_is_deterministic_under_hash_collisions() {
    with_test_hash_override([0xCD; 16], || {
        let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
        let mut grouped =
            execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

        // Intentionally insert in reverse lexical order under one forced
        // hash bucket; finalize must still emit canonical key order.
        grouped
            .apply_borrowed(
                &text_group_key("gamma"),
                &data_key(1),
                &mut execution_context,
            )
            .expect("gamma grouped row should apply");
        grouped
            .apply_borrowed(
                &text_group_key("alpha"),
                &data_key(2),
                &mut execution_context,
            )
            .expect("alpha grouped row should apply");
        grouped
            .apply_borrowed(
                &text_group_key("beta"),
                &data_key(3),
                &mut execution_context,
            )
            .expect("beta grouped row should apply");

        let finalized = into_value_pairs(grouped.finalize());
        assert_eq!(
            count_rows(finalized.as_slice()),
            vec![
                (Value::Text("alpha".to_string()), 1),
                (Value::Text("beta".to_string()), 1),
                (Value::Text("gamma".to_string()), 1),
            ],
            "grouped finalization should remain deterministic across collision buckets",
        );
    });
}

#[test]
fn grouped_aggregate_state_finalization_is_stable_across_insertion_order_matrix() {
    let insertion_orders = [
        vec![0, 1, 2, 3, 4],
        vec![4, 3, 2, 1, 0],
        vec![1, 3, 0, 4, 2],
        vec![2, 0, 4, 1, 3],
    ];
    let expected = grouped_count_rows_for_order(&[0, 1, 2, 3, 4]);

    for order in insertion_orders {
        assert_eq!(
            grouped_count_rows_for_order(order.as_slice()),
            expected,
            "grouped finalization must be invariant to insertion order permutations",
        );
    }
}

#[test]
fn grouped_aggregate_state_finalization_is_stable_across_collision_order_matrix() {
    with_test_hash_override([0xAB; 16], || {
        let expected = vec![
            (Value::Text("alpha".to_string()), 2),
            (Value::Text("beta".to_string()), 2),
            (Value::Text("gamma".to_string()), 1),
        ];
        let insertion_orders = [
            vec![0, 1, 2, 3, 4],
            vec![4, 3, 2, 1, 0],
            vec![1, 3, 0, 4, 2],
            vec![2, 0, 4, 1, 3],
        ];

        for order in insertion_orders {
            assert_eq!(
                grouped_count_rows_for_order(order.as_slice()),
                expected,
                "grouped finalization must stay stable under forced hash collisions and insertion-order permutations",
            );
        }
    });
}
