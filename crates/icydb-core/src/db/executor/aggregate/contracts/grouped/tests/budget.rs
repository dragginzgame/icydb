use super::*;

#[test]
fn grouped_aggregate_state_distinct_deduplicates_repeated_data_keys() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped_distinct = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Count,
            Direction::Asc,
            true,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped COUNT(DISTINCT field) test fixture should construct admitted state");
    let mut grouped_plain =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    let group = text_group_key("alpha");
    let duplicate_key = data_key(42);
    let duplicate_value = RowView::from_single_value(0, Value::Nat(7));
    grouped_distinct
        .apply_borrowed_with_row_view(
            &group,
            &duplicate_key,
            Some(&duplicate_value),
            &mut execution_context,
        )
        .expect("distinct grouped row should apply");
    grouped_distinct
        .apply_borrowed_with_row_view(
            &group,
            &duplicate_key,
            Some(&duplicate_value),
            &mut execution_context,
        )
        .expect("duplicate distinct grouped row should apply as no-op");

    grouped_plain
        .apply_borrowed(&group, &duplicate_key, &mut execution_context)
        .expect("plain grouped row should apply");
    grouped_plain
        .apply_borrowed(&group, &duplicate_key, &mut execution_context)
        .expect("plain grouped duplicate row should increment count");

    let distinct_rows = into_value_pairs(grouped_distinct.finalize());
    let plain_rows = into_value_pairs(grouped_plain.finalize());
    assert_eq!(
        count_rows(distinct_rows.as_slice()),
        vec![(Value::Text("alpha".to_string()), 1)],
        "distinct grouped count should deduplicate repeated data keys",
    );
    assert_eq!(
        count_rows(plain_rows.as_slice()),
        vec![(Value::Text("alpha".to_string()), 2)],
        "non-distinct grouped count should keep repeated data-key contributions",
    );
}

#[test]
fn grouped_aggregate_state_enforces_distinct_values_per_group_limit() {
    let mut execution_context = ExecutionContext::new(
        ExecutionConfig::with_hard_limits_and_distinct(u64::MAX, u64::MAX, 1, u64::MAX),
    );
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Count,
            Direction::Asc,
            true,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped COUNT(DISTINCT field) test fixture should construct admitted state");
    let first_value = RowView::from_single_value(0, Value::Nat(1));
    let second_value = RowView::from_single_value(0, Value::Nat(2));

    grouped
        .apply_borrowed_with_row_view(
            &text_group_key("alpha"),
            &data_key(1),
            Some(&first_value),
            &mut execution_context,
        )
        .expect("first grouped distinct value should fit per-group budget");
    let err = grouped
        .apply_borrowed_with_row_view(
            &text_group_key("alpha"),
            &data_key(2),
            Some(&second_value),
            &mut execution_context,
        )
        .expect_err("second unique grouped distinct value should exceed per-group budget");

    assert!(matches!(
        err,
        GroupError::DistinctBudgetExceeded {
            resource: "distinct_values_per_group",
            attempted: 2,
            limit: 1,
        }
    ));
}

#[test]
fn grouped_aggregate_state_enforces_distinct_values_total_limit() {
    let mut execution_context = ExecutionContext::new(
        ExecutionConfig::with_hard_limits_and_distinct(u64::MAX, u64::MAX, u64::MAX, 1),
    );
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Count,
            Direction::Asc,
            true,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped COUNT(DISTINCT field) test fixture should construct admitted state");
    let first_value = RowView::from_single_value(0, Value::Nat(1));
    let second_value = RowView::from_single_value(0, Value::Nat(2));

    grouped
        .apply_borrowed_with_row_view(
            &text_group_key("alpha"),
            &data_key(1),
            Some(&first_value),
            &mut execution_context,
        )
        .expect("first grouped distinct value should fit total budget");
    let err = grouped
        .apply_borrowed_with_row_view(
            &text_group_key("beta"),
            &data_key(2),
            Some(&second_value),
            &mut execution_context,
        )
        .expect_err("second grouped distinct value should exceed total distinct budget");

    assert!(matches!(
        err,
        GroupError::DistinctBudgetExceeded {
            resource: "distinct_values_total",
            attempted: 2,
            limit: 1,
        }
    ));
}

#[test]
fn grouped_execution_budget_counters_remain_consistent_for_distinct_grouped_fold() {
    let mut execution_context = ExecutionContext::new(
        ExecutionConfig::with_hard_limits_and_distinct(16, 4096, 16, 16),
    );
    let mut grouped = execution_context
        .create_grouped_state_with_target(
            AggregateKind::Count,
            Direction::Asc,
            true,
            Some(FieldSlot::from_parts_for_test(0, "id")),
        )
        .expect("grouped COUNT(DISTINCT field) test fixture should construct admitted state");

    for (group, id, value) in [
        ("alpha", 1_u64, 10_u64),
        ("alpha", 2_u64, 10_u64),
        ("alpha", 3_u64, 20_u64),
        ("beta", 4_u64, 30_u64),
        ("beta", 5_u64, 30_u64),
    ] {
        let row = RowView::from_single_value(0, Value::Nat(value));
        grouped
            .apply_borrowed_with_row_view(
                &text_group_key(group),
                &data_key(id),
                Some(&row),
                &mut execution_context,
            )
            .expect("grouped budget-consistency fixture row should apply");
    }

    assert_eq!(
        execution_context.budget().groups(),
        2,
        "group counter should track unique canonical groups only",
    );
    assert_eq!(
        execution_context.budget().aggregate_states(),
        2,
        "aggregate state counter should track per-group grouped slots",
    );
    assert_eq!(
        execution_context.budget().distinct_values(),
        3,
        "distinct counter should track unique grouped DISTINCT inserts only",
    );
    assert!(
        execution_context.budget().estimated_bytes() > 0,
        "estimated-bytes counter should account for grouped state allocations",
    );
    assert!(
        execution_context.budget().aggregate_states() >= execution_context.budget().groups(),
        "grouped aggregate-state counter must remain >= groups counter",
    );
}

#[test]
fn grouped_aggregate_state_enforces_max_groups_hard_limit() {
    let mut execution_context =
        ExecutionContext::new(ExecutionConfig::with_hard_limits(2, u64::MAX));
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    grouped
        .apply_borrowed(&text_group_key("a"), &data_key(1), &mut execution_context)
        .expect("first group should fit budget");
    grouped
        .apply_borrowed(&text_group_key("b"), &data_key(2), &mut execution_context)
        .expect("second group should fit budget");
    let err = grouped
        .apply_borrowed(&text_group_key("c"), &data_key(3), &mut execution_context)
        .expect_err("third group should exceed max_groups hard limit");

    assert!(matches!(
        err,
        GroupError::MemoryLimitExceeded {
            resource: "groups",
            attempted: 3,
            limit: 2,
        }
    ));
}

#[test]
fn grouped_aggregate_state_enforces_max_estimated_bytes_hard_limit() {
    let mut execution_context =
        ExecutionContext::new(ExecutionConfig::with_hard_limits(u64::MAX, 1));
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    let err = grouped
        .apply_borrowed(
            &text_group_key("only"),
            &data_key(1),
            &mut execution_context,
        )
        .expect_err("tiny byte budget should reject first group insertion");

    assert!(matches!(
        err,
        GroupError::MemoryLimitExceeded {
            resource: "estimated_bytes",
            attempted: _,
            limit: 1,
        }
    ));
}

#[test]
fn grouped_aggregate_state_counts_max_groups_once_per_canonical_group_across_states() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::with_hard_limits(1, 2048));
    let mut grouped_count =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);
    let mut grouped_exists =
        execution_context.create_grouped_state(AggregateKind::Exists, Direction::Asc, false);

    grouped_count
        .apply_borrowed(&text_group_key("a"), &data_key(1), &mut execution_context)
        .expect("first grouped state should accept first canonical group");
    grouped_exists
        .apply_borrowed(&text_group_key("a"), &data_key(1), &mut execution_context)
        .expect("second grouped state should reuse the same canonical group slot");

    assert_eq!(
        execution_context.budget().groups(),
        1,
        "max_groups accounting must be keyed by canonical group identity across all grouped states",
    );
    assert_eq!(
        execution_context.budget().aggregate_states(),
        2,
        "aggregate_state accounting remains per grouped terminal state",
    );

    let err = grouped_count
        .apply_borrowed(&text_group_key("b"), &data_key(2), &mut execution_context)
        .expect_err("second canonical group should exceed max_groups hard limit");

    assert!(matches!(
        err,
        GroupError::MemoryLimitExceeded {
            resource: "groups",
            attempted: 2,
            limit: 1,
        }
    ));
}

#[test]
fn grouped_aggregate_state_budget_violation_keeps_existing_finalization_intact() {
    let mut execution_context =
        ExecutionContext::new(ExecutionConfig::with_hard_limits(1, u64::MAX));
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    grouped
        .apply_borrowed(&text_group_key("a"), &data_key(1), &mut execution_context)
        .expect("first group should fit budget");
    let err = grouped
        .apply_borrowed(&text_group_key("b"), &data_key(2), &mut execution_context)
        .expect_err("second group should exceed max_groups and fail atomically");

    assert!(matches!(
        err,
        GroupError::MemoryLimitExceeded {
            resource: "groups",
            attempted: 2,
            limit: 1,
        }
    ));
    assert_eq!(
        execution_context.budget().groups(),
        1,
        "failed grouped insertion must not leak partial group-count state",
    );
    let finalized = into_value_pairs(grouped.finalize());
    assert_eq!(
        count_rows(finalized.as_slice()),
        vec![(Value::Text("a".to_string()), 1)],
        "budget-limit errors must preserve previously committed grouped outputs",
    );
}
