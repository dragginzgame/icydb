//! Module: db::executor::aggregate::contracts::grouped::tests
//! Responsibility: grouped aggregate contract tests that validate state ownership, budgeting,
//! finalization order, and terminal structural outputs.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::aggregate::{
            FoldControl,
            contracts::{AggregateKind, ExecutionConfig, ExecutionContext, GroupError},
        },
        executor::group::CanonicalKey,
        executor::pipeline::contracts::RowView,
        query::{
            builder::aggregate::{count, min_by},
            plan::FieldSlot,
        },
    },
    model::field::FieldKind,
    testing,
    types::Decimal,
    value::{Value, with_test_hash_override},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};

type FinalizedGroupedRow = super::engine::GroupedAggregateOutput;

crate::test_canister! {
    ident = GroupedStateTestCanister,
    commit_memory_id = testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct GroupedStateTestEntity {
    id: u64,
}

crate::test_entity_schema! {
    ident = GroupedStateTestEntity,
    id = u64,
    id_field = id,
    entity_name = "GroupedStateTestEntity",
    entity_tag = crate::testing::GROUPED_STATE_TEST_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Uint)],
    indexes = [],
    store = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

fn text_group_key(value: &str) -> crate::db::executor::group::GroupKey {
    Value::Text(value.to_string())
        .canonical_key()
        .expect("group key canonicalization should succeed")
}

fn data_key(id: u64) -> DataKey {
    DataKey::try_new::<GroupedStateTestEntity>(id).expect("test data key should build")
}

fn into_value_pairs(rows: Vec<FinalizedGroupedRow>) -> Vec<(Value, Value)> {
    rows.into_iter()
        .map(FinalizedGroupedRow::into_value_pair)
        .collect()
}

fn count_rows(rows: &[(Value, Value)]) -> Vec<(Value, u32)> {
    rows.iter()
        .map(|(group_key, output)| {
            let Value::Uint(count) = output else {
                panic!("grouped count-state test expects count outputs");
            };
            (
                group_key.clone(),
                u32::try_from(*count).expect("grouped count output must fit u32"),
            )
        })
        .collect()
}

fn value_rows(rows: &[(Value, Value)]) -> Vec<(Value, Value)> {
    rows.to_vec()
}

fn finalize_grouped_id_rows(
    kind: AggregateKind,
    direction: Direction,
    ids: &[u64],
    stop_on_break: bool,
) -> Vec<(Value, Value)> {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context.create_grouped_state(kind, direction, false);

    for &id in ids {
        let fold_control = grouped
            .apply_borrowed(
                &text_group_key("alpha"),
                &data_key(id),
                &mut execution_context,
            )
            .expect("grouped id-terminal row should apply");
        if stop_on_break && matches!(fold_control, FoldControl::Break) {
            break;
        }
    }

    into_value_pairs(grouped.finalize())
}

// Apply one fixed grouped-count fixture through one insertion-order projection
// and return finalized `(group_key, count)` rows in emitted order.
fn grouped_count_rows_for_order(order: &[usize]) -> Vec<(Value, u32)> {
    let fixtures = [
        ("alpha", 1_u64),
        ("beta", 2_u64),
        ("alpha", 3_u64),
        ("gamma", 4_u64),
        ("beta", 5_u64),
    ];
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    for fixture_index in order {
        let (group, id) = fixtures[*fixture_index];
        grouped
            .apply_borrowed(
                &text_group_key(group),
                &data_key(id),
                &mut execution_context,
            )
            .expect("determinism fixture rows should apply");
    }

    let finalized = into_value_pairs(grouped.finalize());
    count_rows(finalized.as_slice())
}

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
    let non_null_row = RowView::new(vec![Some(Value::Uint(7))]);
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
    let numeric_row = RowView::new(vec![Some(Value::Uint(7))]);
    let second_numeric_row = RowView::new(vec![Some(Value::Uint(9))]);
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
    let numeric_row = RowView::new(vec![Some(Value::Uint(6))]);
    let second_numeric_row = RowView::new(vec![Some(Value::Uint(12))]);
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
fn grouped_aggregate_state_distinct_deduplicates_repeated_data_keys() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped_distinct =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, true);
    let mut grouped_plain =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, false);

    let group = text_group_key("alpha");
    let duplicate_key = data_key(42);
    grouped_distinct
        .apply_borrowed(&group, &duplicate_key, &mut execution_context)
        .expect("distinct grouped row should apply");
    grouped_distinct
        .apply_borrowed(&group, &duplicate_key, &mut execution_context)
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
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, true);

    grouped
        .apply_borrowed(
            &text_group_key("alpha"),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first grouped distinct value should fit per-group budget");
    let err = grouped
        .apply_borrowed(
            &text_group_key("alpha"),
            &data_key(2),
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
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, true);

    grouped
        .apply_borrowed(
            &text_group_key("alpha"),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first grouped distinct value should fit total budget");
    let err = grouped
        .apply_borrowed(
            &text_group_key("beta"),
            &data_key(2),
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
    let mut grouped =
        execution_context.create_grouped_state(AggregateKind::Count, Direction::Asc, true);

    for (group, id) in [
        ("alpha", 1_u64),
        ("alpha", 1_u64),
        ("alpha", 2_u64),
        ("beta", 3_u64),
        ("beta", 3_u64),
    ] {
        grouped
            .apply_borrowed(
                &text_group_key(group),
                &data_key(id),
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

#[test]
fn grouped_id_terminals_finalize_to_structural_primary_key_values() {
    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Min, Direction::Asc, &[3, 7, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Uint(3))],
        "grouped MIN should finalize to the primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Max, Direction::Desc, &[9, 7, 3], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Uint(9))],
        "grouped MAX should finalize to the primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::First, Direction::Asc, &[7, 3, 9], true,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Uint(7))],
        "grouped FIRST should finalize to the first seen primary-key value with structural output",
    );

    assert_eq!(
        value_rows(
            finalize_grouped_id_rows(AggregateKind::Last, Direction::Asc, &[7, 3, 9], false,)
                .as_slice()
        ),
        vec![(Value::Text("alpha".to_string()), Value::Uint(9))],
        "grouped LAST should finalize to the last seen primary-key value with structural output",
    );
}
