use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::aggregate::contracts::{
            AggregateKind, AggregateOutput, AggregateSpec, AggregateSpecSupportError,
            ExecutionConfig, ExecutionContext, GroupAggregateSpecSupportError, GroupError,
            GroupedAggregateOutput, ensure_grouped_spec_supported_for_execution,
        },
        executor::group::CanonicalKey,
        query::plan::{FieldSlot, GroupAggregateSpec},
    },
    model::field::FieldKind,
    testing,
    traits::EntitySchema,
    value::{Value, with_test_hash_override},
};
use icydb_derive::FieldProjection;
use serde::{Deserialize, Serialize};

crate::test_canister! {
    ident = GroupedStateTestCanister,
    commit_memory_id = testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
struct GroupedStateTestEntity {
    id: u64,
}

crate::test_entity_schema! {
    ident = GroupedStateTestEntity,
    id = u64,
    id_field = id,
    entity_name = "GroupedStateTestEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [("id", FieldKind::Uint)],
    indexes = [],
    store = GroupedStateTestStore,
    canister = GroupedStateTestCanister,
}

fn group_key(value: Value) -> crate::db::executor::group::GroupKey {
    value
        .canonical_key()
        .expect("group key canonicalization should succeed")
}

fn data_key(id: u64) -> DataKey {
    DataKey::try_new::<GroupedStateTestEntity>(id).expect("test data key should build")
}

fn grouped_field_slot(field: &str) -> FieldSlot {
    FieldSlot::resolve(<GroupedStateTestEntity as EntitySchema>::MODEL, field)
        .expect("grouped field slot should resolve in grouped state test model")
}

fn count_rows(rows: &[GroupedAggregateOutput<GroupedStateTestEntity>]) -> Vec<(Value, u32)> {
    rows.iter()
        .map(|row| {
            let AggregateOutput::Count(count) = row.output() else {
                panic!("grouped count-state test expects count outputs");
            };
            (row.group_key().canonical_value().clone(), *count)
        })
        .collect()
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
    let mut grouped = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

    for fixture_index in order {
        let (group, id) = fixtures[*fixture_index];
        grouped
            .apply(
                group_key(Value::Text(group.to_string())),
                &data_key(id),
                &mut execution_context,
            )
            .expect("determinism fixture rows should apply");
    }

    let finalized = grouped.finalize();
    count_rows(finalized.as_slice())
}

#[test]
fn aggregate_spec_support_accepts_terminal_specs_without_field_targets() {
    let spec = AggregateSpec::for_terminal(AggregateKind::Count);

    assert!(spec.ensure_supported_for_execution().is_ok());
}

#[test]
fn aggregate_spec_support_rejects_field_target_non_extrema() {
    let spec = AggregateSpec::for_target_field(AggregateKind::Count, "rank");
    let err = spec
        .ensure_supported_for_execution()
        .expect_err("field-target COUNT should be rejected by support taxonomy");

    assert!(matches!(
        err,
        AggregateSpecSupportError::FieldTargetRequiresExtrema { .. }
    ));
}

#[test]
fn aggregate_spec_support_accepts_field_target_extrema() {
    let spec = AggregateSpec::for_target_field(AggregateKind::Min, "rank");
    assert!(spec.ensure_supported_for_execution().is_ok());
}

#[test]
fn group_aggregate_spec_support_accepts_group_keys_and_supported_specs() {
    let group_fields = vec![grouped_field_slot("id")];
    let grouped_aggregates = vec![
        GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
        },
        GroupAggregateSpec {
            kind: AggregateKind::Max,
            target_field: Some("score".to_string()),
        },
    ];

    assert!(
        ensure_grouped_spec_supported_for_execution(
            group_fields.as_slice(),
            grouped_aggregates.as_slice(),
        )
        .is_ok()
    );
}

#[test]
fn group_aggregate_spec_support_rejects_empty_terminal_list() {
    let group_fields = vec![grouped_field_slot("id")];
    let grouped_aggregates = Vec::<GroupAggregateSpec>::new();
    let err = ensure_grouped_spec_supported_for_execution(
        group_fields.as_slice(),
        grouped_aggregates.as_slice(),
    )
    .expect_err("grouped aggregate contract must reject empty aggregate terminal list");

    assert_eq!(err, GroupAggregateSpecSupportError::MissingAggregateSpecs);
}

#[test]
fn group_aggregate_spec_support_rejects_duplicate_group_key() {
    let duplicate_field = grouped_field_slot("id");
    let group_fields = vec![duplicate_field.clone(), duplicate_field];
    let grouped_aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: None,
    }];
    let err = ensure_grouped_spec_supported_for_execution(
        group_fields.as_slice(),
        grouped_aggregates.as_slice(),
    )
    .expect_err("grouped aggregate contract must reject duplicate group keys");

    assert_eq!(
        err,
        GroupAggregateSpecSupportError::DuplicateGroupKey {
            field: "id".to_string(),
        }
    );
}

#[test]
fn group_aggregate_spec_support_rejects_unsupported_nested_terminal() {
    let group_fields = vec![grouped_field_slot("id")];
    let grouped_aggregates = vec![
        GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
        },
        GroupAggregateSpec {
            kind: AggregateKind::Exists,
            target_field: Some("rank".to_string()),
        },
    ];
    let err = ensure_grouped_spec_supported_for_execution(
        group_fields.as_slice(),
        grouped_aggregates.as_slice(),
    )
    .expect_err("grouped aggregate contract must reject unsupported nested terminals");

    assert!(matches!(
        err,
        GroupAggregateSpecSupportError::AggregateSpecUnsupported {
            index: 1,
            source: AggregateSpecSupportError::FieldTargetRequiresExtrema { .. },
        }
    ));
}

#[test]
fn group_aggregate_spec_support_accepts_empty_group_fields_with_one_terminal_spec() {
    let group_fields = Vec::<FieldSlot>::new();
    let grouped_aggregates = vec![GroupAggregateSpec {
        kind: AggregateKind::Count,
        target_field: None,
    }];

    assert!(
        ensure_grouped_spec_supported_for_execution(
            group_fields.as_slice(),
            grouped_aggregates.as_slice(),
        )
        .is_ok()
    );
}

#[test]
fn grouped_aggregate_state_reuses_per_group_state_and_counts_rows() {
    let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut grouped = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

    grouped
        .apply(
            group_key(Value::Text("alpha".to_string())),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first grouped row should apply");
    grouped
        .apply(
            group_key(Value::Text("alpha".to_string())),
            &data_key(2),
            &mut execution_context,
        )
        .expect("second grouped row for same group should apply");
    grouped
        .apply(
            group_key(Value::Text("beta".to_string())),
            &data_key(3),
            &mut execution_context,
        )
        .expect("third grouped row for second group should apply");

    assert_eq!(
        grouped.group_count(),
        2,
        "grouped state should keep one slot per canonical group key",
    );
    assert_eq!(execution_context.budget().groups(), 2);
    assert_eq!(execution_context.budget().aggregate_states(), 2);
    assert!(
        execution_context.budget().estimated_bytes() > 0,
        "grouped budget should account for inserted group state bytes",
    );

    let finalized = grouped.finalize();
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
fn grouped_aggregate_state_finalization_is_deterministic_under_hash_collisions() {
    with_test_hash_override([0xCD; 16], || {
        let mut execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
        let mut grouped = execution_context
            .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

        // Intentionally insert in reverse lexical order under one forced
        // hash bucket; finalize must still emit canonical key order.
        grouped
            .apply(
                group_key(Value::Text("gamma".to_string())),
                &data_key(1),
                &mut execution_context,
            )
            .expect("gamma grouped row should apply");
        grouped
            .apply(
                group_key(Value::Text("alpha".to_string())),
                &data_key(2),
                &mut execution_context,
            )
            .expect("alpha grouped row should apply");
        grouped
            .apply(
                group_key(Value::Text("beta".to_string())),
                &data_key(3),
                &mut execution_context,
            )
            .expect("beta grouped row should apply");

        let finalized = grouped.finalize();
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
    let mut grouped = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

    grouped
        .apply(
            group_key(Value::Text("a".to_string())),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first group should fit budget");
    grouped
        .apply(
            group_key(Value::Text("b".to_string())),
            &data_key(2),
            &mut execution_context,
        )
        .expect("second group should fit budget");
    let err = grouped
        .apply(
            group_key(Value::Text("c".to_string())),
            &data_key(3),
            &mut execution_context,
        )
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
    let mut grouped = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

    let err = grouped
        .apply(
            group_key(Value::Text("only".to_string())),
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
    let mut grouped_count = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);
    let mut grouped_exists = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Exists, Direction::Asc);

    grouped_count
        .apply(
            group_key(Value::Text("a".to_string())),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first grouped state should accept first canonical group");
    grouped_exists
        .apply(
            group_key(Value::Text("a".to_string())),
            &data_key(1),
            &mut execution_context,
        )
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
        .apply(
            group_key(Value::Text("b".to_string())),
            &data_key(2),
            &mut execution_context,
        )
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
    let mut grouped = execution_context
        .create_grouped_state::<GroupedStateTestEntity>(AggregateKind::Count, Direction::Asc);

    grouped
        .apply(
            group_key(Value::Text("a".to_string())),
            &data_key(1),
            &mut execution_context,
        )
        .expect("first group should fit budget");
    let err = grouped
        .apply(
            group_key(Value::Text("b".to_string())),
            &data_key(2),
            &mut execution_context,
        )
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
        grouped.group_count(),
        1,
        "failed grouped insertion must not leak partial state",
    );
    let finalized = grouped.finalize();
    assert_eq!(
        count_rows(finalized.as_slice()),
        vec![(Value::Text("a".to_string()), 1)],
        "budget-limit errors must preserve previously committed grouped outputs",
    );
}
