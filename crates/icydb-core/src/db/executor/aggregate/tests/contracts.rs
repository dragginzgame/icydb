use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::aggregate::contracts::{
            AggregateKind, AggregateOutput, AggregateSpec, AggregateSpecSupportError,
            ExecutionConfig, ExecutionContext, GroupAggregateSpec, GroupAggregateSpecSupportError,
            GroupError, GroupedAggregateOutput,
        },
        group_key::CanonicalKey,
        value_hash::with_test_hash_override,
    },
    model::field::FieldKind,
    testing,
    value::Value,
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

fn group_key(value: Value) -> crate::db::group_key::GroupKey {
    value
        .canonical_key()
        .expect("group key canonicalization should succeed")
}

fn data_key(id: u64) -> DataKey {
    DataKey::try_new::<GroupedStateTestEntity>(id).expect("test data key should build")
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
    let grouped = GroupAggregateSpec::new(
        vec!["tenant".to_string(), "region".to_string()],
        vec![
            AggregateSpec::for_terminal(AggregateKind::Count),
            AggregateSpec::for_target_field(AggregateKind::Max, "score"),
        ],
    );

    assert!(grouped.is_grouped());
    assert_eq!(
        grouped.group_keys(),
        &["tenant".to_string(), "region".to_string()]
    );
    assert_eq!(grouped.aggregate_specs().len(), 2);
    assert!(grouped.ensure_supported_for_execution().is_ok());
}

#[test]
fn group_aggregate_spec_support_rejects_empty_terminal_list() {
    let grouped = GroupAggregateSpec::new(vec!["tenant".to_string()], Vec::new());
    let err = grouped
        .ensure_supported_for_execution()
        .expect_err("grouped aggregate contract must reject empty aggregate terminal list");

    assert_eq!(err, GroupAggregateSpecSupportError::MissingAggregateSpecs);
}

#[test]
fn group_aggregate_spec_support_rejects_duplicate_group_key() {
    let grouped = GroupAggregateSpec::new(
        vec!["tenant".to_string(), "tenant".to_string()],
        vec![AggregateSpec::for_terminal(AggregateKind::Count)],
    );
    let err = grouped
        .ensure_supported_for_execution()
        .expect_err("grouped aggregate contract must reject duplicate group keys");

    assert_eq!(
        err,
        GroupAggregateSpecSupportError::DuplicateGroupKey {
            field: "tenant".to_string(),
        }
    );
}

#[test]
fn group_aggregate_spec_support_rejects_unsupported_nested_terminal() {
    let grouped = GroupAggregateSpec::new(
        vec!["tenant".to_string()],
        vec![
            AggregateSpec::for_terminal(AggregateKind::Count),
            AggregateSpec::for_target_field(AggregateKind::Exists, "rank"),
        ],
    );
    let err = grouped
        .ensure_supported_for_execution()
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
fn group_aggregate_spec_support_accepts_global_terminal_constructor() {
    let grouped =
        GroupAggregateSpec::for_global_terminal(AggregateSpec::for_terminal(AggregateKind::Count));

    assert!(!grouped.is_grouped());
    assert!(grouped.group_keys().is_empty());
    assert_eq!(grouped.aggregate_specs().len(), 1);
    assert!(grouped.ensure_supported_for_execution().is_ok());
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
