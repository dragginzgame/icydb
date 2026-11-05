use super::{ExecutionConfig, ExecutionContext};
use crate::{
    db::{
        executor::{
            aggregate::contracts::{GroupBudgetResourceCode, error::GroupError},
            group::{GroupKey, GroupKeySet, retained_hash_entry_backing_bytes},
        },
        resource::budget::runtime_value_work,
    },
    error::InternalError,
    value::Value,
};
use std::panic::AssertUnwindSafe;

#[test]
fn grouped_distinct_counts_are_derived_from_grouped_limits() {
    for (groups, bytes, per_group, total) in [
        (2, 0, 1, 2),
        (2, 63, 1, 2),
        (2, 64, 1, 2),
        (2, 127, 1, 2),
        (2, 128, 2, 4),
        (100, 65_536, 1_024, 102_400),
        (u64::MAX, 128, 2, u64::MAX),
    ] {
        let config = ExecutionConfig::with_hard_limits(groups, bytes);
        assert_eq!(config.max_distinct_values_per_group(), per_group);
        assert_eq!(config.max_distinct_values_total(), total);
    }
}

// Isolate count enforcement: real query limits also impose a byte ceiling
// that can reject before these count boundaries are reached.
fn distinct_count_context() -> ExecutionContext {
    let mut config = ExecutionConfig::with_hard_limits(2, 128);
    config.max_group_bytes = 64 * 1024;

    ExecutionContext::new(config)
}

fn distinct_key(value: u64) -> GroupKey {
    GroupKey::from_group_values(vec![Value::Nat64(value)]).expect("canonical key")
}

#[test]
fn grouped_distinct_per_group_cap_admits_exact_limit_and_duplicates() {
    let mut context = distinct_count_context();
    let mut keys = GroupKeySet::new();
    let limit = context.config().max_distinct_values_per_group();
    for value in 0..limit {
        assert!(
            context
                .admit_distinct_value(&mut keys, limit, distinct_key(value).canonical_value())
                .expect("within cap")
        );
    }
    let before = *context.budget();
    assert_eq!(before.distinct_values(), limit);
    assert!(
        !context
            .admit_distinct_value(&mut keys, limit, distinct_key(0).canonical_value())
            .expect("duplicate at cap")
    );
    let error = context
        .admit_distinct_value(&mut keys, limit, distinct_key(limit).canonical_value())
        .expect_err("new key exceeds cap");
    assert!(matches!(
        error,
        GroupError::DistinctBudgetExceeded {
            resource: GroupBudgetResourceCode::DistinctValuesPerGroup,
            attempted: 3,
            limit: 2,
        }
    ));
    assert_eq!(*context.budget(), before);
    assert_eq!(keys.len(), 2);
    assert!(!keys.contains_value(
        distinct_key(limit).canonical_value(),
        distinct_key(limit).hash()
    ));
}

#[test]
fn grouped_distinct_total_cap_is_shared_and_precedes_per_group_cap() {
    let mut context = distinct_count_context();
    let mut groups = [GroupKeySet::new(), GroupKeySet::new()];
    let per_group = context.config().max_distinct_values_per_group();
    for keys in &mut groups {
        for value in 0..per_group {
            assert!(
                context
                    .admit_distinct_value(keys, per_group, distinct_key(value).canonical_value())
                    .expect("within caps")
            );
        }
    }
    let before = *context.budget();
    assert_eq!(
        before.distinct_values(),
        context.config().max_distinct_values_total()
    );
    assert!(
        !context
            .admit_distinct_value(&mut groups[0], per_group, distinct_key(0).canonical_value())
            .expect("duplicate at both caps")
    );
    // Both limits are exhausted here; the total resource must win.
    let error = context
        .admit_distinct_value(&mut groups[0], per_group, distinct_key(2).canonical_value())
        .expect_err("total cap");
    assert!(matches!(
        error,
        GroupError::DistinctBudgetExceeded {
            resource: GroupBudgetResourceCode::DistinctValuesTotal,
            attempted: 5,
            limit: 4,
        }
    ));
    assert_eq!(*context.budget(), before);
    assert_eq!(groups[0].len(), 2);
    assert_eq!(groups[1].len(), 2);
    assert!(!groups[0].contains_value(distinct_key(2).canonical_value(), distinct_key(2).hash()));
}

#[test]
fn grouped_distinct_memory_cap_rejects_without_changing_keys_or_usage() {
    let key = distinct_key(0);
    let bytes = runtime_value_work(key.canonical_value())
        .0
        .saturating_add(retained_hash_entry_backing_bytes::<GroupKey, ()>());
    let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(2, bytes - 1));
    let mut keys = GroupKeySet::new();
    let before = *context.budget();
    let limit = context.config().max_distinct_values_per_group();
    let error = context
        .admit_distinct_value(&mut keys, limit, key.canonical_value())
        .expect_err("memory cap");
    assert!(matches!(error, GroupError::MemoryLimitExceeded {
        resource: GroupBudgetResourceCode::EstimatedBytes,
        attempted,
        limit,
    } if attempted == bytes && limit == bytes - 1));
    assert_eq!(*context.budget(), before);
    assert_eq!(keys.len(), 0);

    let mut context = ExecutionContext::new(ExecutionConfig::with_hard_limits(2, bytes));
    let limit = context.config().max_distinct_values_per_group();
    assert!(
        context
            .admit_distinct_value(&mut keys, limit, key.canonical_value())
            .expect("exact memory cap")
    );
    assert_eq!(context.budget().estimated_bytes(), bytes);
    assert_eq!(context.budget().distinct_values(), 1);
}

#[test]
fn grouped_distinct_estimate_includes_hash_set_capacity() {
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut distinct = GroupKeySet::new();
    let key =
        GroupKey::from_group_values(vec![Value::Text("one".to_string())]).expect("canonical key");
    let expected_minimum = runtime_value_work(key.canonical_value())
        .0
        .saturating_add(retained_hash_entry_backing_bytes::<GroupKey, ()>());

    assert!(
        context
            .admit_distinct_value(&mut distinct, u64::MAX, key.canonical_value())
            .expect("first distinct key")
    );
    assert!(context.budget().estimated_bytes() >= expected_minimum);
    let after_first = context.budget().estimated_bytes();
    assert!(
        !context
            .admit_distinct_value(&mut distinct, u64::MAX, key.canonical_value())
            .expect("duplicate distinct key")
    );
    assert_eq!(context.budget().estimated_bytes(), after_first);
}

#[test]
fn borrowed_distinct_preserves_canonical_budget_and_failure_precedence() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
    let mut keys = GroupKeySet::new();
    let value = Value::Map(vec![
        (Value::Text("z".into()), Value::Blob(vec![7; 4096])),
        (Value::Text("a".into()), Value::Nat64(1)),
    ]);
    assert!(context.admit_distinct_value(&mut keys, 1, &value).unwrap());
    let before = *context.budget();
    let Value::Map(mut entries) = value.clone() else {
        unreachable!()
    };
    entries.reverse();
    assert!(
        !context
            .admit_distinct_value(&mut keys, 1, &Value::Map(entries))
            .unwrap()
    );
    assert_eq!(*context.budget(), before);
    with_test_hash_override(
        Err(test_hash_budget_error),
        AssertUnwindSafe(|| {
            let error = context
                .admit_distinct_value(&mut keys, 1, &value)
                .unwrap_err()
                .into_internal_error();
            assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
            assert_eq!(
                error.diagnostic_facts(),
                test_hash_budget_error().diagnostic_facts()
            );
            let invalid = Value::Map(vec![(Value::Null, Value::Nat64(1))]);
            let error = context
                .admit_distinct_value(&mut keys, 1, &invalid)
                .unwrap_err()
                .into_internal_error();
            assert_eq!(
                error.diagnostic(),
                InternalError::executor_invariant().diagnostic()
            );
        }),
    );
    assert_eq!(*context.budget(), before);
    assert_eq!(keys.len(), 1);
}
