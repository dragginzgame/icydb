use super::{
    ScalarDistinctValueBucket, ScalarDistinctValueSet, scalar_distinct_conservative_unit_work,
};
use crate::{
    db::{
        QueryError,
        executor::group::stable_hash_value,
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            charge_current_execution_budget, runtime_value_work,
            with_query_execution_budget_for_tests,
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope,
    DiagnosticExecutionLane, RuntimeBoundaryCode,
};

const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::TrustedRead,
    0x2220_0008_0000_0001,
);

#[test]
fn scalar_distinct_value_set_uses_hash_buckets_without_changing_value_equality() {
    let mut values = ScalarDistinctValueSet::new();
    let first = Value::Text("first".to_string());
    let second = Value::Text("second".to_string());
    let first_hash = stable_hash_value(&first).expect("first hash");
    let second_hash = stable_hash_value(&second).expect("second hash");

    assert!(!values.contains(first_hash, &first));
    values
        .insert(first_hash, first.clone())
        .expect("first retained value");
    assert!(values.contains(first_hash, &first));
    assert!(!values.contains(second_hash, &second));
    values
        .insert(second_hash, second.clone())
        .expect("second retained value");
    assert!(values.contains(second_hash, &second));
}

#[test]
fn scalar_distinct_collision_bucket_compares_exact_values() {
    let mut bucket = ScalarDistinctValueBucket::Single(Value::Nat64(1));
    bucket
        .insert(Value::Nat64(2))
        .expect("collision bucket promotion");

    assert!(bucket.contains(&Value::Nat64(1)));
    assert!(bucket.contains(&Value::Nat64(2)));
    assert!(!bucket.contains(&Value::Nat64(3)));
}

#[test]
fn exact_distinct_budget_covers_hash_entries_and_collision_promotion() {
    let value = Value::Int64(0);
    let (state_bytes, nested_steps) = scalar_distinct_conservative_unit_work(&value);
    let (value_bytes, expected_nested_steps) = runtime_value_work(&value);
    let hash_entry_bytes = crate::db::executor::group::retained_hash_entry_backing_bytes::<
        crate::db::executor::group::StableHash,
        ScalarDistinctValueBucket,
    >();
    let collision_transition_bytes =
        crate::db::executor::group::retained_vec_element_backing_bytes::<Value>().saturating_mul(2);

    assert_eq!(
        std::mem::size_of::<ScalarDistinctValueBucket>(),
        std::mem::size_of::<Value>(),
        "the inline scalar DISTINCT bucket must retain the canonical value layout",
    );
    assert_eq!(nested_steps, expected_nested_steps);
    assert!(state_bytes >= value_bytes.saturating_add(hash_entry_bytes));
    assert!(state_bytes >= value_bytes.saturating_add(collision_transition_bytes));
}

#[test]
fn scalar_distinct_hash_capacity_is_part_of_the_typed_state_budget() {
    let value = Value::Text("retained".to_string());
    let hash = stable_hash_value(&value).expect("stable hash");
    let value_bytes = runtime_value_work(&value).0;
    let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            value_bytes,
        );
    let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
        let mut values = ScalarDistinctValueSet::new();
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            value_bytes,
        )
        .map_err(QueryError::execute)?;
        values.insert(hash, value).map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("value bytes alone must not admit hidden scalar hash capacity")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
}
