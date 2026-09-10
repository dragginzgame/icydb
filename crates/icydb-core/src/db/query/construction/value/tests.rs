use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
    },
    types::{IntBig, NatBig},
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn copy(value: &Value, root: &RequestExecutionRoot) -> Result<Value, QueryError> {
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        work.copy_value(value)
    })
}

#[test]
fn owned_payload_families_charge_before_each_allocation() {
    let slot = size_of::<Value>() as u64;
    let cases = [
        (Value::Text("abc".to_string()), 3, 1),
        (Value::Blob(vec![1, 2, 3]), 3, 1),
        (Value::IntBig(IntBig::from(i64::MIN)), 8, 1),
        (Value::NatBig(NatBig::from(u64::MAX)), 8, 1),
        (
            Value::List(vec![Value::Text("a".to_string()), Value::Blob(vec![2, 3])]),
            2 * slot + 3,
            3,
        ),
        (
            Value::Map(vec![(
                Value::Text("a".to_string()),
                Value::Blob(vec![2, 3]),
            )]),
            2 * slot + 3,
            3,
        ),
        (
            Value::Enum(ValueEnum::test_payload(
                3,
                4,
                Value::Text("abc".to_string()),
            )),
            slot + 3,
            2,
        ),
    ];
    for (value, bytes, visits) in cases {
        let exact = root(Resource::TemporaryBytes, bytes);
        assert_eq!(copy(&value, &exact).unwrap(), value);
        assert_eq!(exact.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(exact.observed(Resource::NestedValueSteps), visits);
        let short = root(Resource::TemporaryBytes, bytes - 1);
        let error = copy(&value, &short).expect_err("last allocation must reject");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        )));
    }
}

#[test]
fn nested_copy_preserves_enum_identity_collection_order_and_duplicate_entries() {
    let entries = vec![
        (Value::Nat64(2), Value::Text("b".to_string())),
        (Value::Nat64(1), Value::Text("a".to_string())),
        (Value::Nat64(2), Value::Text("again".to_string())),
    ];
    let value = Value::Enum(ValueEnum::test_payload(
        7,
        9,
        Value::List(vec![Value::Map(entries.clone())]),
    ));
    let result = copy(&value, &root(Resource::TemporaryBytes, 10_000)).unwrap();
    let Value::Enum(result) = result else {
        panic!("enum tag");
    };
    assert_eq!(result.type_id().get(), 7);
    assert_eq!(result.variant_id().get(), 9);
    let Some(Value::List(values)) = result.payload() else {
        panic!("list payload");
    };
    let [Value::Map(copied)] = values.as_slice() else {
        panic!("map child");
    };
    assert_eq!(copied, &entries);
}

#[test]
fn zero_backing_values_still_charge_a_visit() {
    for value in [
        Value::Null,
        Value::Unit,
        Value::Bool(true),
        Value::Int64(-1),
        Value::Nat128(u128::MAX),
        Value::Text(String::new()),
        Value::Blob(vec![]),
        Value::List(vec![]),
        Value::Map(vec![]),
        Value::Enum(ValueEnum::test_unit(1, 2)),
        Value::IntBig(IntBig::from(0)),
        Value::NatBig(NatBig::from(0_u64)),
    ] {
        let exact = root(Resource::TemporaryBytes, 0);
        assert_eq!(copy(&value, &exact).unwrap(), value);
        assert_eq!(exact.observed(Resource::TemporaryBytes), 0);
        assert_eq!(exact.observed(Resource::NestedValueSteps), 1);
        assert!(copy(&value, &root(Resource::NestedValueSteps, 0)).is_err());
    }
}

#[test]
fn failed_nested_copy_keeps_source_and_request_charges() {
    let value = Value::List(vec![
        Value::Text("first".to_string()),
        Value::Text("last".to_string()),
    ]);
    let original = value.clone();
    let request = root(Resource::TemporaryBytes, 2 * size_of::<Value>() as u64 + 5);
    let mut observed = 0;
    for _ in 0..2 {
        assert!(copy(&value, &request).is_err());
        assert_eq!(value, original);
        assert!(request.observed(Resource::TemporaryBytes) > observed);
        observed = request.observed(Resource::TemporaryBytes);
    }
    let request = root(Resource::NestedValueSteps, 2);
    assert!(copy(&value, &request).is_err());
    assert_eq!(request.observed(Resource::NestedValueSteps), 3);
}

#[test]
fn byte_copy_work_exhaustion_precedes_scalar_allocation() {
    let request = root(Resource::PredicateExpressionSteps, 3);
    let value = Value::Text("four".to_string());
    let error = copy(&value, &request).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw()
    )));
    assert_eq!(request.observed(Resource::TemporaryBytes), 0);
}
