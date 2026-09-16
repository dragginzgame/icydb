//! The shared comparison extent is cumulative and does not copy operands.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{construction::ConstructionBudget, preparation::PreparationWork},
    },
    types::{IntBig, NatBig},
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn admit(value: &Value, root: &RequestExecutionRoot) -> Result<(), QueryError> {
    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        let budget: &dyn ConstructionBudget = work;
        budget
            .admit_value_comparison(value)
            .map_err(QueryError::execute)
    })
}

#[test]
fn comparison_extent_covers_bytes_limbs_and_nested_nodes_without_backing() {
    for (value, bytes, visits) in [
        (Value::Text("λx".into()), 3, 2),
        (Value::Blob(vec![1, 2, 3]), 3, 2),
        (
            Value::IntBig(i128::MIN.to_string().parse::<IntBig>().unwrap()),
            16,
            2,
        ),
        (
            Value::NatBig(u128::MAX.to_string().parse::<NatBig>().unwrap()),
            16,
            2,
        ),
        (
            Value::List(vec![Value::Text("a".into()), Value::Blob(vec![2, 3])]),
            3,
            6,
        ),
        (
            Value::Map(vec![(Value::Text("a".into()), Value::Blob(vec![2, 3]))]),
            3,
            6,
        ),
        (
            Value::Enum(ValueEnum::test_payload(3, 4, Value::Text("abc".into()))),
            3,
            4,
        ),
    ] {
        let before = value.clone();
        for (resource, cost) in [
            (Resource::PredicateExpressionSteps, bytes),
            (Resource::NestedValueSteps, visits),
        ] {
            let exact = request(resource, cost * 2);
            admit(&value, &exact).unwrap();
            admit(&value, &exact).unwrap();
            assert_eq!(exact.observed(resource), cost * 2);
            let error = admit(&value, &exact).unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
            for limit in 0..cost {
                let short = request(resource, limit);
                assert!(admit(&value, &short).is_err());
            }
            assert_eq!(exact.observed(Resource::TemporaryBytes), 0);
            assert_eq!(exact.observed(Resource::RowsVisited), 0);
            assert_eq!(value, before);
        }
    }
}

#[test]
fn comparison_extent_stops_at_rejected_parent_before_visiting_payload() {
    let value = Value::Enum(ValueEnum::test_payload(
        1,
        2,
        Value::List(vec![Value::Text("payload".into())]),
    ));
    let root = request(Resource::NestedValueSteps, 1);
    assert!(admit(&value, &root).is_err());
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn admitted_equality_preserves_structural_semantics_and_short_circuits() {
    use crate::db::query::preparation::with_preparation_work;
    use std::borrow::Cow;

    for (left, right) in [
        (Value::Nat64(1), Value::Int64(1)),
        (Value::Text("λ".into()), Value::Text("λ".into())),
        (Value::Text("λ".into()), Value::Text("x".into())),
        (
            Value::List(vec![Value::Nat64(1)]),
            Value::List(vec![Value::Int64(1)]),
        ),
        (
            Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1]))]),
            Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1]))]),
        ),
    ] {
        with_preparation_work(|work| {
            let budget: &dyn ConstructionBudget = work;
            assert_eq!(budget.values_equal(&left, &right).unwrap(), left == right);
            let left = [Cow::Borrowed(&left)];
            let right = [Cow::Borrowed(&right)];
            assert_eq!(
                budget.value_slices_equal(&left, &right).unwrap(),
                left == right
            );
        });
    }

    let root = request(Resource::NestedValueSteps, 0);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        let budget: &dyn ConstructionBudget = work;
        assert!(budget.value_slices_equal::<Value>(&[], &[]).unwrap());
        assert!(!budget.value_slices_equal(&[Value::Null], &[]).unwrap());
        assert!(budget.values_equal(&Value::Null, &Value::Null).is_err());
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn slice_equality_admits_each_payload_and_stops_after_a_mismatch() {
    let left = [Value::Text("λ".into()), Value::Text("tail".into())];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, cost) in [
            (Resource::PredicateExpressionSteps, 6),
            (Resource::NestedValueSteps, 4),
        ] {
            let root = request(resource, cost * 2);
            for attempt in 0..3 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    let budget: &dyn ConstructionBudget = work;
                    budget
                        .value_slices_equal(&left, &left)
                        .map_err(QueryError::execute)
                });
                if attempt < 2 {
                    assert!(result.unwrap());
                    assert_eq!(root.observed(resource), (attempt + 1) * cost);
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                }
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        }
    }
    let root = request(Resource::NestedValueSteps, 2);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        let budget: &dyn ConstructionBudget = work;
        assert!(
            !budget
                .value_slices_equal(&left, &[Value::Null, Value::Null])
                .map_err(QueryError::execute)?
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
}
