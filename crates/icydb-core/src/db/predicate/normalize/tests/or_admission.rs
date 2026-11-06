//! OR eligibility and duplicate-only collapse admit comparisons before consumption.

use crate::{
    db::{
        QueryError,
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, normalize,
            normalize::{collapse_same_field_or_equalities, equality_values_are_identical},
            normalize_with_budget,
        },
        query::preparation::PreparationWork,
        tests::support::{assert_budget_exceeded, assert_budget_resource, request_with_limit},
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

#[test]
fn duplicate_or_admits_labels_and_nested_payloads_exactly_and_cumulatively() {
    for (value, bytes, visits) in [
        (Value::Text("λ".repeat(128)), 256, 2),
        (Value::Blob(vec![1; 256]), 256, 2),
        (
            Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Map(vec![(Value::Text("k".into()), Value::Blob(vec![1; 256]))]),
            )),
            257,
            8,
        ),
    ] {
        for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
            let field = "tag".repeat(64);
            let expected = Predicate::Compare(ComparePredicate::with_coercion(
                &field,
                CompareOp::Eq,
                value.clone(),
                coercion,
            ));
            let input = Predicate::Or(vec![expected.clone(); 3]);
            assert_eq!(normalize(input.clone()), expected);
            let steps = 4 + 2 * (1 + field.len() as u64 + bytes);
            for (resource, amount) in [
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, 2 * visits),
            ] {
                assert_boundary(&input, &expected, resource, amount);
            }
        }
    }
}

fn assert_boundary(input: &Predicate, expected: &Predicate, resource: Resource, amount: u64) {
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        for limit in [amount - 1, amount, amount * 2] {
            let root = request_with_limit(resource, limit);
            for attempt in 1..=3 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    normalize_with_budget(input.clone(), work).map_err(QueryError::execute)
                });
                assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                assert_eq!(root.observed(Resource::SortEntries), 0);
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap(), *expected);
                    assert_eq!(root.observed(resource), attempt * amount);
                } else {
                    let error = result.unwrap_err();
                    let actual = root.observed(resource);
                    assert_budget_exceeded(
                        &error.diagnostic_facts(),
                        resource,
                        lane,
                        limit,
                        actual,
                    );
                    break;
                }
            }
        }
    }
}

#[test]
fn or_comparison_refusal_preserves_operands_before_consumption() {
    let before = vec![Predicate::eq("tag".repeat(64), Value::Text("v".repeat(512))); 3];
    for resource in [
        Resource::PredicateExpressionSteps,
        Resource::NestedValueSteps,
    ] {
        let mut children = before.clone();
        let root = request_with_limit(resource, 0);
        let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            collapse_same_field_or_equalities(&mut children, &mut |resource, amount| {
                work.charge(resource, amount)
            })
        })
        .unwrap_err();
        assert_budget_resource(&error.diagnostic_facts(), resource);
        assert_eq!(children, before);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        assert_eq!(root.observed(Resource::SortEntries), 0);
    }
}

#[test]
fn duplicate_comparison_stops_before_later_payloads_on_first_difference() {
    let children = vec![
        Predicate::eq("tag".into(), Value::Nat64(1)),
        Predicate::eq("tag".into(), Value::Nat64(2)),
        Predicate::eq("tag".into(), Value::Blob(vec![1; 4096])),
    ];
    let root = request_with_limit(Resource::NestedValueSteps, 2);
    let identical = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        equality_values_are_identical(&children, &mut |resource, amount| {
            work.charge(resource, amount)
        })
    })
    .unwrap();
    assert!(!identical);
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
}
