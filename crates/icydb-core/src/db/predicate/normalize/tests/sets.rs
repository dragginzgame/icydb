//! Accepted sets share ordering admission through recursive literal normalization.

use crate::{
    db::{
        predicate::{
            CoercionId, CoercionSpec, CompareOp,
            normalize::normalize_compare_value_for_accepted_kind,
        },
        query::preparation::PreparationWork,
        schema::AcceptedFieldKind,
        tests::support::request_with_limit,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn accepted_set_ordering_accumulates_through_collection_and_membership_shells() {
    let kind = AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64));
    let input = Value::List(vec![Value::Int64(3), Value::Nat64(1), Value::Nat64(3)]);
    let expected = Value::List(vec![Value::Nat64(1), Value::Nat64(3)]);
    assert_admission(&kind, CompareOp::Eq, &input, &expected, [3, 10]);
    let repeated_input = Value::List(vec![input.clone(); 2]);
    let repeated_expected = Value::List(vec![expected.clone(); 2]);
    assert_admission(
        &AcceptedFieldKind::List(Box::new(kind.clone())),
        CompareOp::Eq,
        &repeated_input,
        &repeated_expected,
        [6, 20],
    );
    // Outer set deduplication happens after each inner set is canonicalized.
    let singleton = Value::List(vec![expected.clone()]);
    assert_admission(
        &AcceptedFieldKind::Set(Box::new(kind.clone())),
        CompareOp::Eq,
        &repeated_input,
        &singleton,
        [8, 24],
    );
    for op in [CompareOp::In, CompareOp::NotIn] {
        assert_admission(&kind, op, &repeated_input, &singleton, [8, 24]);
    }
    assert_admission(
        &AcceptedFieldKind::List(Box::new(kind.clone())),
        CompareOp::Contains,
        &input,
        &expected,
        [3, 10],
    );
    let key = Value::Text("key".into());
    assert_admission(
        &AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(kind),
        },
        CompareOp::Eq,
        &Value::Map(vec![(key.clone(), input)]),
        &Value::Map(vec![(key, expected)]),
        [3, 10],
    );
}

fn assert_admission(
    kind: &AcceptedFieldKind,
    op: CompareOp,
    input: &Value,
    expected: &Value,
    amounts: [u64; 2],
) {
    for (resource, amount) in [Resource::SortEntries, Resource::SortComparisons]
        .into_iter()
        .zip(amounts)
    {
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
                        normalize_compare_value_for_accepted_kind(
                            "values",
                            op,
                            input,
                            kind,
                            &CoercionSpec::new(CoercionId::Strict),
                            work,
                        )
                    });
                    assert_eq!(root.observed(Resource::SortTemporaryBytes), 0);
                    if attempt * amount <= limit {
                        assert_eq!(result.unwrap(), *expected);
                        assert_eq!(root.observed(resource), attempt * amount);
                    } else {
                        let error = result.unwrap_err();
                        let actual = root.observed(resource);
                        assert!(actual > limit);
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                            (DiagnosticFactTag::Actual, actual),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        break;
                    }
                }
            }
        }
    }
}

#[test]
fn accepted_empty_singleton_sets_and_ordered_lists_need_no_sort_budget() {
    for (kind, input, expected) in [
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![]),
            Value::List(vec![]),
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Int64(3)]),
            Value::List(vec![Value::Nat64(3)]),
        ),
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
            Value::List(vec![Value::Int64(3), Value::Int64(1), Value::Int64(3)]),
            Value::List(vec![Value::Nat64(3), Value::Nat64(1), Value::Nat64(3)]),
        ),
    ] {
        for resource in [Resource::SortEntries, Resource::SortComparisons] {
            let root = request_with_limit(resource, 0);
            let actual = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                normalize_compare_value_for_accepted_kind(
                    "values",
                    CompareOp::Eq,
                    &input,
                    &kind,
                    &CoercionSpec::new(CoercionId::Strict),
                    work,
                )
            })
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(root.observed(Resource::SortEntries), 0);
            assert_eq!(root.observed(Resource::SortComparisons), 0);
        }
    }
}

#[test]
fn recursive_set_payload_allowances_accumulate_above_ordinary_list_construction() {
    let text_kind = AcceptedFieldKind::Text { max_len: None };
    let set = AcceptedFieldKind::Set(Box::new(text_kind.clone()));
    let list = AcceptedFieldKind::List(Box::new(text_kind));
    let text = Value::Text("λ".repeat(128));
    let input = Value::List(vec![text.clone(); 3]);
    let expected = Value::List(vec![text]);
    for (kind, baseline_kind, input, expected, repeats) in [
        (
            set.clone(),
            list.clone(),
            input.clone(),
            expected.clone(),
            1,
        ),
        (
            AcceptedFieldKind::List(Box::new(set)),
            AcceptedFieldKind::List(Box::new(list)),
            Value::List(vec![input; 2]),
            Value::List(vec![expected; 2]),
            2,
        ),
    ] {
        for (resource, extra) in [
            (Resource::NestedValueSteps, repeats * 26),
            (Resource::PredicateExpressionSteps, repeats * 13 * 256),
        ] {
            let run = |kind: &AcceptedFieldKind, root: &crate::db::RequestExecutionRoot| {
                PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                    normalize_compare_value_for_accepted_kind(
                        "values",
                        CompareOp::Eq,
                        &input,
                        kind,
                        &CoercionSpec::new(CoercionId::Strict),
                        work,
                    )
                })
            };
            let baseline = request_with_limit(resource, 16_000_000);
            assert_eq!(run(&baseline_kind, &baseline).unwrap(), input);
            let amount = baseline.observed(resource) + extra;
            let exact = request_with_limit(resource, amount);
            assert_eq!(run(&kind, &exact).unwrap(), expected);
            assert_eq!(exact.observed(resource), amount);
            for root in [&exact, &request_with_limit(resource, amount - 1)] {
                let error = run(&kind, root).unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
            }
        }
    }
}
