//! Conjunction reductions admit payloads before comparing or folding text.

use crate::{
    db::{
        QueryError,
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, normalize, normalize_with_budget,
            simplify::simplify_and_compare_constraints,
        },
        query::preparation::PreparationWork,
        tests::support::{assert_budget_exceeded, request_with_limit},
    },
    value::{Value, ValueEnum, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn compare(field: &str, op: CompareOp, value: Value, coercion: CoercionId) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
}

fn assert_boundary(input: &Predicate, resource: Resource, amount: u64) {
    let expected = normalize(input.clone());
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
                assert_eq!(root.observed(Resource::SortEntries), 0);
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap(), expected);
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
fn conjunction_comparison_payloads_use_exact_and_cumulative_request_limits() {
    let field = "tag".repeat(64);
    for coercion in [CoercionId::Strict, CoercionId::CollectionElement] {
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
            let leaf = compare(&field, CompareOp::Eq, value, coercion);
            let input = Predicate::And(vec![leaf.clone(), leaf]);
            assert_boundary(
                &input,
                Resource::PredicateExpressionSteps,
                4 + field.len() as u64 + bytes,
            );
            assert_boundary(&input, Resource::NestedValueSteps, visits);
        }
    }
}

#[test]
fn every_reducing_order_pair_admits_its_comparison_and_casefold_backing() {
    for (left_op, right_op, shifted) in [
        (CompareOp::Eq, CompareOp::Eq, 0),
        (CompareOp::Eq, CompareOp::Lte, 0),
        (CompareOp::Gte, CompareOp::Eq, 1),
        (CompareOp::Gt, CompareOp::Gte, 0),
        (CompareOp::Lt, CompareOp::Lte, 0),
        (CompareOp::Gte, CompareOp::Lte, 0),
        (CompareOp::Lte, CompareOp::Gte, 1),
        (CompareOp::Gt, CompareOp::Lt, 0),
    ] {
        for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
            let text = "İΣ".repeat(64);
            let len = text.len() as u64;
            let input = Predicate::And(vec![
                compare("tag", left_op, Value::Text(text.clone()), coercion),
                compare("tag", right_op, Value::Text(text.clone()), coercion),
            ]);
            if coercion == CoercionId::TextCasefold {
                let (backing, steps) = lower_text_construction_allowance(text.len());
                assert_boundary(&input, Resource::TemporaryBytes, 2 * backing);
                assert_boundary(
                    &input,
                    Resource::PredicateExpressionSteps,
                    7 + shifted + 2 * steps + 2 * len,
                );
            } else {
                assert_boundary(&input, Resource::NestedValueSteps, 2);
                assert_boundary(
                    &input,
                    Resource::PredicateExpressionSteps,
                    7 + shifted + len,
                );
            }
        }
    }
}

#[test]
fn unrelated_domains_and_unsupported_operators_skip_payload_admission() {
    let value = Value::Text("İ".repeat(1024));
    for (field, op, coercion) in [
        ("other", CompareOp::Eq, CoercionId::TextCasefold),
        ("tag", CompareOp::Eq, CoercionId::Strict),
        ("tag", CompareOp::Contains, CoercionId::TextCasefold),
    ] {
        let children = vec![
            compare(
                "tag",
                CompareOp::Eq,
                value.clone(),
                CoercionId::TextCasefold,
            ),
            compare(field, op, value.clone(), coercion),
        ];
        let mut charges = Vec::new();
        let output = simplify_and_compare_constraints(children.clone(), &mut |resource, amount| {
            charges.push((resource, amount));
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
        assert_eq!(output, Some(children));
        assert_eq!(
            charges,
            vec![
                (Resource::PredicateExpressionSteps, 1),
                (Resource::PredicateExpressionSteps, 3)
            ]
        );
    }
}

#[test]
fn conjunction_refusal_stops_before_later_payloads_and_returning_partial_output() {
    let input = Predicate::And(vec![
        Predicate::eq("tag".into(), Value::Text("same".repeat(64))),
        Predicate::eq("tag".into(), Value::Text("same".repeat(64))),
        Predicate::eq("tag".into(), Value::Blob(vec![0; 4096])),
    ]);
    let root = request_with_limit(Resource::NestedValueSteps, 1);
    let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        normalize_with_budget(input, work).map_err(QueryError::execute)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::NestedValueSteps.raw()
    )));
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
    // Four node dispatches plus the first pair and its field label. No payload
    // bytes, later pair, sorting, or newly allocated backing are admitted.
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 8);
    assert_eq!(root.observed(Resource::SortEntries), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}
