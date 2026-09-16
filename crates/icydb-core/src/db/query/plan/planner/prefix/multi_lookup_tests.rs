//! IN validation visits reject before construction and retain lookup semantics.

use super::index_multi_lookup_for_in;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate},
        query::{
            plan::{VisibleIndexes, planner::prefix_test_schema},
            preparation::PreparationWork,
        },
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::PredicateExpressionSteps, limit),
    )
}

#[test]
fn multi_lookup_visits_admit_schema_and_expression_checks_cumulatively() {
    let schema = prefix_test_schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    for count in [1, 3, 64] {
        let values = vec![Value::Text("İ".into()); count];
        for (coercion, selected, expected_text, per_value_steps) in [
            (CoercionId::Strict, "a_raw", "İ", 1 + "İ".len() as u64),
            // Schema visit, one shape visit per eligible expression index, conversion.
            (
                CoercionId::TextCasefold,
                "a_lower",
                "i\u{307}",
                3 + lower_text_construction_allowance("İ".len()).1,
            ),
        ] {
            let cmp = ComparePredicate::with_coercion(
                "name",
                CompareOp::In,
                Value::List(values.clone()),
                coercion,
            );
            let exact = indexes.len() as u64 + count as u64 * per_value_steps;
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for limit in [0, count as u64 - 1, exact - 1, exact * 2] {
                    let root = request(limit);
                    PreparationWork::run(&root.scope(), lane, |budget| {
                        for attempt in 1..=3 {
                            let result = index_multi_lookup_for_in(
                                indexes, &schema, &cmp, &values, None, false, budget,
                            );
                            if attempt * exact <= limit {
                                let plan = result.unwrap().unwrap();
                                assert_eq!(
                                    plan.selected_index_contract().unwrap().name(),
                                    selected
                                );
                                assert_eq!(
                                    plan.as_index_multi_lookup_contract_path().unwrap().1,
                                    vec![Value::Text(expected_text.into()); count]
                                );
                                assert_eq!(
                                    root.observed(Resource::PredicateExpressionSteps),
                                    attempt * exact
                                );
                            } else {
                                assert!(
                                    QueryError::execute(result.unwrap_err())
                                        .diagnostic_facts()
                                        .contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            Resource::PredicateExpressionSteps.raw()
                                        ))
                                );
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    if limit < count as u64 {
                        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                        assert_eq!(root.observed(Resource::NestedValueSteps), 0);
                    }
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
            assert_eq!(cmp.value(), &Value::List(values.clone()));
        }
    }
}

#[test]
fn multi_lookup_validation_stops_at_first_incompatible_value() {
    let schema = prefix_test_schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    for (values, visited) in [
        (vec![], 0),
        (vec![Value::Nat64(1), Value::Text("later".into())], 1),
        (
            vec![
                Value::Text("first".into()),
                Value::Nat64(1),
                Value::Text("later".into()),
            ],
            2,
        ),
    ] {
        let cmp = ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(values.clone()),
            CoercionId::Strict,
        );
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            let root = request(visited);
            PreparationWork::run(&root.scope(), lane, |budget| {
                assert!(
                    index_multi_lookup_for_in(indexes, &schema, &cmp, &values, None, false, budget)
                        .unwrap()
                        .is_none()
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), visited);
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::NestedValueSteps), 0);
        }
    }
}
