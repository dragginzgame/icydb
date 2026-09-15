//! Branch scoring canonicalizes borrowed operands without exporting a value list.

use super::{evaluate_branch_set_candidate_from_contract, evaluate_branch_values};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            plan::{
                VisibleIndexes,
                access_choice::model::{AccessChoiceRejectedReason as Reason, CandidateEvaluation},
                exact_metadata_schema,
                planner::{MAX_INDEX_BRANCH_SET_VALUES, prefix_test_schema},
            },
            preparation::PreparationWork,
        },
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::borrow::Cow;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn membership(field: &str, values: &[i64]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        CompareOp::In,
        Value::List(values.iter().copied().map(Value::Int64).collect()),
        CoercionId::Strict,
    ))
}

const fn prefix_len(evaluation: CandidateEvaluation) -> Result<usize, Reason> {
    match evaluation {
        CandidateEvaluation::Eligible(score) => Ok(score.prefix_len),
        CandidateEvaluation::Rejected(reason) => Err(reason),
    }
}

#[test]
fn branch_scores_preserve_set_equality_caps_and_rejection_order() {
    let schema = exact_metadata_schema(&[("a", &["age", "rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = &visible.accepted_semantic_index_contracts()[0];
    let too_many: Vec<_> = (0..=MAX_INDEX_BRANCH_SET_VALUES)
        .map(|value| i64::try_from(value).unwrap())
        .collect();
    let invalid = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::In,
        Value::List(vec![Value::Text("invalid".into())]),
        CoercionId::Strict,
    ));
    for (children, expected) in [
        (
            vec![Predicate::True],
            Err(Reason::PredicateShapeNotBranchSet),
        ),
        (
            vec![membership("age", &[1, 2])],
            Err(Reason::PredicateShapeNotBranchSet),
        ),
        (vec![membership("rank", &[])], Err(Reason::InLiteralEmpty)),
        (
            vec![membership("rank", &[1, 1])],
            Err(Reason::PredicateShapeNotBranchSet),
        ),
        (
            vec![membership("rank", &[2, 1, 1]), membership("rank", &[1, 2])],
            Ok(2),
        ),
        (
            vec![membership("rank", &[1, 2]), membership("rank", &[1, 3])],
            Err(Reason::ConflictingEqConstraints),
        ),
        (
            vec![membership("rank", &too_many[..MAX_INDEX_BRANCH_SET_VALUES])],
            Ok(2),
        ),
        (
            vec![membership("rank", &too_many)],
            Err(Reason::PredicateShapeNotBranchSet),
        ),
        (
            vec![membership("rank", &too_many), invalid.clone()],
            Err(Reason::InLiteralIncompatible),
        ),
        (
            vec![membership("rank", &[1]), invalid],
            Err(Reason::InLiteralIncompatible),
        ),
        (
            vec![Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::In,
                Value::Int64(1),
                CoercionId::Strict,
            ))],
            Err(Reason::InLiteralNotList),
        ),
        (
            vec![Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::In,
                Value::Int64(1),
                CoercionId::NumericWiden,
            ))],
            Err(Reason::NonStrictCoercion),
        ),
    ] {
        let before = children.clone();
        let root = request(Resource::NestedValueSteps, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let evaluation = evaluate_branch_values(index, 1, &schema, &children, work).unwrap();
            if let CandidateEvaluation::Eligible(score) = &evaluation {
                assert!(!score.exact);
            }
            assert_eq!(prefix_len(evaluation), expected);
            Ok(())
        })
        .unwrap();
        assert_eq!(children, before);
    }
}

#[test]
fn branch_views_and_expression_conversion_obey_cumulative_admission() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let sources: [&[&str]; 2] = [&["İΣ", "A", "A"], &["A", "İΣ"]];
    let (lower_bytes, lower_steps) =
        sources
            .iter()
            .flat_map(|values| values.iter())
            .fold((0, 0), |(bytes, steps), text| {
                let (next_bytes, next_steps) = lower_text_construction_allowance(text.len());
                (bytes + next_bytes, steps + next_steps)
            });
    for expression in [false, true] {
        let index = indexes
            .iter()
            .find(|index| index.name() == if expression { "a_lower" } else { "a_raw" })
            .unwrap();
        let children: Vec<_> = sources
            .iter()
            .map(|values| {
                Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::In,
                    Value::List(
                        values
                            .iter()
                            .map(|text| Value::Text((*text).into()))
                            .collect(),
                    ),
                    if expression {
                        CoercionId::TextCasefold
                    } else {
                        CoercionId::Strict
                    },
                ))
            })
            .collect();
        let bytes =
            (5 * size_of::<Cow<'_, Value>>()) as u64 + if expression { lower_bytes } else { 0 };
        let steps = 12 + if expression { lower_steps } else { 0 };
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (Resource::TemporaryBytes, bytes),
                (Resource::PredicateExpressionSteps, steps),
                (Resource::NestedValueSteps, 0),
            ] {
                for limit in [0, exact.saturating_sub(1), exact * 2] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        for attempt in 1..=3 {
                            let result = evaluate_branch_values(index, 0, &schema, &children, work);
                            if exact == 0 || attempt * exact <= limit {
                                assert_eq!(prefix_len(result.unwrap()), Ok(1));
                                assert_eq!(root.observed(resource), attempt * exact);
                            } else {
                                assert!(
                                    QueryError::execute(result.unwrap_err())
                                        .diagnostic_facts()
                                        .contains(&(
                                            DiagnosticFactTag::BudgetResource,
                                            resource.raw()
                                        ))
                                );
                                break;
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn branch_scores_compare_normalized_sets_and_count_distinct_values() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = visible
        .accepted_semantic_index_contracts()
        .iter()
        .find(|index| index.name() == "a_lower")
        .unwrap();
    for (sets, expected) in [
        (
            vec![vec!["İΣ", "NAME"], vec!["name", "i\u{307}ς", "name"]],
            Ok(1),
        ),
        (
            vec![vec!["NAME", "name"]],
            Err(Reason::PredicateShapeNotBranchSet),
        ),
        (
            vec![vec!["İΣ", "NAME"], vec!["name", "i"]],
            Err(Reason::ConflictingEqConstraints),
        ),
    ] {
        let children: Vec<_> = sets
            .into_iter()
            .map(|values| {
                Predicate::Compare(ComparePredicate::with_coercion(
                    "name",
                    CompareOp::In,
                    Value::List(
                        values
                            .into_iter()
                            .map(|text| Value::Text(text.into()))
                            .collect(),
                    ),
                    CoercionId::TextCasefold,
                ))
            })
            .collect();
        let root = request(Resource::NestedValueSteps, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                prefix_len(evaluate_branch_values(index, 0, &schema, &children, work).unwrap()),
                expected
            );
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn branch_candidate_propagates_exhaustion_after_equality_prefix_admission() {
    let schema = exact_metadata_schema(&[("a", &["age", "rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = &visible.accepted_semantic_index_contracts()[0];
    let predicate = Predicate::And(vec![
        Predicate::eq("age".into(), Value::Int64(7)),
        membership("rank", &[2, 1]),
    ]);
    let prefix_bytes = (2 * size_of::<(&str, &Value, CoercionId, bool)>()) as u64;
    let branch_bytes = (2 * size_of::<Cow<'_, Value>>()) as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [prefix_bytes, prefix_bytes + branch_bytes] {
            let root = request(Resource::TemporaryBytes, limit);
            PreparationWork::run(&root.scope(), lane, |work| {
                let result =
                    evaluate_branch_set_candidate_from_contract(index, &schema, &predicate, work);
                if limit == prefix_bytes {
                    assert!(
                        QueryError::execute(result.unwrap_err())
                            .diagnostic_facts()
                            .contains(&(
                                DiagnosticFactTag::BudgetResource,
                                Resource::TemporaryBytes.raw()
                            ))
                    );
                } else {
                    assert_eq!(prefix_len(result.unwrap()), Ok(2));
                }
                Ok(())
            })
            .unwrap();
        }
    }
}
