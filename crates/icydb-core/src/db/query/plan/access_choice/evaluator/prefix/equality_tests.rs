//! Equality-prefix evaluation borrows operands and preserves diagnostic reasons.

use super::{evaluate_branch_set_candidate_from_contract, evaluate_prefix_candidate};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            plan::{
                VisibleIndexes,
                access_choice::{
                    chosen_score_for_visible_indexes,
                    model::{
                        AccessChoiceFamily, AccessChoiceRejectedReason as Reason,
                        CandidateEvaluation,
                    },
                },
                exact_metadata_schema,
                planner::{AccessCandidateScore, prefix_test_schema},
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

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn equal(field: &str, value: i64) -> Predicate {
    Predicate::eq(field.into(), Value::Int64(value))
}

const fn prefix_len(evaluation: CandidateEvaluation) -> Result<usize, Reason> {
    match evaluation {
        CandidateEvaluation::Eligible(score) => Ok(score.prefix_len),
        CandidateEvaluation::Rejected(reason) => Err(reason),
    }
}

#[test]
fn equality_prefix_scores_preserve_gaps_conflicts_and_rejection_precedence() {
    let schema = exact_metadata_schema(&[("a", &["age", "rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = &visible.accepted_semantic_index_contracts()[0];
    let invalid = Predicate::eq("age".into(), Value::Text("invalid".into()));
    for (children, expected) in [
        (vec![Predicate::True], Err(Reason::NoEqConstraints)),
        (
            vec![equal("rank", 1)],
            Err(Reason::LeadingFieldUnconstrained),
        ),
        (vec![invalid.clone()], Err(Reason::LiteralIncompatible)),
        (vec![invalid.clone(), equal("age", 1)], Ok(1)),
        (vec![equal("age", 1), invalid], Ok(1)),
        (vec![equal("age", 1), equal("age", 1)], Ok(1)),
        (
            vec![equal("age", 1), equal("age", 2)],
            Err(Reason::ConflictingEqConstraints),
        ),
        (vec![equal("age", 1), equal("id", 2), equal("id", 3)], Ok(1)),
        (
            vec![equal("age", 1), equal("rank", 2), equal("id", 3)],
            Ok(3),
        ),
        (
            vec![Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Eq,
                Value::Int64(1),
                CoercionId::NumericWiden,
            ))],
            Err(Reason::NoEqConstraints),
        ),
    ] {
        let predicate = Predicate::And(children);
        let before = predicate.clone();
        let root = request(Resource::NestedValueSteps, 16_000_000);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let evaluation = evaluate_prefix_candidate(index, &schema, &predicate, work).unwrap();
            if let CandidateEvaluation::Eligible(score) = &evaluation {
                assert_eq!(score.exact, score.prefix_len == 3);
            }
            assert_eq!(prefix_len(evaluation), expected);
            if let Err(reason) = expected {
                assert_eq!(
                    prefix_len(
                        evaluate_branch_set_candidate_from_contract(
                            index, &schema, &predicate, work
                        )
                        .unwrap()
                    ),
                    Err(reason)
                );
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(predicate, before);
    }
}

#[test]
fn equality_prefix_conversion_and_constraint_backing_share_cumulative_admission() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let source = "İΣ".repeat(128);
    let (lower_bytes, lower_steps) = lower_text_construction_allowance(source.len());
    for expression in [false, true] {
        let index = indexes
            .iter()
            .find(|index| index.name() == if expression { "a_lower" } else { "a_raw" })
            .unwrap();
        let cmp = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text(source.clone()),
            if expression {
                CoercionId::TextCasefold
            } else {
                CoercionId::Strict
            },
        ));
        let predicate = Predicate::And(vec![cmp.clone(), cmp]);
        let bytes = (2 * size_of::<(&str, &Value, CoercionId, bool)>()) as u64
            + if expression { 2 * lower_bytes } else { 0 };
        let comparison_bytes = if expression {
            source.to_lowercase().len()
        } else {
            source.len()
        } as u64;
        let steps = 5 + if expression { 2 * lower_steps } else { 0 } + comparison_bytes;
        for branch in [false, true] {
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, exact) in [
                    (Resource::TemporaryBytes, bytes),
                    (Resource::PredicateExpressionSteps, steps),
                    (Resource::NestedValueSteps, 2),
                ] {
                    for limit in [0, exact.saturating_sub(1), exact * 2] {
                        let root = request(resource, limit);
                        PreparationWork::run(&root.scope(), lane, |work| {
                            for attempt in 1..=3 {
                                let result = if branch {
                                    evaluate_branch_set_candidate_from_contract(
                                        index, &schema, &predicate, work,
                                    )
                                } else {
                                    evaluate_prefix_candidate(index, &schema, &predicate, work)
                                };
                                if exact == 0 || attempt * exact <= limit {
                                    assert_eq!(
                                        prefix_len(result.unwrap()),
                                        if branch {
                                            Err(Reason::MissingContiguousPrefixOrRange)
                                        } else {
                                            Ok(1)
                                        }
                                    );
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
}

#[test]
fn equality_score_hint_never_hides_exhausted_evaluation() {
    let schema = exact_metadata_schema(&[("a", &["age", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let hint = AccessCandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: false,
    };
    for predicate in [
        Predicate::And(vec![equal("age", 1)]),
        Predicate::And(vec![Predicate::True]),
    ] {
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let result = chosen_score_for_visible_indexes(
                AccessChoiceFamily::Prefix,
                hint,
                "a",
                indexes,
                &schema,
                Some(&predicate),
                None,
                None,
                work,
            );
            assert!(
                QueryError::execute(result.unwrap_err())
                    .diagnostic_facts()
                    .contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::TemporaryBytes.raw()
                    ))
            );
            Ok(())
        })
        .unwrap();
    }
    let root = request(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        assert_eq!(
            chosen_score_for_visible_indexes(
                AccessChoiceFamily::Prefix,
                hint,
                "a",
                indexes,
                &schema,
                Some(&Predicate::And(vec![Predicate::True])),
                None,
                None,
                work
            )
            .unwrap(),
            hint
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn equality_prefix_expression_duplicates_compare_lowered_values() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let index = indexes
        .iter()
        .find(|index| index.name() == "a_lower")
        .unwrap();
    for (left, right, equal) in [
        ("Name", "name", true),
        ("İΣ", "i\u{307}ς", true),
        ("İ", "i", false),
    ] {
        let predicate = Predicate::And(
            [left, right]
                .map(|value| {
                    Predicate::Compare(ComparePredicate::with_coercion(
                        "name",
                        CompareOp::Eq,
                        Value::Text(value.into()),
                        CoercionId::TextCasefold,
                    ))
                })
                .into(),
        );
        let root = request(Resource::NestedValueSteps, 2);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                prefix_len(evaluate_prefix_candidate(index, &schema, &predicate, work).unwrap()),
                if equal {
                    Ok(1)
                } else {
                    Err(Reason::ConflictingEqConstraints)
                },
            );
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn equality_payload_exhaustion_cannot_become_a_conflict_or_score_hint() {
    let schema = exact_metadata_schema(&[("a", &["age", "id"])], &[]);
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let hint = AccessCandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: false,
    };
    for second in [1, 2] {
        let predicate = Predicate::And(vec![equal("age", 1), equal("age", second)]);
        let root = request(Resource::NestedValueSteps, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let error = chosen_score_for_visible_indexes(
                AccessChoiceFamily::Prefix,
                hint,
                "a",
                visible.accepted_semantic_index_contracts(),
                &schema,
                Some(&predicate),
                None,
                None,
                work,
            )
            .unwrap_err();
            assert!(QueryError::execute(error).diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::NestedValueSteps.raw(),
            )));
            Ok(())
        })
        .unwrap();
    }
}
