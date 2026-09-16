//! Range scoring borrows validated input and keeps only fixed-size field facts.

use super::evaluate_range_candidate_from_contract;
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
                planner::prefix_test_schema,
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

fn compare(field: &str, op: CompareOp, value: i64) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::Int64(value),
        CoercionId::Strict,
    ))
}

const fn score(evaluation: CandidateEvaluation) -> Result<(usize, bool, u8), Reason> {
    match evaluation {
        CandidateEvaluation::Eligible(score) => {
            Ok((score.prefix_len, score.exact, score.range_bound_count))
        }
        CandidateEvaluation::Rejected(reason) => Err(reason),
    }
}

#[test]
fn range_scores_keep_gaps_conflicts_and_full_input_rejection_precedence() {
    let schema = exact_metadata_schema(&[("a", &["age", "rank", "id"])], &[]);
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let index = &visible.accepted_semantic_index_contracts()[0];
    let eq = compare("age", CompareOp::Eq, 1);
    let lower = compare("rank", CompareOp::Gt, 2);
    for (children, expected) in [
        (vec![], Err(Reason::PredicateShapeNotRangeEligible)),
        (vec![eq.clone(), lower.clone()], Ok((1, false, 1))),
        (
            vec![
                eq.clone(),
                lower.clone(),
                compare("rank", CompareOp::Lte, 8),
            ],
            Ok((1, false, 2)),
        ),
        (vec![lower], Err(Reason::MissingContiguousPrefixOrRange)),
        (
            vec![
                eq.clone(),
                compare("rank", CompareOp::Eq, 2),
                compare("id", CompareOp::Eq, 3),
            ],
            Err(Reason::MissingRangeConstraint),
        ),
        (
            vec![
                compare("age", CompareOp::Gt, 1),
                compare("rank", CompareOp::Eq, 2),
            ],
            Err(Reason::NonContiguousRangeConstraints),
        ),
        (
            vec![eq.clone(), compare("age", CompareOp::Gt, 1)],
            Err(Reason::EqRangeConflict),
        ),
        (
            vec![compare("age", CompareOp::Gt, 1), eq.clone()],
            Err(Reason::EqRangeConflict),
        ),
        (
            vec![eq.clone(), compare("age", CompareOp::Eq, 2)],
            Err(Reason::ConflictingEqConstraints),
        ),
        (
            vec![
                eq.clone(),
                compare("age", CompareOp::Eq, 2),
                Predicate::True,
            ],
            Err(Reason::PredicateShapeNotRangeEligible),
        ),
        (
            vec![
                eq.clone(),
                compare("age", CompareOp::Eq, 2),
                compare("rank", CompareOp::Ne, 3),
            ],
            Err(Reason::OperatorNotRangeSupported),
        ),
        (
            vec![
                eq,
                Predicate::Compare(ComparePredicate::with_coercion(
                    "rank",
                    CompareOp::Gt,
                    Value::Int64(2),
                    CoercionId::NumericWiden,
                )),
            ],
            Err(Reason::NonStrictCoercion),
        ),
    ] {
        let predicate = Predicate::And(children);
        let before = predicate.clone();
        let root = request(Resource::TemporaryBytes, 0);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            assert_eq!(
                score(
                    evaluate_range_candidate_from_contract(index, &schema, &predicate, work)
                        .unwrap()
                ),
                expected
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(predicate, before);
        let compared_nodes = if expected == Err(Reason::ConflictingEqConstraints) {
            2
        } else {
            0
        };
        assert_eq!(root.observed(Resource::NestedValueSteps), compared_nodes);
    }
}

#[test]
fn range_equality_matching_borrows_raw_values_and_admits_conversion_cumulatively() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    let text = "İΣ".repeat(128);
    let (lower_bytes, lower_steps) = lower_text_construction_allowance(text.len());
    for expression in [false, true] {
        let index = indexes
            .iter()
            .find(|index| index.name() == if expression { "a_lower" } else { "a_raw" })
            .unwrap();
        let cmp = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text(text.clone()),
            if expression {
                CoercionId::TextCasefold
            } else {
                CoercionId::Strict
            },
        ));
        let predicate = Predicate::And(vec![cmp.clone(), cmp]);
        let bytes = if expression { 2 * lower_bytes } else { 0 };
        let compared_bytes = if expression {
            text.to_lowercase().len()
        } else {
            text.len()
        };
        let steps = 5 + if expression { 2 * lower_steps } else { 0 } + compared_bytes as u64;
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
                            let result = evaluate_range_candidate_from_contract(
                                index, &schema, &predicate, work,
                            );
                            if exact == 0 || attempt * exact <= limit {
                                assert_eq!(
                                    score(result.unwrap()),
                                    Err(Reason::MissingRangeConstraint)
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

#[test]
fn range_expression_equality_uses_canonical_values_and_retains_prefix_bound_strength() {
    let schema = prefix_test_schema();
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema).expect("valid accepted index fixture");
    let indexes = visible.accepted_semantic_index_contracts();
    for expression in [false, true] {
        let index = indexes
            .iter()
            .find(|index| index.name() == if expression { "a_lower" } else { "a_raw" })
            .unwrap();
        let coercion = if expression {
            CoercionId::TextCasefold
        } else {
            CoercionId::Strict
        };
        let text_compare = |op, value: &str| {
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                op,
                Value::Text(value.into()),
                coercion,
            ))
        };
        let cases = [
            (
                vec![
                    text_compare(CompareOp::Eq, "İΣ"),
                    text_compare(CompareOp::Eq, "i\u{307}ς"),
                ],
                Err(if expression {
                    Reason::MissingRangeConstraint
                } else {
                    Reason::ConflictingEqConstraints
                }),
            ),
            (
                vec![text_compare(CompareOp::StartsWith, "İΣ")],
                Ok((0, false, if expression { 1 } else { 2 })),
            ),
            (
                vec![
                    text_compare(CompareOp::StartsWith, "İΣ"),
                    text_compare(CompareOp::Gt, "A"),
                    text_compare(CompareOp::Lt, "Z"),
                ],
                Ok((0, false, if expression { 1 } else { 2 })),
            ),
            (
                vec![
                    text_compare(CompareOp::Gt, "A"),
                    text_compare(CompareOp::Lt, "Z"),
                ],
                Ok((0, false, 2)),
            ),
            (
                vec![text_compare(CompareOp::StartsWith, "")],
                Err(Reason::StartsWithPrefixInvalid),
            ),
        ];
        for (children, expected) in cases {
            // Only the duplicate equality case compares operands; range facts
            // are classified without ordering the bounds in this diagnostic owner.
            let compared_nodes = if matches!(
                expected,
                Err(Reason::MissingRangeConstraint | Reason::ConflictingEqConstraints)
            ) {
                2
            } else {
                0
            };
            let root = request(Resource::NestedValueSteps, compared_nodes);
            PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
                assert_eq!(
                    score(
                        evaluate_range_candidate_from_contract(
                            index,
                            &schema,
                            &Predicate::And(children),
                            work
                        )
                        .unwrap()
                    ),
                    expected
                );
                Ok(())
            })
            .unwrap();
            assert_eq!(root.observed(Resource::NestedValueSteps), compared_nodes);
        }
    }
}
