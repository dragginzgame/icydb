//! Multi-lookup diagnostics charge visits without copying or converting operands.

use super::evaluate_multi_lookup_candidate_from_contract;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            plan::{
                VisibleIndexes,
                access_choice::model::{AccessChoiceRejectedReason as Reason, CandidateEvaluation},
                planner::prefix_test_schema,
            },
            preparation::PreparationWork,
        },
    },
    value::Value,
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
fn multi_lookup_diagnostic_visits_are_exact_cumulative_and_borrowed() {
    let schema = prefix_test_schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    for index in visible.accepted_semantic_index_contracts() {
        let coercion = if index.name().ends_with("lower") {
            CoercionId::TextCasefold
        } else {
            CoercionId::Strict
        };
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(vec![Value::Text("İ".repeat(64)); 3]),
            coercion,
        ));
        let before = predicate.clone();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for limit in [0, 2, 6] {
                let root = request(limit);
                PreparationWork::run(&root.scope(), lane, |budget| {
                    for attempt in 1..=3 {
                        let result = evaluate_multi_lookup_candidate_from_contract(
                            index, &schema, &predicate, budget,
                        );
                        if attempt * 3 <= limit {
                            let CandidateEvaluation::Eligible(score) = result.unwrap() else {
                                panic!("eligible multi-lookup")
                            };
                            assert_eq!(score.prefix_len, 1);
                            assert!(score.exact);
                            assert_eq!(
                                root.observed(Resource::PredicateExpressionSteps),
                                attempt * 3
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
                assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                assert_eq!(root.observed(Resource::NestedValueSteps), 0);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(predicate, before);
            }
        }
    }
}

#[test]
fn multi_lookup_diagnostic_rejections_keep_precedence_and_short_circuits() {
    let schema = prefix_test_schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let index = visible
        .accepted_semantic_index_contracts()
        .iter()
        .find(|index| index.name() == "a_raw")
        .unwrap();
    let compare = |field, op, value, coercion| {
        Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion))
    };
    for (predicate, reason, visits) in [
        (Predicate::True, Reason::PredicateShapeNotMultiLookup, 0),
        (
            compare("name", CompareOp::Eq, Value::Null, CoercionId::NumericWiden),
            Reason::NonStrictCoercion,
            0,
        ),
        (
            compare("name", CompareOp::Eq, Value::Null, CoercionId::Strict),
            Reason::OperatorNotMultiLookupIn,
            0,
        ),
        (
            compare("missing", CompareOp::In, Value::Null, CoercionId::Strict),
            Reason::LeadingFieldMismatch,
            0,
        ),
        (
            compare("name", CompareOp::In, Value::Null, CoercionId::Strict),
            Reason::InLiteralNotList,
            0,
        ),
        (
            compare(
                "name",
                CompareOp::In,
                Value::List(vec![]),
                CoercionId::Strict,
            ),
            Reason::InLiteralEmpty,
            0,
        ),
        (
            compare(
                "name",
                CompareOp::In,
                Value::List(vec![
                    Value::Text("first".into()),
                    Value::Nat64(1),
                    Value::Text("later".into()),
                ]),
                CoercionId::Strict,
            ),
            Reason::InLiteralIncompatible,
            2,
        ),
    ] {
        let root = request(visits);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |budget| {
            assert_eq!(
                evaluate_multi_lookup_candidate_from_contract(index, &schema, &predicate, budget)
                    .unwrap(),
                CandidateEvaluation::Rejected(reason)
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), visits);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn multi_lookup_score_hint_cannot_hide_exhausted_visits() {
    use crate::db::query::plan::{
        access_choice::{chosen_score_for_visible_indexes, model::AccessChoiceFamily},
        planner::AccessCandidateScore,
    };

    let schema = prefix_test_schema();
    let visible = VisibleIndexes::accepted_schema_visible(&schema).unwrap();
    let indexes = visible.accepted_semantic_index_contracts();
    let predicate = Predicate::in_("name".into(), vec![Value::Text("a".into()); 3]);
    let hint = AccessCandidateScore {
        prefix_len: 0,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: false,
    };
    let baseline = request(16_000_000);
    let expected = PreparationWork::run(&baseline.scope(), Lane::Diagnostic, |budget| {
        chosen_score_for_visible_indexes(
            AccessChoiceFamily::MultiLookup,
            hint,
            "a_raw",
            indexes,
            &schema,
            Some(&predicate),
            None,
            None,
            budget,
        )
        .map_err(QueryError::execute)
    })
    .unwrap();
    assert_ne!(expected, hint);
    let visits = baseline.observed(Resource::PredicateExpressionSteps);
    let short = request(visits - 1);
    let result = PreparationWork::run(&short.scope(), Lane::Diagnostic, |budget| {
        chosen_score_for_visible_indexes(
            AccessChoiceFamily::MultiLookup,
            hint,
            "a_raw",
            indexes,
            &schema,
            Some(&predicate),
            None,
            None,
            budget,
        )
        .map_err(QueryError::execute)
    });
    assert!(result.unwrap_err().diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw()
    )));
    assert_eq!(short.observed(Resource::TemporaryBytes), 0);
    assert_eq!(short.observed(Resource::RowsVisited), 0);
}
