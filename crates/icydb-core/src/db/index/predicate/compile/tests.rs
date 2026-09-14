//! Both compiler inputs preserve semantic absence and typed admission failures.

use super::*;
use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::{CoercionId, CoercionSpec, IndexCompileTargetKind},
    query::preparation::{PreparationWork, with_preparation_work},
    schema::PersistedIndexExpressionOp,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

const TARGETS: [IndexCompileTarget; 1] = [IndexCompileTarget {
    component_index: 0,
    field_slot: 0,
    kind: IndexCompileTargetKind::Field,
}];
const POLICIES: [IndexCompilePolicy; 2] = [
    IndexCompilePolicy::ConservativeSubset,
    IndexCompilePolicy::StrictAllOrNone,
];

fn compare(op: CompareOp, value: Value) -> ExecutablePredicate {
    ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
        Some(0),
        op,
        value,
        CoercionSpec::new(CoercionId::Strict),
    ))
}

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn expression_conversion_admission_preserves_programs_and_cumulative_limits() {
    let targets = [IndexCompileTarget {
        kind: IndexCompileTargetKind::Expression(PersistedIndexExpressionOp::Lower),
        ..TARGETS[0]
    }];
    for (text, bytes, steps) in [
        ("A".to_string(), 1, 3),
        ("İ".to_string(), 10, 22),
        ("İΣ".to_string(), 12, 36),
        ("İ".repeat(2048), 12_288, 36_864),
    ] {
        for op in [CompareOp::Eq, CompareOp::In, CompareOp::StartsWith] {
            let literal = |text: String| {
                if op == CompareOp::In {
                    Value::List(vec![Value::Text(text.clone()), Value::Text(text)])
                } else {
                    Value::Text(text)
                }
            };
            let predicate =
                ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
                    Some(0),
                    op,
                    literal(text.clone()),
                    CoercionSpec::new(CoercionId::TextCasefold),
                ));
            let canonical = compare(op, literal(text.to_lowercase()));
            let operands = if op == CompareOp::In { 2 } else { 1 };
            for policy in POLICIES {
                let measured = root(Resource::TemporaryBytes, 16_000_000);
                let expected = PreparationWork::run(&measured.scope(), Lane::Diagnostic, |work| {
                    compile_index_program(&canonical, &[0], policy, work)
                        .map_err(QueryError::execute)
                })
                .unwrap();
                assert!(expected.is_some());
                for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                    for (resource, conversion) in [
                        (Resource::TemporaryBytes, bytes),
                        (Resource::PredicateExpressionSteps, steps),
                    ] {
                        let build = |work: &PreparationWork<'_>| {
                            compile_index_program_for_targets(&predicate, &targets, policy, work)
                                .map_err(QueryError::execute)
                        };
                        let rejected = root(resource, conversion - 1);
                        let error =
                            PreparationWork::run(&rejected.scope(), lane, build).unwrap_err();
                        assert!(
                            error
                                .diagnostic_facts()
                                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        if resource == Resource::TemporaryBytes {
                            assert_eq!(rejected.observed(Resource::PredicateExpressionSteps), 0);
                        } else {
                            assert_eq!(rejected.observed(Resource::TemporaryBytes), bytes);
                        }
                        let allowance = measured.observed(resource) + operands * conversion;
                        let request = root(resource, allowance);
                        assert_eq!(
                            PreparationWork::run(&request.scope(), lane, build).unwrap(),
                            expected
                        );
                        assert_eq!(request.observed(resource), allowance);
                        assert!(PreparationWork::run(&request.scope(), lane, build).is_err());
                        assert_eq!(request.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn prefix_compilation_rejects_semantic_output_before_scalar_encoding() {
    let predicate = compare(CompareOp::StartsWith, Value::Text("abc".into()));
    for policy in POLICIES {
        for targets in [false, true] {
            for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                for (resource, limit) in [
                    (Resource::TemporaryBytes, 6),
                    (Resource::PredicateExpressionSteps, 9),
                ] {
                    let request = root(resource, limit);
                    let result = PreparationWork::run(&request.scope(), lane, |work| {
                        if targets {
                            compile_index_program_for_targets(&predicate, &TARGETS, policy, work)
                        } else {
                            compile_index_program(&predicate, &[0], policy, work)
                        }
                        .map_err(QueryError::execute)
                    });
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                    // A failed backing charge does no scan work; a failed scan
                    // charge has only admitted semantic backing, not encoded bytes.
                    if resource == Resource::TemporaryBytes {
                        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
                    } else {
                        assert_eq!(request.observed(Resource::TemporaryBytes), 7);
                    }
                    assert_eq!(request.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn both_compilers_admit_each_encoded_scalar_in_every_read_lane() {
    let samples = [
        (compare(CompareOp::Eq, Value::Nat64(4)), 9, 9),
        (
            compare(
                CompareOp::In,
                Value::List(vec![Value::Nat64(1), Value::Nat64(2)]),
            ),
            18,
            18,
        ),
        (
            compare(
                CompareOp::NotIn,
                Value::List((0..20).map(Value::Nat64).collect()),
            ),
            180,
            180,
        ),
        (
            compare(CompareOp::StartsWith, Value::Text("abc".into())),
            25,
            28,
        ),
        (
            compare(CompareOp::Eq, Value::Text("x".repeat(4096))),
            8195,
            8195,
        ),
    ];
    for (predicate, bytes, steps) in samples {
        for policy in POLICIES {
            let expected =
                with_preparation_work(|work| compile_index_program(&predicate, &[0], policy, work))
                    .unwrap()
                    .unwrap();
            for targets in [false, true] {
                for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
                    for (resource, allowance) in [
                        (Resource::TemporaryBytes, bytes),
                        (Resource::PredicateExpressionSteps, steps),
                    ] {
                        let request = root(resource, allowance * 2 - 1);
                        for invocation in 0..2 {
                            let result = PreparationWork::run(&request.scope(), lane, |work| {
                                if targets {
                                    compile_index_program_for_targets(
                                        &predicate, &TARGETS, policy, work,
                                    )
                                } else {
                                    compile_index_program(&predicate, &[0], policy, work)
                                }
                                .map_err(QueryError::execute)
                            });
                            if invocation == 0 {
                                assert_eq!(result.unwrap().as_ref(), Some(&expected));
                                assert_eq!(request.observed(resource), allowance);
                            } else {
                                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                                    DiagnosticFactTag::BudgetResource,
                                    resource.raw()
                                ),));
                            }
                        }
                        assert_eq!(request.observed(Resource::RowsVisited), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn subset_only_drops_unsupported_conjunctions_never_budget_errors() {
    let leaf = compare(CompareOp::Eq, Value::Nat64(1));
    let unsupported = ExecutablePredicate::IsNull {
        field_slot: Some(0),
    };
    let subset = ExecutablePredicate::And(vec![
        unsupported.clone(),
        ExecutablePredicate::And(vec![leaf.clone(), ExecutablePredicate::True]),
    ]);
    let or = ExecutablePredicate::Or(vec![leaf.clone(), unsupported]);
    let not = ExecutablePredicate::Not(Box::new(subset.clone()));
    for targets in [false, true] {
        let compile = |predicate: &ExecutablePredicate, policy, budget: &dyn ConstructionBudget| {
            if targets {
                compile_index_program_for_targets(predicate, &TARGETS, policy, budget)
            } else {
                compile_index_program(predicate, &[0], policy, budget)
            }
        };
        with_preparation_work(|work| {
            let expected = compile(&leaf, IndexCompilePolicy::StrictAllOrNone, work).unwrap();
            assert_eq!(
                compile(&subset, IndexCompilePolicy::ConservativeSubset, work).unwrap(),
                expected
            );
            for predicate in [&subset, &or, &not] {
                assert!(
                    compile(predicate, IndexCompilePolicy::StrictAllOrNone, work)
                        .unwrap()
                        .is_none()
                );
            }
            for predicate in [&or, &not] {
                assert!(
                    compile(predicate, IndexCompilePolicy::ConservativeSubset, work)
                        .unwrap()
                        .is_none()
                );
            }
            let nested = ExecutablePredicate::Or(vec![
                ExecutablePredicate::And(vec![leaf.clone(), ExecutablePredicate::True]),
                ExecutablePredicate::Not(Box::new(leaf.clone())),
            ]);
            // Strict capability admission intentionally rejects NOT, while
            // conservative translation supports a fully compilable NOT child.
            assert!(matches!(
                compile(&nested, POLICIES[0], work).unwrap(),
                Some(IndexPredicateProgram::Or(_))
            ));
            assert!(compile(&nested, POLICIES[1], work).unwrap().is_none());
        });
        let request = root(Resource::TemporaryBytes, 8);
        let result = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
            compile(&subset, IndexCompilePolicy::ConservativeSubset, work)
                .map_err(QueryError::execute)
        });
        assert!(result.unwrap_err().diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        ),));
        // Strict classification rejects the unsupported tree without encoding.
        let request = root(Resource::TemporaryBytes, 0);
        let result = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            compile(&subset, IndexCompilePolicy::StrictAllOrNone, work).map_err(QueryError::execute)
        });
        assert!(result.unwrap().is_none());
    }
}

#[test]
fn expression_target_programs_match_canonical_field_operands() {
    let targets = [IndexCompileTarget {
        kind: IndexCompileTargetKind::Expression(PersistedIndexExpressionOp::Lower),
        ..TARGETS[0]
    }];
    for op in [CompareOp::Eq, CompareOp::In, CompareOp::StartsWith] {
        let (authored, canonical) = if op == CompareOp::In {
            (
                Value::List(vec![Value::Text("ÄBC".into()), Value::Text("DEF".into())]),
                Value::List(vec![Value::Text("äbc".into()), Value::Text("def".into())]),
            )
        } else {
            (Value::Text("ÄBC".into()), Value::Text("äbc".into()))
        };
        let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(0),
            op,
            authored,
            CoercionSpec::new(CoercionId::TextCasefold),
        ));
        let canonical = compare(op, canonical);
        for policy in POLICIES {
            with_preparation_work(|work| {
                let expected = compile_index_program(&canonical, &[0], policy, work).unwrap();
                assert!(expected.is_some());
                assert_eq!(
                    compile_index_program_for_targets(&predicate, &targets, policy, work).unwrap(),
                    expected
                );
                assert!(
                    compile_index_program_for_targets(&predicate, &TARGETS, policy, work)
                        .unwrap()
                        .is_none()
                );
            });
        }
    }
}
