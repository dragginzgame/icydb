//! Stable reductions admit moved and revisited predicate shells.

use super::compare;
use crate::db::{
    predicate::{
        CompareOp, Predicate,
        simplify::{remove_predicate, simplify_and_compare_constraints},
    },
    query::preparation::PreparationWork,
    tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn all_reduction_branches_admit_stable_compaction_with_typed_cumulative_limits() {
    let before = Predicate::IsNull {
        field: "before".into(),
    };
    let middle = Predicate::IsNull {
        field: "middle".into(),
    };
    let after = Predicate::IsNull {
        field: "after".into(),
    };
    for (left, right, retained, keeps_first, amount) in [
        // Pair/domain work, shifted suffixes, and skipped guard-shell visits.
        (CompareOp::Gt, CompareOp::Gte, CompareOp::Gt, true, 11),
        (CompareOp::Gte, CompareOp::Gt, CompareOp::Gt, false, 13),
        (CompareOp::Gte, CompareOp::Lte, CompareOp::Eq, true, 13),
        (CompareOp::Lte, CompareOp::Gte, CompareOp::Eq, false, 14),
    ] {
        let input = vec![
            before.clone(),
            compare("rank", left, 7),
            middle.clone(),
            compare("rank", right, 7),
            after.clone(),
        ];
        let mut expected = vec![before.clone()];
        if keeps_first {
            expected.extend([compare("rank", retained, 7), middle.clone()]);
        } else {
            expected.extend([middle.clone(), compare("rank", retained, 7)]);
        }
        expected.push(after.clone());
        assert_boundary(&input, &expected, amount);
    }
}

fn assert_boundary(input: &[Predicate], expected: &[Predicate], amount: u64) {
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        for limit in [amount - 1, amount, amount * 2] {
            let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
            for attempt in 1..=3 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    simplify_and_compare_constraints(input.to_vec(), &mut |resource, amount| {
                        work.charge(resource, amount)
                    })
                });
                assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                assert_eq!(root.observed(Resource::SortEntries), 0);
                if attempt * amount <= limit {
                    assert_eq!(result.unwrap().as_deref(), Some(expected));
                    assert_eq!(
                        root.observed(Resource::PredicateExpressionSteps),
                        attempt * amount
                    );
                } else {
                    let error = result.unwrap_err();
                    let actual = root.observed(Resource::PredicateExpressionSteps);
                    assert!(actual > limit && actual <= attempt * amount);
                    for fact in [
                        (
                            DiagnosticFactTag::BudgetResource,
                            Resource::PredicateExpressionSteps.raw(),
                        ),
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

#[test]
fn repeated_duplicate_removal_accounts_for_every_shifted_suffix() {
    for count in [2_u64, 4, 64] {
        let leaf = compare("rank", CompareOp::Eq, 7);
        let input = vec![leaf.clone(); usize::try_from(count).unwrap()];
        let pairs = count - 1;
        let shifted = (count - 2) * (count - 1) / 2;
        assert_boundary(&input, &[leaf], pairs * 5 + shifted);
    }
}

#[test]
fn noncomparison_scans_admit_each_shell_without_pair_or_payload_work() {
    for count in [1, 2, 64] {
        let input = vec![
            Predicate::IsNull {
                field: "guard".into()
            };
            count
        ];
        assert_boundary(&input, &input, count as u64);
    }
    let input = vec![
        Predicate::IsNull {
            field: "guard".into(),
        },
        compare("rank", CompareOp::Gte, 7),
        compare("rank", CompareOp::Lte, 7),
    ];
    let root = request_with_limit(Resource::PredicateExpressionSteps, 0);
    let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        simplify_and_compare_constraints(input, &mut |resource, amount| {
            work.charge(resource, amount)
        })
    })
    .unwrap_err();
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::Actual, 1))
    );
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
    assert_eq!(root.observed(Resource::NestedValueSteps), 0);
}

#[test]
fn replacement_revisits_admit_each_noncomparison_prefix() {
    for (left, right) in [
        (CompareOp::Gte, CompareOp::Lte),
        (CompareOp::Lte, CompareOp::Gte),
    ] {
        for replacements in [1, 2, 4] {
            let comparisons = (0..replacements)
                .flat_map(|index| {
                    let field = format!("field{index}");
                    [compare(&field, left, 7), compare(&field, right, 7)]
                })
                .collect::<Vec<_>>();
            let baseline = request_with_limit(Resource::PredicateExpressionSteps, u64::MAX);
            let reduced = PreparationWork::run(&baseline.scope(), Lane::PublicRead, |work| {
                simplify_and_compare_constraints(comparisons.clone(), &mut |resource, amount| {
                    work.charge(resource, amount)
                })
            })
            .unwrap()
            .unwrap();
            for width in [1, 2, 64] {
                let mut input = vec![
                    Predicate::IsNull {
                        field: "guard".into()
                    };
                    width
                ];
                let mut expected = input.clone();
                input.extend(comparisons.clone());
                expected.extend(reduced.clone());
                // Prefix guards are never shifted by these suffix removals.
                // Every replacement revisits the entire prefix once, in
                // addition to its initial pass. Pair/payload work is unchanged.
                let visits = (width * (replacements + 1)) as u64;
                let amount = baseline.observed(Resource::PredicateExpressionSteps) + visits;
                assert_boundary(&input, &expected, amount);
            }
        }
    }
}

#[test]
fn compaction_refusal_preserves_the_owned_vector_and_tail_removal_is_free() {
    let input = vec![compare("rank", CompareOp::Gte, 7); 4];
    for limit in [1, 2] {
        let mut children = input.clone();
        let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
        let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            remove_predicate(&mut children, 1, &mut |resource, amount| {
                work.charge(resource, amount)
            })
        });
        if limit == 2 {
            result.unwrap();
            assert_eq!(children, input[..3]);
        } else {
            let error = result.unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::Actual, 2))
            );
            assert_eq!(children, input);
        }
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);
    }
    let mut children = input.clone();
    let root = request_with_limit(Resource::PredicateExpressionSteps, 0);
    PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        remove_predicate(&mut children, 3, &mut |resource, amount| {
            work.charge(resource, amount)
        })
    })
    .unwrap();
    assert_eq!(children, input[..3]);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
}

#[test]
fn shift_refusal_stops_before_later_pair_work_or_partial_output() {
    let input = vec![
        compare("rank", CompareOp::Gt, 7),
        compare("rank", CompareOp::Gte, 7),
        Predicate::IsNull {
            field: "later".into(),
        },
    ];
    let root = request_with_limit(Resource::PredicateExpressionSteps, 5);
    let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
        simplify_and_compare_constraints(input, &mut |resource, amount| {
            work.charge(resource, amount)
        })
    })
    .unwrap_err();
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::Actual, 6))
    );
    assert_eq!(root.observed(Resource::NestedValueSteps), 2);
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 6);
}
