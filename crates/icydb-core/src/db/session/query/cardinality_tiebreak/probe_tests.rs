use super::{
    CardinalityTiebreakAttempt, PreparedCardinalityCandidate, admitted_cardinality_probe_count,
    cardinality_probe_keys_equal, prepare_cardinality_candidates,
};
use crate::db::{
    RequestExecutionRoot,
    access::LoweredIndexPrefixSpec,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    index::UserIndexPrefixCardinalityKey,
    query::preparation::{PreparationWork, with_preparation_work},
    session::tests::cardinality_tiebreak::{
        probe_candidates_for_tests, ranking_candidates_for_tests,
    },
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::{hint::black_box, time::Instant};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn excessive_probes_fall_back_before_output_allocation() {
    for width in [17, 128] {
        let (session, authority, candidates) = probe_candidates_for_tests(width);
        let root = request(Resource::TemporaryBytes, 0);
        let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            session.cardinality_tiebreak_attempt(&authority, candidates, work)
        })
        .unwrap();
        assert!(matches!(result, CardinalityTiebreakAttempt::PolicyFallback));
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn probe_lowering_exhaustion_is_not_a_policy_fallback() {
    let (session, authority, candidates) = ranking_candidates_for_tests();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        // Shape admission and candidate visit succeed; the lowering visit fails.
        let root = request(Resource::PredicateExpressionSteps, 2);
        let result = PreparationWork::run(&root.scope(), lane, |work| {
            session.cardinality_tiebreak_attempt(&authority, vec![candidates[0].clone()], work)
        });
        let Err(error) = result else {
            panic!("lowering exhaustion must reject, not fall back");
        };
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        )));
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 3);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn prelowering_count_policy_preserves_exact_and_first_excess_limits() {
    let (_, _, candidates) = probe_candidates_for_tests(16);
    with_preparation_work(|work| {
        assert_eq!(
            admitted_cardinality_probe_count(&vec![candidates[0].clone(); 16], work).unwrap(),
            Some(256)
        );
        assert_eq!(
            admitted_cardinality_probe_count(&vec![candidates[0].clone(); 17], work).unwrap(),
            None
        );
    });
    let (_, _, candidates) = ranking_candidates_for_tests();
    with_preparation_work(|work| {
        assert_eq!(
            admitted_cardinality_probe_count(&vec![candidates[0].clone(); 64], work).unwrap(),
            Some(64)
        );
        assert_eq!(
            admitted_cardinality_probe_count(&vec![candidates[0].clone(); 65], work).unwrap(),
            None
        );
    });
}

#[test]
fn probe_preparation_charges_exact_backing_and_bookkeeping() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let count = candidates.len() as u64;
    let costs = [
        (
            Resource::TemporaryBytes,
            count
                * (size_of::<PreparedCardinalityCandidate>()
                    + size_of::<UserIndexPrefixCardinalityKey>()
                    + 4 * size_of::<LoweredIndexPrefixSpec>()
                    + size_of::<crate::db::index::EncodedValue>()) as u64,
        ),
        (
            Resource::PredicateExpressionSteps,
            6 * count + count * (count - 1) / 2,
        ),
    ];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, exact) in costs {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    prepare_cardinality_candidates(
                        authority.entity_tag(),
                        authority.accepted_schema_info().unwrap(),
                        candidates.clone(),
                        work,
                    )
                });
                if limit == exact {
                    let (prepared, keys) = result.unwrap().unwrap();
                    assert_eq!(keys.len(), candidates.len());
                    for (slot, item) in prepared.iter().enumerate() {
                        assert_eq!((item.probe_start, item.probe_end), (slot, slot + 1));
                        assert_eq!(item.candidate.access(), candidates[slot].access());
                    }
                } else {
                    let Err(error) = result else {
                        panic!("budget exhaustion must reject");
                    };
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn duplicate_probe_comparison_preserves_identity_and_budget_errors() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let id = candidates[0]
        .route_pin(authority.entity_tag())
        .unwrap()
        .index_id();
    let left = UserIndexPrefixCardinalityKey::new(id, vec![vec![1, 2, 3, 4]]);
    with_preparation_work(|work| {
        for components in [
            vec![],
            vec![vec![]],
            vec![vec![1, 2, 3, 4]],
            vec![vec![1, 2, 3, 5]],
            vec![vec![1], vec![2]],
        ] {
            let right = UserIndexPrefixCardinalityKey::new(id, components);
            assert_eq!(
                cardinality_probe_keys_equal(&left, &right, work).unwrap(),
                left == right
            );
        }
        assert!(
            prepare_cardinality_candidates(
                authority.entity_tag(),
                authority.accepted_schema_info().unwrap(),
                vec![candidates[0].clone(); 2],
                work
            )
            .unwrap()
            .is_none()
        );
    });
    for limit in [6, 7] {
        let root = request(Resource::PredicateExpressionSteps, limit);
        let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            cardinality_probe_keys_equal(&left, &left, work)
        });
        if limit == 7 {
            assert!(result.unwrap());
        } else {
            assert!(result.is_err());
        }
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 7);
    }
}

// Include owned-input cleanup, but exclude input cloning and schema setup.
#[test]
#[ignore = "manual native cardinality probe preparation timing"]
fn cardinality_probe_preparation_native_timing() {
    const ITERATIONS: usize = 500;
    for width in [16, 17, 128] {
        let (_, authority, candidates) = probe_candidates_for_tests(width);
        for sample in 0..5 {
            let inputs: Vec<_> = (0..ITERATIONS).map(|_| candidates.clone()).collect();
            let elapsed = with_preparation_work(|work| {
                let start = Instant::now();
                for input in inputs {
                    let result = prepare_cardinality_candidates(
                        authority.entity_tag(),
                        authority.accepted_schema_info().unwrap(),
                        input,
                        work,
                    )
                    .unwrap();
                    assert_eq!(black_box(result).is_some(), width == 16);
                }
                start.elapsed()
            });
            eprintln!(
                "cardinality_probe_preparation width={width} sample={sample} ns/op={}",
                elapsed.as_nanos() / ITERATIONS as u128
            );
        }
    }
}
