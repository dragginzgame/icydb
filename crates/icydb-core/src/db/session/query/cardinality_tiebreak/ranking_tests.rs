use super::{
    CardinalityTiebreakAttempt, PreparedCardinalityCandidate, rank_prepared_cardinality_candidates,
};
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::{
            plan::{CardinalityTiebreakCandidate, CardinalityTiebreakCandidateEvidence},
            preparation::{PreparationWork, with_preparation_work},
        },
        session::tests::cardinality_tiebreak::ranking_candidates_for_tests,
    },
    types::EntityTag,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::{hint::black_box, time::Instant};

fn prepared(candidates: &[CardinalityTiebreakCandidate]) -> Vec<PreparedCardinalityCandidate> {
    candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| PreparedCardinalityCandidate {
            candidate: candidate.clone(),
            probe_start: index,
            probe_end: index + 1,
        })
        .collect()
}

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

// One visit, prefix addition, name-copy visit and comparison per candidate;
// the first candidate skips comparison, and the final route pin adds one visit.
fn exact_costs(candidates: &[CardinalityTiebreakCandidate]) -> [(Resource, u64); 2] {
    let names = candidates
        .iter()
        .map(|candidate| candidate.index().name().len())
        .sum::<usize>();
    [
        (
            Resource::PredicateExpressionSteps,
            (4 * candidates.len() + names) as u64,
        ),
        (
            Resource::TemporaryBytes,
            (candidates.len() * size_of::<CardinalityTiebreakCandidateEvidence>() + names) as u64,
        ),
    ]
}

#[test]
fn exact_evidence_preserves_first_minimum_and_candidate_order() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    for (counts, winner) in [([12, 6, 6], 1), ([0, 0, 0], 0), ([12, 6, 0], 2)] {
        let (selected, evidence) = with_preparation_work(|work| {
            rank_prepared_cardinality_candidates(
                authority.entity_tag(),
                prepared(&candidates),
                &counts,
                work,
            )
        })
        .unwrap()
        .unwrap();
        assert_eq!(selected.access(), candidates[winner].access());
        assert_eq!(
            Some(evidence.route_pin()),
            candidates[winner].route_pin(authority.entity_tag())
        );
        let actual: Vec<_> = evidence
            .candidates()
            .iter()
            .map(|item| (item.index_name(), item.exact_prefix_entries()))
            .collect();
        let expected: Vec<_> = candidates
            .iter()
            .zip(counts)
            .map(|(candidate, count)| (candidate.index().name(), count))
            .collect();
        assert_eq!(actual, expected);
    }
}

#[test]
fn exact_evidence_sums_prefixes_and_preserves_unavailable_results() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let multi = || {
        let mut inputs = prepared(&candidates);
        inputs[0].probe_end = 2;
        inputs[1].probe_start = 2;
        inputs[1].probe_end = 3;
        inputs[2].probe_start = 3;
        inputs[2].probe_end = 4;
        inputs
    };
    with_preparation_work(|work| {
        let (selected, evidence) = rank_prepared_cardinality_candidates(
            authority.entity_tag(),
            multi(),
            &[2, 3, 5, 6],
            work,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.access(), candidates[0].access());
        assert_eq!(
            evidence
                .candidates()
                .iter()
                .map(CardinalityTiebreakCandidateEvidence::exact_prefix_entries)
                .collect::<Vec<_>>(),
            [5, 5, 6]
        );
        for counts in [&[u64::MAX, 1, 5, 6][..], &[1, 2][..]] {
            assert!(
                rank_prepared_cardinality_candidates(authority.entity_tag(), multi(), counts, work)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            rank_prepared_cardinality_candidates(authority.entity_tag(), Vec::new(), &[], work)
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn exact_evidence_charges_exact_limits_and_cumulative_retries() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (resource, exact) in exact_costs(&candidates) {
            for limit in [exact - 1, exact] {
                let root = request(resource, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    rank_prepared_cardinality_candidates(
                        authority.entity_tag(),
                        prepared(&candidates),
                        &[12, 6, 6],
                        work,
                    )
                });
                if limit == exact {
                    assert!(result.unwrap().is_some());
                } else {
                    assert!(
                        result
                            .unwrap_err()
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
            let root = request(resource, 2 * exact - 1);
            for attempt in 0..2 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    rank_prepared_cardinality_candidates(
                        authority.entity_tag(),
                        prepared(&candidates),
                        &[12, 6, 6],
                        work,
                    )
                });
                assert_eq!(result.is_ok(), attempt == 0);
            }
            assert_eq!(root.observed(resource), 2 * exact);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn exact_cardinality_attempt_propagates_budget_failure_instead_of_policy_fallback() {
    let (session, authority, candidates) = ranking_candidates_for_tests();
    for (resource, exact) in exact_costs(&candidates) {
        // This fixture has one component/probe per distinct physical index.
        let count = candidates.len() as u64;
        let preparation = match resource {
            Resource::PredicateExpressionSteps => 6 * count + count * (count - 1) / 2,
            Resource::TemporaryBytes => {
                count
                    * (size_of::<PreparedCardinalityCandidate>()
                        + size_of::<crate::db::index::UserIndexPrefixCardinalityKey>()
                        + 4 * size_of::<crate::db::access::LoweredIndexPrefixSpec>()
                        + size_of::<crate::db::index::EncodedValue>()) as u64
            }
            _ => unreachable!("fixture costs"),
        };
        let exact = exact + preparation;
        for limit in [exact - 1, exact] {
            let root = request(resource, limit);
            let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                session.cardinality_tiebreak_attempt(&authority, candidates.clone(), work)
            });
            if limit == exact {
                assert!(matches!(
                    result,
                    Ok(CardinalityTiebreakAttempt::Exact { .. })
                ));
            } else {
                let Err(error) = result else {
                    panic!("budget exhaustion must reject, not fall back");
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

// Input construction and accepted-schema setup are outside the timed region.
// This measures evidence construction only, not planner or storage costs.
#[test]
#[ignore = "manual native exact-cardinality evidence timing"]
fn exact_cardinality_evidence_native_timing() {
    const ITERATIONS: usize = 2_000;
    let (_, _, candidates) = ranking_candidates_for_tests();
    assert_eq!(candidates.len(), 3);
    let counts = [12, 6, 6];
    for sample in 0..5 {
        let inputs: Vec<_> = (0..ITERATIONS).map(|_| prepared(&candidates)).collect();
        let elapsed = with_preparation_work(|work| {
            let started = Instant::now();
            for input in inputs {
                black_box(rank_prepared_cardinality_candidates(
                    EntityTag::new(219),
                    input,
                    black_box(&counts),
                    work,
                ))
                .unwrap()
                .unwrap();
            }
            started.elapsed()
        });
        eprintln!(
            "exact_cardinality_evidence sample={sample} ns/op={}",
            elapsed.as_nanos() / ITERATIONS as u128,
        );
    }
}
