//! Streaming reason evidence preserves the maintained diagnostic precedence.

use super::{CandidateRankingEvidence, ranked_rejection_reason};
use crate::db::query::plan::access_choice::model::{
    AccessChoiceFamily as Family, AccessChoiceRankingReason as Reason,
    AccessChoiceRejectedReason as Rejected, AccessChoiceSelectedReason as Selected, CandidateScore,
};

// Independent policy table: precedence is not the enum's declaration order.
fn expected_reason(reasons: &[Reason], residual: bool) -> Reason {
    [
        Reason::ExactMatchPreferred,
        Reason::FilteredPredicatePreferred,
        Reason::StrongerRangeBoundsPreferred,
        Reason::ResidualBurdenPreferred,
        Reason::OrderCompatiblePreferred,
        Reason::LexicographicTiebreak,
    ]
    .into_iter()
    .find(|reason| {
        reasons.contains(reason)
            || (*reason == Reason::ResidualBurdenPreferred && residual)
            || *reason == Reason::LexicographicTiebreak
    })
    .unwrap()
}

#[test]
fn streaming_reasons_preserve_family_precedence_prefix_and_residual_rules() {
    let chosen = CandidateScore::new(2, true, true, 2, true);
    for family in [
        Family::NonIndex,
        Family::Prefix,
        Family::MultiLookup,
        Family::BranchSet,
        Family::Range,
    ] {
        let indexed = !matches!(family, Family::NonIndex);
        let exact_family = matches!(
            family,
            Family::Prefix | Family::MultiLookup | Family::BranchSet
        );
        let cases = [
            (
                CandidateScore::new(1, false, false, 0, false),
                Reason::LexicographicTiebreak,
            ),
            (chosen, Reason::LexicographicTiebreak),
            (
                CandidateScore::new(2, false, false, 0, false),
                if exact_family {
                    Reason::ExactMatchPreferred
                } else {
                    Reason::LexicographicTiebreak
                },
            ),
            (
                CandidateScore::new(2, true, false, 0, false),
                if indexed {
                    Reason::FilteredPredicatePreferred
                } else {
                    Reason::LexicographicTiebreak
                },
            ),
            (
                CandidateScore::new(2, true, true, 1, false),
                if family == Family::Range {
                    Reason::StrongerRangeBoundsPreferred
                } else {
                    Reason::LexicographicTiebreak
                },
            ),
            (
                CandidateScore::new(2, true, true, 2, false),
                if indexed {
                    Reason::OrderCompatiblePreferred
                } else {
                    Reason::LexicographicTiebreak
                },
            ),
            (
                CandidateScore::new(3, false, false, 0, false),
                Reason::LexicographicTiebreak,
            ),
        ];
        for residual in [false, true] {
            assert_eq!(
                CandidateRankingEvidence::new().selected_reason(chosen, residual),
                Selected::SingleCandidate
            );
            for (first, first_reason) in cases {
                assert_eq!(
                    ranked_rejection_reason(family, first, chosen, residual),
                    if first.prefix_len < chosen.prefix_len {
                        Rejected::ShorterPrefix
                    } else {
                        Rejected::Ranked(expected_reason(&[first_reason], residual))
                    }
                );
                for (second, second_reason) in cases {
                    let expected = if first.prefix_len.max(second.prefix_len) < chosen.prefix_len {
                        Selected::BestPrefixLen
                    } else {
                        Selected::Ranked(expected_reason(&[first_reason, second_reason], residual))
                    };
                    for scores in [[first, second, first], [second, first, second]] {
                        let mut evidence = CandidateRankingEvidence::new();
                        for score in scores {
                            evidence.observe(family, chosen, score);
                        }
                        assert_eq!(evidence.selected_reason(chosen, residual), expected);
                    }
                }
            }
        }
    }
}
