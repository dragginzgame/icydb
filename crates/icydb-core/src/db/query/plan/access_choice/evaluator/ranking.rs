use crate::db::query::{
    explain::ExplainAccessPath,
    plan::access_choice::model::{
        AccessChoiceFamily, AccessChoiceRankingReason, AccessChoiceRejectedReason,
        AccessChoiceSelectedReason, CandidateScore,
    },
};

pub(in crate::db::query::plan::access_choice) const fn chosen_access_shape_projection(
    access: &ExplainAccessPath,
) -> (AccessChoiceFamily, Option<&str>, CandidateScore) {
    match access {
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::KeyRange { .. }
        | ExplainAccessPath::FullScan
        | ExplainAccessPath::Union(_)
        | ExplainAccessPath::Intersection(_) => (
            AccessChoiceFamily::NonIndex,
            None,
            CandidateScore::new(0, true, false),
        ),
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            ..
        } => (
            AccessChoiceFamily::Prefix,
            Some(*name),
            CandidateScore::new(*prefix_len, *prefix_len == fields.len(), false),
        ),
        ExplainAccessPath::IndexMultiLookup { name, fields, .. } => (
            AccessChoiceFamily::MultiLookup,
            Some(*name),
            CandidateScore::new(1, fields.len() == 1, false),
        ),
        ExplainAccessPath::IndexRange {
            name, prefix_len, ..
        } => (
            AccessChoiceFamily::Range,
            Some(*name),
            CandidateScore::new(*prefix_len, false, false),
        ),
    }
}

pub(in crate::db::query::plan::access_choice) fn chosen_selection_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    eligible_other_scores: &[CandidateScore],
) -> AccessChoiceSelectedReason {
    if eligible_other_scores.is_empty() {
        return AccessChoiceSelectedReason::SingleCandidate;
    }

    if eligible_other_scores
        .iter()
        .all(|score| score.prefix_len < chosen_score.prefix_len)
    {
        return AccessChoiceSelectedReason::BestPrefixLen;
    }

    AccessChoiceSelectedReason::Ranked(ranked_preference_reason(
        family,
        chosen_score,
        eligible_other_scores,
    ))
}

pub(in crate::db::query::plan::access_choice) fn ranked_rejection_reason(
    family: AccessChoiceFamily,
    candidate: CandidateScore,
    chosen: CandidateScore,
) -> AccessChoiceRejectedReason {
    if candidate.prefix_len < chosen.prefix_len {
        return AccessChoiceRejectedReason::ShorterPrefix;
    }

    AccessChoiceRejectedReason::Ranked(ranked_preference_reason(family, chosen, &[candidate]))
}

// Resolve the canonical ranking reason once from the winning candidate and
// the competing same-prefix candidates so selected and rejected explain paths
// do not re-encode the same tie-break policy separately.
fn ranked_preference_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    competing_scores: &[CandidateScore],
) -> AccessChoiceRankingReason {
    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup
    ) && chosen_score.exact
        && competing_scores
            .iter()
            .any(|score| score.prefix_len == chosen_score.prefix_len && !score.exact)
    {
        return AccessChoiceRankingReason::ExactMatchPreferred;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::Range
    ) && chosen_score.order_compatible
        && competing_scores.iter().any(|score| {
            score.prefix_len == chosen_score.prefix_len
                && score.exact == chosen_score.exact
                && !score.order_compatible
        })
    {
        return AccessChoiceRankingReason::OrderCompatiblePreferred;
    }

    AccessChoiceRankingReason::LexicographicTiebreak
}
