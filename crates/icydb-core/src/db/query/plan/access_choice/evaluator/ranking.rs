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

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup
    ) && chosen_score.exact
        && eligible_other_scores
            .iter()
            .any(|score| score.prefix_len == chosen_score.prefix_len && !score.exact)
    {
        return AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::ExactMatchPreferred);
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::Range
    ) && chosen_score.order_compatible
        && eligible_other_scores.iter().any(|score| {
            score.prefix_len == chosen_score.prefix_len
                && score.exact == chosen_score.exact
                && !score.order_compatible
        })
    {
        return AccessChoiceSelectedReason::Ranked(
            AccessChoiceRankingReason::OrderCompatiblePreferred,
        );
    }

    AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::LexicographicTiebreak)
}

pub(in crate::db::query::plan::access_choice) const fn ranked_rejection_reason(
    family: AccessChoiceFamily,
    candidate: CandidateScore,
    chosen: CandidateScore,
) -> AccessChoiceRejectedReason {
    if candidate.prefix_len < chosen.prefix_len {
        return AccessChoiceRejectedReason::ShorterPrefix;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup
    ) && !candidate.exact
        && chosen.exact
        && candidate.prefix_len == chosen.prefix_len
    {
        return AccessChoiceRejectedReason::Ranked(AccessChoiceRankingReason::ExactMatchPreferred);
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::Range
    ) && !candidate.order_compatible
        && chosen.order_compatible
        && candidate.prefix_len == chosen.prefix_len
        && candidate.exact == chosen.exact
    {
        return AccessChoiceRejectedReason::Ranked(
            AccessChoiceRankingReason::OrderCompatiblePreferred,
        );
    }

    AccessChoiceRejectedReason::Ranked(AccessChoiceRankingReason::LexicographicTiebreak)
}
