#[cfg(test)]
mod tests;

use crate::db::{
    access::{AccessPlan, SemanticIndexAccessContract},
    query::plan::{
        AccessPlanProjection,
        access_choice::model::{
            AccessChoiceFamily, AccessChoiceRankingReason, AccessChoiceRejectedReason,
            AccessChoiceSelectedReason, CandidateScore,
        },
        planner::range_bound_count,
        project_access_plan,
    },
};
use crate::value::Value;

///
/// ChosenAccessShapeProjection
///
/// Planner-owned projection adapter for classifying the already-chosen access
/// route without converting it through EXPLAIN transport first.
///

struct ChosenAccessShapeProjection;

impl AccessPlanProjection<Value> for ChosenAccessShapeProjection {
    type Output = (AccessChoiceFamily, CandidateScore);

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        (
            AccessChoiceFamily::NonIndex,
            CandidateScore::new(0, true, false, 0, false),
        )
    }

    fn by_keys(&mut self, _keys: &[Value]) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn index_prefix<'a>(
        &mut self,
        _index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        (
            AccessChoiceFamily::Prefix,
            CandidateScore::new(
                prefix_len,
                prefix_len == index_fields.len(),
                false,
                0,
                false,
            ),
        )
    }

    fn index_multi_lookup<'a>(
        &mut self,
        _index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        _values: &[Value],
    ) -> Self::Output {
        (
            AccessChoiceFamily::MultiLookup,
            CandidateScore::new(1, index_fields.len() == 1, false, 0, false),
        )
    }

    fn index_branch_set<'a>(
        &mut self,
        _index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        fixed_values: &[Value],
        _branch_values: &[Value],
    ) -> Self::Output {
        let prefix_len = fixed_values.len().saturating_add(1);
        (
            AccessChoiceFamily::BranchSet,
            CandidateScore::new(
                prefix_len,
                prefix_len == index_fields.len(),
                false,
                0,
                false,
            ),
        )
    }

    fn index_range<'a>(
        &mut self,
        _index_name: &str,
        _index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        _prefix: &[Value],
        lower: &std::ops::Bound<Value>,
        upper: &std::ops::Bound<Value>,
    ) -> Self::Output {
        (
            AccessChoiceFamily::Range,
            CandidateScore::new(
                prefix_len,
                false,
                false,
                range_bound_count(lower, upper),
                false,
            ),
        )
    }

    fn full_scan(&mut self) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn union<T>(
        &mut self,
        _children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn intersection<T>(
        &mut self,
        _children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        self.by_key(&Value::Null)
    }
}

pub(in crate::db::query::plan::access_choice) fn chosen_access_shape_projection(
    access: &AccessPlan<Value>,
) -> (
    AccessChoiceFamily,
    Option<SemanticIndexAccessContract>,
    CandidateScore,
) {
    let (family, score) = project_access_plan(access, &mut ChosenAccessShapeProjection);
    (family, access.selected_index_contract(), score)
}

/// Sufficient evidence for the selected reason; no competing-score list is needed.
pub(in crate::db::query::plan::access_choice) struct CandidateRankingEvidence {
    longest_prefix: Option<usize>,
    preference: AccessChoiceRankingReason,
}

impl CandidateRankingEvidence {
    pub(in crate::db::query::plan::access_choice) const fn new() -> Self {
        Self {
            longest_prefix: None,
            preference: AccessChoiceRankingReason::LexicographicTiebreak,
        }
    }

    pub(in crate::db::query::plan::access_choice) fn observe(
        &mut self,
        family: AccessChoiceFamily,
        chosen: CandidateScore,
        candidate: CandidateScore,
    ) {
        self.longest_prefix = Some(self.longest_prefix.map_or(candidate.prefix_len, |prefix| {
            prefix.max(candidate.prefix_len)
        }));
        let preference = ranked_preference_reason(family, chosen, candidate);
        if reason_priority(preference) < reason_priority(self.preference) {
            self.preference = preference;
        }
    }

    pub(in crate::db::query::plan::access_choice) const fn selected_reason(
        &self,
        chosen: CandidateScore,
        residual_burden_preferred: bool,
    ) -> AccessChoiceSelectedReason {
        let Some(longest_prefix) = self.longest_prefix else {
            return AccessChoiceSelectedReason::SingleCandidate;
        };
        if longest_prefix < chosen.prefix_len {
            return AccessChoiceSelectedReason::BestPrefixLen;
        }
        AccessChoiceSelectedReason::Ranked(self.ranked_reason(residual_burden_preferred))
    }

    const fn ranked_reason(&self, residual_burden_preferred: bool) -> AccessChoiceRankingReason {
        if residual_burden_preferred
            && reason_priority(AccessChoiceRankingReason::ResidualBurdenPreferred)
                < reason_priority(self.preference)
        {
            AccessChoiceRankingReason::ResidualBurdenPreferred
        } else {
            self.preference
        }
    }
}

// This order is diagnostic policy, not the representation order of the public
// reason enum. Residual evidence is known only after all candidates are visited.
const fn reason_priority(reason: AccessChoiceRankingReason) -> u8 {
    match reason {
        AccessChoiceRankingReason::ExactCardinalityTiebreak => 0,
        AccessChoiceRankingReason::ExactMatchPreferred => 1,
        AccessChoiceRankingReason::FilteredPredicatePreferred => 2,
        AccessChoiceRankingReason::StrongerRangeBoundsPreferred => 3,
        AccessChoiceRankingReason::ResidualBurdenPreferred => 4,
        AccessChoiceRankingReason::OrderCompatiblePreferred => 5,
        AccessChoiceRankingReason::LexicographicTiebreak => 6,
    }
}

pub(in crate::db::query::plan::access_choice) fn ranked_rejection_reason(
    family: AccessChoiceFamily,
    candidate: CandidateScore,
    chosen: CandidateScore,
    residual_burden_preferred: bool,
) -> AccessChoiceRejectedReason {
    if candidate.prefix_len < chosen.prefix_len {
        return AccessChoiceRejectedReason::ShorterPrefix;
    }

    let mut evidence = CandidateRankingEvidence::new();
    evidence.observe(family, chosen, candidate);
    AccessChoiceRejectedReason::Ranked(evidence.ranked_reason(residual_burden_preferred))
}

// Resolve the canonical ranking reason once from the winning candidate and
// the competing same-prefix candidates so selected and rejected explain paths
// do not re-encode the same tie-break policy separately.
const fn ranked_preference_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    score: CandidateScore,
) -> AccessChoiceRankingReason {
    if matches!(
        family,
        AccessChoiceFamily::Prefix
            | AccessChoiceFamily::MultiLookup
            | AccessChoiceFamily::BranchSet
    ) && chosen_score.exact
        && score.prefix_len == chosen_score.prefix_len
        && !score.exact
    {
        return AccessChoiceRankingReason::ExactMatchPreferred;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix
            | AccessChoiceFamily::MultiLookup
            | AccessChoiceFamily::BranchSet
            | AccessChoiceFamily::Range
    ) && chosen_score.filtered
        && (score.prefix_len == chosen_score.prefix_len
            && score.exact == chosen_score.exact
            && !score.filtered)
    {
        return AccessChoiceRankingReason::FilteredPredicatePreferred;
    }

    if matches!(family, AccessChoiceFamily::Range)
        && chosen_score.range_bound_count > 0
        && (score.prefix_len == chosen_score.prefix_len
            && score.exact == chosen_score.exact
            && score.filtered == chosen_score.filtered
            && score.range_bound_count < chosen_score.range_bound_count)
    {
        return AccessChoiceRankingReason::StrongerRangeBoundsPreferred;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix
            | AccessChoiceFamily::MultiLookup
            | AccessChoiceFamily::BranchSet
            | AccessChoiceFamily::Range
    ) && chosen_score.order_compatible
        && (score.prefix_len == chosen_score.prefix_len
            && score.exact == chosen_score.exact
            && score.filtered == chosen_score.filtered
            && score.range_bound_count == chosen_score.range_bound_count
            && !score.order_compatible)
    {
        return AccessChoiceRankingReason::OrderCompatiblePreferred;
    }

    AccessChoiceRankingReason::LexicographicTiebreak
}
