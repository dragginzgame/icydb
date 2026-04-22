use crate::db::{
    access::AccessPlan,
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
    type Output = (AccessChoiceFamily, Option<&'static str>, CandidateScore);

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        (
            AccessChoiceFamily::NonIndex,
            None,
            CandidateScore::new(0, true, false, 0, false),
        )
    }

    fn by_keys(&mut self, _keys: &[Value]) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        (
            AccessChoiceFamily::Prefix,
            Some(index_name),
            CandidateScore::new(
                prefix_len,
                prefix_len == index_fields.len(),
                false,
                0,
                false,
            ),
        )
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        _values: &[Value],
    ) -> Self::Output {
        (
            AccessChoiceFamily::MultiLookup,
            Some(index_name),
            CandidateScore::new(1, index_fields.len() == 1, false, 0, false),
        )
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        _index_fields: &[&'static str],
        prefix_len: usize,
        _prefix: &[Value],
        lower: &std::ops::Bound<Value>,
        upper: &std::ops::Bound<Value>,
    ) -> Self::Output {
        (
            AccessChoiceFamily::Range,
            Some(index_name),
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

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        self.by_key(&Value::Null)
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        self.by_key(&Value::Null)
    }
}

pub(in crate::db::query::plan::access_choice) fn chosen_access_shape_projection(
    access: &AccessPlan<Value>,
) -> (AccessChoiceFamily, Option<&'static str>, CandidateScore) {
    project_access_plan(access, &mut ChosenAccessShapeProjection)
}

pub(in crate::db::query::plan::access_choice) fn chosen_selection_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    eligible_other_scores: &[CandidateScore],
    residual_burden_preferred: bool,
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
        residual_burden_preferred,
    ))
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

    AccessChoiceRejectedReason::Ranked(ranked_preference_reason(
        family,
        chosen,
        &[candidate],
        residual_burden_preferred,
    ))
}

// Resolve the canonical ranking reason once from the winning candidate and
// the competing same-prefix candidates so selected and rejected explain paths
// do not re-encode the same tie-break policy separately.
fn ranked_preference_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    competing_scores: &[CandidateScore],
    residual_burden_preferred: bool,
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
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup | AccessChoiceFamily::Range
    ) && chosen_score.filtered
        && competing_scores.iter().any(|score| {
            score.prefix_len == chosen_score.prefix_len
                && score.exact == chosen_score.exact
                && !score.filtered
        })
    {
        return AccessChoiceRankingReason::FilteredPredicatePreferred;
    }

    if matches!(family, AccessChoiceFamily::Range)
        && chosen_score.range_bound_count > 0
        && competing_scores.iter().any(|score| {
            score.prefix_len == chosen_score.prefix_len
                && score.exact == chosen_score.exact
                && score.filtered == chosen_score.filtered
                && score.range_bound_count < chosen_score.range_bound_count
        })
    {
        return AccessChoiceRankingReason::StrongerRangeBoundsPreferred;
    }

    if residual_burden_preferred {
        return AccessChoiceRankingReason::ResidualBurdenPreferred;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::Range
    ) && chosen_score.order_compatible
        && competing_scores.iter().any(|score| {
            score.prefix_len == chosen_score.prefix_len
                && score.exact == chosen_score.exact
                && score.filtered == chosen_score.filtered
                && score.range_bound_count == chosen_score.range_bound_count
                && !score.order_compatible
        })
    {
        return AccessChoiceRankingReason::OrderCompatiblePreferred;
    }

    AccessChoiceRankingReason::LexicographicTiebreak
}
