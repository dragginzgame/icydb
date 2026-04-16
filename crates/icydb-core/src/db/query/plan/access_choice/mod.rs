//! Module: query::plan::access_choice
//! Responsibility: planner-owned access-choice explain metadata projection.
//! Does not own: access-path execution, route decisions, or explain rendering.
//! Boundary: derives deterministic candidate/rejection metadata from planning contracts.

mod evaluator;
mod model;

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        query::plan::{
            AccessPlannedQuery,
            access_choice::{
                evaluator::{
                    chosen_access_shape_projection, chosen_selection_reason,
                    evaluate_index_candidate, ranked_rejection_reason, sorted_indexes,
                },
                model::AccessChoiceFamily,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, index::IndexModel},
};

pub(in crate::db) use self::model::AccessChoiceExplainSnapshot;

///
/// project_access_choice_explain_snapshot_with_indexes
///
/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// one explicit planner-visible index set.
///

#[must_use]
pub(in crate::db) fn project_access_choice_explain_snapshot_with_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    plan: &AccessPlannedQuery,
) -> AccessChoiceExplainSnapshot {
    let access = crate::db::query::explain::ExplainAccessPath::from_access_plan(&plan.access);

    // Phase 1: classify chosen access family and seed non-index fallbacks.
    let (family, chosen_index_name, chosen_score_hint) = chosen_access_shape_projection(&access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return AccessChoiceExplainSnapshot::non_index_access();
    }

    let Some(chosen_index_name) = chosen_index_name else {
        return AccessChoiceExplainSnapshot::selected_index_not_projected();
    };

    let schema_info = SchemaInfo::cached_for_entity_model(model);

    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let chosen_score = visible_indexes
        .iter()
        .copied()
        .find(|index| index.name() == chosen_index_name)
        .and_then(|index| {
            match evaluate_index_candidate(family, index, model, schema_info, predicate, order) {
                self::model::CandidateEvaluation::Eligible(score) => Some(score),
                self::model::CandidateEvaluation::Rejected(_) => None,
            }
        })
        .unwrap_or(chosen_score_hint);
    let mut alternatives = Vec::new();
    let mut rejected = Vec::new();
    let mut eligible_other_scores = Vec::new();

    // Phase 2: walk deterministic model order once so alternative/rejection
    // projection stays under one evaluation owner after the chosen score has
    // already been frozen from planner evaluation.
    for index in sorted_indexes(visible_indexes) {
        let index_name = index.name();
        match evaluate_index_candidate(family, index, model, schema_info, predicate, order) {
            self::model::CandidateEvaluation::Eligible(_score)
                if index_name == chosen_index_name => {}
            self::model::CandidateEvaluation::Eligible(score) => {
                alternatives.push(index_name);
                eligible_other_scores.push(score);
                rejected.push(
                    ranked_rejection_reason(family, score, chosen_score)
                        .render_for_index(index_name),
                );
            }
            self::model::CandidateEvaluation::Rejected(reason) => {
                rejected.push(reason.render_for_index(index_name));
            }
        }
    }

    // Phase 3: derive deterministic winner/rejection reason codes from the
    // one-pass candidate evaluation results above.
    AccessChoiceExplainSnapshot {
        chosen_reason: chosen_selection_reason(family, chosen_score, &eligible_other_scores),
        alternatives,
        rejected,
    }
}
