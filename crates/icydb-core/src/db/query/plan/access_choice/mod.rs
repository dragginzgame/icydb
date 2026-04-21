//! Module: query::plan::access_choice
//! Responsibility: planner-owned access-choice scoring helpers and explain metadata projection.
//! Does not own: access-path execution or explain rendering.
//! Boundary: derives deterministic candidate/rejection metadata and bounded
//! same-score reranking helpers from planning contracts.

mod evaluator;
mod model;

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::AccessPlan,
        predicate::Predicate,
        query::plan::{
            AccessPlannedQuery,
            access_choice::{
                evaluator::{
                    chosen_access_shape_projection, chosen_selection_reason,
                    evaluate_index_candidate, ranked_rejection_reason, sorted_indexes,
                },
                model::AccessChoiceFamily,
            },
            plan_access_with_order,
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
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
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = visible_indexes
        .iter()
        .copied()
        .find(|index| index.name() == chosen_index_name)
        .and_then(|index| {
            match evaluate_index_candidate(
                family,
                index,
                model,
                schema_info,
                predicate,
                order,
                grouped,
            ) {
                self::model::CandidateEvaluation::Eligible(score) => Some(score),
                self::model::CandidateEvaluation::Rejected(_) => None,
            }
        })
        .unwrap_or(chosen_score_hint);
    let mut alternatives = Vec::new();
    let mut rejected = Vec::new();
    let mut eligible_other_scores = Vec::new();
    let residual_burden_rejected_indexes =
        same_score_competing_residual_rejection_indexes(model, visible_indexes, schema_info, plan);

    // Phase 2: walk deterministic model order once so alternative/rejection
    // projection stays under one evaluation owner after the chosen score has
    // already been frozen from planner evaluation.
    for index in sorted_indexes(visible_indexes) {
        let index_name = index.name();
        match evaluate_index_candidate(family, index, model, schema_info, predicate, order, grouped)
        {
            self::model::CandidateEvaluation::Eligible(_score)
                if index_name == chosen_index_name => {}
            self::model::CandidateEvaluation::Eligible(score) => {
                alternatives.push(index_name);
                eligible_other_scores.push(score);
                let rejected_on_residual_burden = residual_burden_rejected_indexes
                    .as_ref()
                    .is_some_and(|indexes| indexes.contains(&index_name));
                rejected.push(
                    ranked_rejection_reason(
                        family,
                        score,
                        chosen_score,
                        rejected_on_residual_burden,
                    )
                    .render_for_index(index_name),
                );
            }
            self::model::CandidateEvaluation::Rejected(reason) => {
                rejected.push(reason.render_for_index(index_name));
            }
        }
    }

    let residual_burden_preferred =
        chosen_access_prefers_lower_residual_burden(model, visible_indexes, schema_info, plan);

    // Phase 3: derive deterministic winner/rejection reason codes from the
    // one-pass candidate evaluation results above.
    AccessChoiceExplainSnapshot {
        chosen_reason: chosen_selection_reason(
            family,
            chosen_score,
            &eligible_other_scores,
            residual_burden_preferred,
        ),
        alternatives,
        rejected,
    }
}

/// Return one reranked access plan when a same-score competing index route
/// leaves less residual work than the current chosen route.
#[must_use]
pub(in crate::db::query) fn rerank_access_plan_by_residual_burden_with_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<AccessPlan<Value>> {
    let preferred = preferred_same_score_competing_access_by_residual_burden(
        model,
        visible_indexes,
        schema_info,
        plan,
    )?;

    Some(preferred.access)
}

// Determine whether the already-selected access route beats one same-score
// competing candidate on residual burden alone.
fn chosen_access_prefers_lower_residual_burden(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> bool {
    preferred_same_score_competing_access_by_residual_burden(
        model,
        visible_indexes,
        schema_info,
        plan,
    )
    .is_none()
        && same_score_competing_candidate_plans(model, visible_indexes, schema_info, plan)
            .into_iter()
            .flatten()
            .any(|candidate| candidate.residual_burden > residual_burden_for_plan(plan))
}

// Return the index names for same-score competing routes that lose on
// residual burden once the structural ranking dimensions already tie.
fn same_score_competing_residual_rejection_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<Vec<&'static str>> {
    let chosen_burden = residual_burden_for_plan(plan);
    let rejected = same_score_competing_candidate_plans(model, visible_indexes, schema_info, plan)?
        .into_iter()
        .filter(|candidate| candidate.residual_burden > chosen_burden)
        .filter_map(|candidate| {
            candidate
                .access
                .selected_index_model()
                .map(IndexModel::name)
        })
        .collect::<Vec<_>>();

    (!rejected.is_empty()).then_some(rejected)
}

///
/// ResidualBurdenProfile
///
/// ResidualBurdenProfile carries one bounded planner-visible residual ranking
/// category for same-score candidate comparison.
///
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResidualBurdenProfile {
    kind_rank: u8,
    predicate_term_count: usize,
}

///
/// ResidualComparableCandidate
///
/// ResidualComparableCandidate couples one same-score competing access route
/// with its derived residual burden for bounded `.1` reranking.
///
#[derive(Clone, Debug)]
struct ResidualComparableCandidate {
    access: AccessPlan<Value>,
    residual_burden: ResidualBurdenProfile,
}

// Build the best same-score competing access route that leaves less residual
// work than the current chosen route.
fn preferred_same_score_competing_access_by_residual_burden(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<ResidualComparableCandidate> {
    let chosen_burden = residual_burden_for_plan(plan);
    let mut best: Option<ResidualComparableCandidate> = None;

    for candidate in same_score_competing_candidate_plans(model, visible_indexes, schema_info, plan)
        .into_iter()
        .flatten()
    {
        if candidate.residual_burden >= chosen_burden {
            continue;
        }

        match &best {
            None => best = Some(candidate),
            Some(existing) if candidate.residual_burden < existing.residual_burden => {
                best = Some(candidate);
            }
            Some(_) => {}
        }
    }

    best
}

// Enumerate same-family, same-score competing index routes by rebuilding each
// candidate through the existing single-index planner entry and deriving its
// residual burden from the coupled logical+access plan.
fn same_score_competing_candidate_plans(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<Vec<ResidualComparableCandidate>> {
    let access = crate::db::query::explain::ExplainAccessPath::from_access_plan(&plan.access);
    let (family, chosen_index_name, chosen_score_hint) = chosen_access_shape_projection(&access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return None;
    }

    let chosen_index_name = chosen_index_name?;
    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = visible_indexes
        .iter()
        .copied()
        .find(|index| index.name() == chosen_index_name)
        .and_then(|index| {
            match evaluate_index_candidate(
                family,
                index,
                model,
                schema_info,
                predicate,
                order,
                grouped,
            ) {
                self::model::CandidateEvaluation::Eligible(score) => Some(score),
                self::model::CandidateEvaluation::Rejected(_) => None,
            }
        })
        .unwrap_or(chosen_score_hint);

    let mut candidates = Vec::new();
    for index in sorted_indexes(visible_indexes) {
        if index.name() == chosen_index_name {
            continue;
        }
        let self::model::CandidateEvaluation::Eligible(score) =
            evaluate_index_candidate(family, index, model, schema_info, predicate, order, grouped)
        else {
            continue;
        };
        if score != chosen_score {
            continue;
        }

        let candidate_access =
            plan_access_with_order(model, &[index], schema_info, predicate, order, grouped).ok()?;
        let candidate_access_name = candidate_access
            .selected_index_model()
            .map(crate::model::index::IndexModel::name);
        if candidate_access_name != Some(index.name()) {
            continue;
        }

        let candidate_plan = AccessPlannedQuery::from_parts_with_projection(
            plan.logical.clone(),
            candidate_access.clone(),
            plan.projection_selection.clone(),
        );
        candidates.push(ResidualComparableCandidate {
            access: candidate_access,
            residual_burden: residual_burden_for_plan(&candidate_plan),
        });
    }

    Some(candidates)
}

// Project one bounded residual burden category from the coupled logical+access
// plan without inventing numeric costs or selectivity math.
fn residual_burden_for_plan(plan: &AccessPlannedQuery) -> ResidualBurdenProfile {
    let predicate_term_count = plan
        .effective_execution_predicate()
        .as_ref()
        .map_or(0, count_predicate_terms);
    let kind_rank = if plan.residual_filter_expr().is_some() {
        2
    } else {
        u8::from(predicate_term_count > 0)
    };

    ResidualBurdenProfile {
        kind_rank,
        predicate_term_count,
    }
}

// Count residual predicate terms using the planner-owned boolean tree shape so
// same-score candidate comparison can prefer the route that leaves a smaller
// predicate remainder.
fn count_predicate_terms(predicate: &Predicate) -> usize {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().map(count_predicate_terms).sum()
        }
        Predicate::True | Predicate::False => 0,
        Predicate::Not(_)
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => 1,
    }
}
