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
        access::{AccessPlan, SemanticIndexAccessContract},
        predicate::Predicate,
        query::plan::{
            AcceptedPlannerFieldPathIndex, AccessPlannedQuery,
            access_choice::{
                evaluator::{
                    chosen_access_shape_projection, chosen_selection_reason,
                    evaluate_index_candidate, ranked_rejection_reason, sorted_indexes,
                },
                model::AccessChoiceFamily,
            },
            access_plan_label as planner_access_plan_label,
            plan_access_selection_with_order_and_accepted_indexes, plan_access_with_order,
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

pub(in crate::db) use self::model::{
    AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot, AccessChoiceResidualBurden,
    AccessChoiceSelectedReason,
};

///
/// project_access_choice_explain_snapshot_with_indexes
///
/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// one explicit planner-visible index set.
///

/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// explicit schema authority.
#[must_use]
pub(in crate::db) fn project_access_choice_explain_snapshot_with_indexes_and_schema(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> AccessChoiceExplainSnapshot {
    project_access_choice_explain_snapshot_from_authority(
        model,
        visible_indexes,
        &[],
        schema_info,
        plan,
    )
}

/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// accepted field-path index contracts where runtime accepted authority exists.
#[must_use]
pub(in crate::db) fn project_access_choice_explain_snapshot_with_accepted_indexes_and_schema(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> AccessChoiceExplainSnapshot {
    project_access_choice_explain_snapshot_from_authority(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    )
}

fn project_access_choice_explain_snapshot_from_authority(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> AccessChoiceExplainSnapshot {
    // Phase 1: classify chosen access family and reuse one already-frozen
    // planner-owned non-index snapshot when the selected route never entered
    // index candidate projection at all.
    let (family, chosen_index_name, chosen_score_hint) =
        chosen_access_shape_projection(&plan.access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return plan.access_choice().clone();
    }

    let Some(chosen_index_name) = chosen_index_name else {
        return AccessChoiceExplainSnapshot::selected_index_not_projected();
    };

    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = chosen_score_for_visible_indexes(
        family,
        chosen_score_hint,
        chosen_index_name,
        model,
        visible_indexes,
        schema_info,
        predicate,
        order,
        grouped,
    );
    let mut alternatives = Vec::new();
    let mut candidates = Vec::new();
    let mut rejected = Vec::new();
    let mut eligible_other_scores = Vec::new();
    let residual_burden_rejected_indexes = same_score_competing_residual_rejection_indexes(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    );

    // Phase 2: walk deterministic model order once so alternative/rejection
    // projection stays under one evaluation owner after the chosen score has
    // already been frozen from planner evaluation.
    for index in sorted_indexes(visible_indexes) {
        let index_name = index.name();
        match evaluate_index_candidate(family, index, model, schema_info, predicate, order, grouped)
        {
            self::model::CandidateEvaluation::Eligible(score)
                if index_name == chosen_index_name =>
            {
                candidates.push(project_candidate_explain_summary(
                    score,
                    &plan.access,
                    residual_burden_for_plan(plan),
                ));
            }
            self::model::CandidateEvaluation::Eligible(score) => {
                alternatives.push(index_name);
                eligible_other_scores.push(score);
                if let Some(candidate_access) = eligible_candidate_access_for_index(
                    model,
                    accepted_field_path_indexes,
                    schema_info,
                    plan,
                    index,
                ) {
                    let candidate_plan = candidate_plan_with_access(plan, candidate_access.clone());
                    candidates.push(project_candidate_explain_summary(
                        score,
                        &candidate_access,
                        residual_burden_for_plan(&candidate_plan),
                    ));
                }
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

    let residual_burden_preferred = chosen_access_prefers_lower_residual_burden(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    );

    // Phase 3: derive deterministic winner/rejection reason codes from the
    // one-pass candidate evaluation results above.
    AccessChoiceExplainSnapshot {
        chosen_reason: chosen_selection_reason(
            family,
            chosen_score,
            &eligible_other_scores,
            residual_burden_preferred,
        ),
        candidates,
        alternatives,
        rejected,
    }
}

// Keep non-index chosen-reason projection explicit and shape-based until the
// planner stores a more detailed non-index family winner reason on the plan.
pub(in crate::db) fn non_index_access_choice_snapshot_for_access_plan<K>(
    access: &AccessPlan<K>,
) -> AccessChoiceExplainSnapshot {
    if access.has_selected_index_access_path() {
        return AccessChoiceExplainSnapshot::selected_index_not_projected();
    }
    if access.as_by_key_path().is_some() {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::ByKeyAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    }
    if access
        .as_path()
        .and_then(|path| path.as_by_keys())
        .is_some()
    {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::ByKeysAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    }
    if access.as_primary_key_range_path().is_some() {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::PrimaryKeyRangeAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    }
    if access.is_single_full_scan() {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::FullScanAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    }

    AccessChoiceExplainSnapshot::non_index_access()
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
    rerank_access_plan_by_residual_burden_from_authority(
        model,
        visible_indexes,
        &[],
        schema_info,
        plan,
    )
}

/// Return one reranked access plan using accepted field-path index contracts
/// for candidate access reconstruction where runtime accepted authority exists.
#[must_use]
pub(in crate::db::query) fn rerank_access_plan_by_residual_burden_with_accepted_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<AccessPlan<Value>> {
    rerank_access_plan_by_residual_burden_from_authority(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    )
}

fn rerank_access_plan_by_residual_burden_from_authority(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<AccessPlan<Value>> {
    let preferred = preferred_same_score_competing_access_by_residual_burden(
        model,
        visible_indexes,
        accepted_field_path_indexes,
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
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> bool {
    preferred_same_score_competing_access_by_residual_burden(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    )
    .is_none()
        && same_score_competing_candidate_plans(
            model,
            visible_indexes,
            accepted_field_path_indexes,
            schema_info,
            plan,
        )
        .into_iter()
        .flatten()
        .any(|candidate| candidate.residual_burden > residual_burden_for_plan(plan))
}

// Return the index names for same-score competing routes that lose on
// residual burden once the structural ranking dimensions already tie.
fn same_score_competing_residual_rejection_indexes(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<Vec<&'static str>> {
    let chosen_burden = residual_burden_for_plan(plan);
    let rejected = same_score_competing_candidate_plans(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    )?
    .into_iter()
    .filter(|candidate| candidate.residual_burden > chosen_burden)
    .filter_map(|candidate| {
        candidate
            .access
            .selected_index_contract()
            .map(SemanticIndexAccessContract::name)
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

impl ResidualBurdenProfile {
    const fn kind(self) -> AccessChoiceResidualBurden {
        match self.kind_rank {
            0 => AccessChoiceResidualBurden::None,
            1 => AccessChoiceResidualBurden::PredicateOnly,
            _ => AccessChoiceResidualBurden::ScalarExpression,
        }
    }
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
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<ResidualComparableCandidate> {
    let chosen_burden = residual_burden_for_plan(plan);
    let mut best: Option<ResidualComparableCandidate> = None;

    for candidate in same_score_competing_candidate_plans(
        model,
        visible_indexes,
        accepted_field_path_indexes,
        schema_info,
        plan,
    )
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

#[expect(
    clippy::too_many_arguments,
    reason = "access-choice scoring keeps candidate authority and query shape explicit"
)]
fn chosen_score_for_visible_indexes(
    family: AccessChoiceFamily,
    chosen_score_hint: crate::db::query::plan::planner::AccessCandidateScore,
    chosen_index_name: &str,
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&crate::db::query::plan::OrderSpec>,
    grouped: bool,
) -> crate::db::query::plan::planner::AccessCandidateScore {
    visible_indexes
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
        .unwrap_or(chosen_score_hint)
}

// Build one candidate access plan through the existing single-index planner
// entry so explain and reranking consume the same planner-owned route shape.
fn eligible_candidate_access_for_index(
    model: &EntityModel,
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    index: &'static IndexModel,
) -> Option<AccessPlan<Value>> {
    if !accepted_field_path_indexes.is_empty() {
        return plan_access_selection_with_order_and_accepted_indexes(
            model,
            &[index],
            accepted_field_path_indexes,
            schema_info,
            plan.scalar_plan().predicate.as_ref(),
            plan.scalar_plan().order.as_ref(),
            plan.grouped_plan().is_some(),
        )
        .ok()
        .map(super::planner::PlannedAccessSelection::into_access);
    }

    plan_access_with_order(
        model,
        &[index],
        schema_info,
        plan.scalar_plan().predicate.as_ref(),
        plan.scalar_plan().order.as_ref(),
        plan.grouped_plan().is_some(),
    )
    .ok()
}

// Rebuild one coupled logical+access plan shell so residual burden can be
// measured against the same logical filter contract across candidates.
fn candidate_plan_with_access(
    plan: &AccessPlannedQuery,
    access: AccessPlan<Value>,
) -> AccessPlannedQuery {
    AccessPlannedQuery::from_parts_with_projection(
        plan.logical.clone(),
        access,
        plan.projection_selection.clone(),
    )
}

// Project one verbose explain summary for an eligible candidate route using
// the same candidate score and residual profile used by planner ranking.
fn project_candidate_explain_summary(
    score: crate::db::query::plan::planner::AccessCandidateScore,
    access: &AccessPlan<Value>,
    residual_burden: ResidualBurdenProfile,
) -> AccessChoiceCandidateExplainSummary {
    AccessChoiceCandidateExplainSummary {
        label: planner_access_plan_label(access),
        exact: score.exact,
        filtered: score.filtered,
        range_bound_count: usize::from(score.range_bound_count),
        order_compatible: score.order_compatible,
        residual_burden: residual_burden.kind(),
        residual_predicate_terms: residual_burden.predicate_term_count,
    }
}

// Enumerate same-family, same-score competing index routes by rebuilding each
// candidate through the existing single-index planner entry and deriving its
// residual burden from the coupled logical+access plan.
fn same_score_competing_candidate_plans(
    model: &EntityModel,
    visible_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
) -> Option<Vec<ResidualComparableCandidate>> {
    let (family, chosen_index_name, chosen_score_hint) =
        chosen_access_shape_projection(&plan.access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return None;
    }

    let chosen_index_name = chosen_index_name?;
    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = chosen_score_for_visible_indexes(
        family,
        chosen_score_hint,
        chosen_index_name,
        model,
        visible_indexes,
        schema_info,
        predicate,
        order,
        grouped,
    );

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

        let candidate_access = eligible_candidate_access_for_index(
            model,
            accepted_field_path_indexes,
            schema_info,
            plan,
            index,
        )?;
        let candidate_access_name = candidate_access
            .selected_index_contract()
            .map(SemanticIndexAccessContract::name);
        if candidate_access_name != Some(index.name()) {
            continue;
        }

        let candidate_plan = candidate_plan_with_access(plan, candidate_access.clone());
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
