//! Module: query::plan::access_choice
//! Responsibility: planner-owned access-choice scoring helpers and explain metadata projection.
//! Does not own: access-path execution or explain rendering.
//! Boundary: derives deterministic candidate/rejection metadata and bounded
//! same-score reranking helpers from planning contracts.

mod evaluator;
mod model;

#[cfg(test)]
mod tests;

pub(in crate::db) use self::model::{
    AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot, AccessChoiceRejectedIndex,
    AccessChoiceResidualBurden, AccessChoiceSelectedReason, PrimaryKeyInputResourceSummary,
};
///
/// TESTS
///
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract},
        predicate::Predicate,
        query::construction::ConstructionBudget,
        query::plan::{
            AccessPlannedQuery, CardinalityTiebreakCandidate, CardinalityTiebreakFamily,
            ResidualFilterShape,
            access_choice::{
                evaluator::{
                    CandidateRankingEvidence, chosen_access_shape_projection,
                    evaluate_index_candidate, ranked_rejection_reason,
                },
                model::{AccessChoiceCandidateKind, AccessChoiceFamily},
            },
            plan_access_selection_with_order_and_semantic_indexes,
            residual_filter_facts_for_access,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

///
/// project_access_choice_explain_snapshot_with_indexes
///
/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// one explicit planner-visible index set.
///

/// Project planner-owned access-choice candidate metadata for EXPLAIN using
/// already-projected semantic index contracts from the visible-index boundary.
pub(in crate::db) fn project_access_choice_explain_snapshot_with_semantic_indexes_and_schema(
    semantic_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<AccessChoiceExplainSnapshot, InternalError> {
    project_access_choice_explain_snapshot_from_authority(
        semantic_indexes,
        schema_info,
        plan,
        budget,
    )
}

/// Enumerate the complete final tie set from the existing access-choice owner.
///
/// This does not read cardinality. It returns candidates only when at least two
/// non-grouped routes remain equal under the maintained structural and residual
/// policy, immediately before the predecessor lexicographic tie-break.
pub(in crate::db) fn exact_cardinality_tiebreak_candidates<'a>(
    semantic_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &'a AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Vec<CardinalityTiebreakCandidate<'a>>>, InternalError> {
    if plan.grouped_plan().is_some() {
        return Ok(None);
    }
    let Some((family, consumed_prefix_arity)) = cardinality_family_and_arity(&plan.access) else {
        return Ok(None);
    };
    let explain_family = access_choice_family_for_cardinality(family);
    let Some(chosen_index) = plan.access.selected_index_contract() else {
        return Ok(None);
    };
    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let chosen_score = match evaluate_index_candidate(
        explain_family,
        &chosen_index,
        schema_info,
        predicate,
        order,
        false,
        budget,
    )? {
        self::model::CandidateEvaluation::Eligible(score) => score,
        self::model::CandidateEvaluation::Rejected(_) => return Ok(None),
    };
    let chosen_burden = residual_burden_for_plan(plan);
    // At most one retained route per visible index. Route payload construction
    // remains separately owned; admit list backing before retaining candidates.
    let mut candidates = budget.vec_with_capacity(semantic_indexes.len())?;
    let mut chosen_seen = false;

    for index in semantic_indexes {
        if index.name() == chosen_index.name() {
            if !chosen_seen {
                chosen_seen = true;
                candidates.push(CardinalityTiebreakCandidate::new(
                    Cow::Borrowed(&plan.access),
                    chosen_index.clone(),
                    family,
                    consumed_prefix_arity,
                ));
            }
            continue;
        }
        let self::model::CandidateEvaluation::Eligible(score) = evaluate_index_candidate(
            explain_family,
            index,
            schema_info,
            predicate,
            order,
            false,
            budget,
        )?
        else {
            continue;
        };
        if score != chosen_score {
            continue;
        }
        let Some(candidate_access) =
            eligible_candidate_access_for_index(schema_info, plan, index, budget)?
        else {
            return Ok(None);
        };
        let Some((candidate_family, candidate_prefix_arity)) =
            cardinality_family_and_arity(&candidate_access)
        else {
            return Ok(None);
        };
        if candidate_family != family || candidate_prefix_arity != consumed_prefix_arity {
            continue;
        }
        if residual_burden_for_candidate(plan, &candidate_access) != chosen_burden {
            continue;
        }
        candidates.push(CardinalityTiebreakCandidate::new(
            Cow::Owned(candidate_access),
            index.clone(),
            candidate_family,
            candidate_prefix_arity,
        ));
    }
    if !chosen_seen || candidates.len() < 2 {
        return Ok(None);
    }

    Ok(Some(candidates))
}

fn cardinality_family_and_arity(
    access: &AccessPlan<Value>,
) -> Option<(CardinalityTiebreakFamily, usize)> {
    let path = access.as_path()?;
    if let Some((_index, values)) = path.as_index_prefix_contract() {
        return Some((CardinalityTiebreakFamily::Prefix, values.len()));
    }
    if path.as_index_multi_lookup_contract().is_some() {
        return Some((CardinalityTiebreakFamily::MultiLookup, 1));
    }
    path.as_index_branch_set_spec().map(|spec| {
        (
            CardinalityTiebreakFamily::BranchSet,
            spec.branch_prefix_len(),
        )
    })
}

const fn access_choice_family_for_cardinality(
    family: CardinalityTiebreakFamily,
) -> AccessChoiceFamily {
    match family {
        CardinalityTiebreakFamily::Prefix => AccessChoiceFamily::Prefix,
        CardinalityTiebreakFamily::MultiLookup => AccessChoiceFamily::MultiLookup,
        CardinalityTiebreakFamily::BranchSet => AccessChoiceFamily::BranchSet,
    }
}

fn project_access_choice_explain_snapshot_from_authority(
    visible_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<AccessChoiceExplainSnapshot, InternalError> {
    // Phase 1: classify chosen access family and reuse one already-frozen
    // planner-owned non-index snapshot when the selected route never entered
    // index candidate projection at all.
    let (family, chosen_index_name, chosen_score_hint) =
        chosen_access_shape_projection(&plan.access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return Ok(plan.access_choice().clone());
    }
    let (Some(candidate_kind), Some(chosen_index_name)) =
        (family.candidate_kind(), chosen_index_name)
    else {
        return Ok(AccessChoiceExplainSnapshot::selected_index_not_projected());
    };

    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = chosen_score_for_visible_indexes(
        family,
        chosen_score_hint,
        chosen_index_name.name(),
        visible_indexes,
        schema_info,
        predicate,
        order,
        grouped,
        budget,
    )?;
    // Each visible index contributes at most one element to each retained list.
    // Reserve once before evaluating candidates; names are charged when copied.
    let mut alternatives = budget.vec_with_capacity(visible_indexes.len())?;
    let mut candidates = budget.vec_with_capacity(visible_indexes.len())?;
    let mut rejected = budget.vec_with_capacity(visible_indexes.len())?;
    let mut ranking = CandidateRankingEvidence::new();
    let chosen_burden = residual_burden_for_plan(plan);
    let mut found_lower_residual_burden = false;
    let mut found_higher_residual_burden = false;

    // Phase 2: walk deterministic model order once so alternative/rejection
    // projection stays under one evaluation owner after the chosen score has
    // already been frozen from planner evaluation.
    for index in visible_indexes {
        let index_name = budget.copy_text(index.name())?;
        match evaluate_index_candidate(
            family,
            index,
            schema_info,
            predicate,
            order,
            grouped,
            budget,
        )? {
            self::model::CandidateEvaluation::Eligible(score)
                if index_name == chosen_index_name.name() =>
            {
                candidates.push(project_candidate_explain_summary(
                    candidate_kind,
                    index_name,
                    score,
                    residual_burden_for_plan(plan),
                ));
            }
            self::model::CandidateEvaluation::Eligible(score) => {
                alternatives.push(budget.copy_text(&index_name)?);
                ranking.observe(family, chosen_score, score);
                let mut rejected_on_residual_burden = false;
                if let Some(candidate_access) =
                    eligible_candidate_access_for_index(schema_info, plan, index, budget)?
                {
                    let residual_burden = residual_burden_for_candidate(plan, &candidate_access);
                    candidates.push(project_candidate_explain_summary(
                        candidate_kind,
                        budget.copy_text(&index_name)?,
                        score,
                        residual_burden,
                    ));
                    if score == chosen_score
                        && candidate_access
                            .selected_index_contract()
                            .is_some_and(|contract| contract.name() == index_name.as_str())
                    {
                        found_lower_residual_burden |= residual_burden < chosen_burden;
                        rejected_on_residual_burden = residual_burden > chosen_burden;
                        found_higher_residual_burden |= rejected_on_residual_burden;
                    }
                }
                rejected.push(AccessChoiceRejectedIndex::new(
                    index_name,
                    ranked_rejection_reason(
                        family,
                        score,
                        chosen_score,
                        rejected_on_residual_burden,
                    ),
                ));
            }
            self::model::CandidateEvaluation::Rejected(reason) => {
                rejected.push(AccessChoiceRejectedIndex::new(index_name, reason));
            }
        }
    }

    let residual_burden_preferred = found_higher_residual_burden && !found_lower_residual_burden;

    // Phase 3: derive deterministic winner/rejection reason codes from the
    // one-pass candidate evaluation results above.
    Ok(AccessChoiceExplainSnapshot {
        chosen_reason: ranking.selected_reason(chosen_score, residual_burden_preferred),
        candidates,
        alternatives,
        rejected,
        primary_key_input_resource: None,
        cardinality_evidence_state: "not_applicable",
    })
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
            primary_key_input_resource: None,
            cardinality_evidence_state: "not_applicable",
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
            primary_key_input_resource: None,
            cardinality_evidence_state: "not_applicable",
        };
    }
    if access.as_primary_key_range_path().is_some() {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::PrimaryKeyRangeAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
            primary_key_input_resource: None,
            cardinality_evidence_state: "not_applicable",
        };
    }
    if access.is_single_full_scan() {
        return AccessChoiceExplainSnapshot {
            chosen_reason: self::model::AccessChoiceSelectedReason::FullScanAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
            primary_key_input_resource: None,
            cardinality_evidence_state: "not_applicable",
        };
    }

    AccessChoiceExplainSnapshot::non_index_access()
}

/// Return one reranked access plan using already-projected semantic index
/// contracts from the runtime visible-index boundary.
pub(in crate::db::query) fn rerank_access_plan_by_residual_burden_with_semantic_indexes(
    semantic_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    rerank_access_plan_by_residual_burden_from_authority(
        semantic_indexes,
        schema_info,
        plan,
        budget,
    )
}

fn rerank_access_plan_by_residual_burden_from_authority(
    visible_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    if residual_burden_for_plan(plan).is_empty() {
        return Ok(None);
    }

    let preferred = preferred_same_score_competing_access_by_residual_burden(
        visible_indexes,
        schema_info,
        plan,
        budget,
    )?;

    Ok(preferred.map(|preferred| preferred.access))
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
    const fn kind_rank_for_residual_shape(shape: ResidualFilterShape) -> u8 {
        match shape {
            ResidualFilterShape::Absent => 0,
            ResidualFilterShape::Predicate => 1,
            ResidualFilterShape::Expression | ResidualFilterShape::ExpressionAndPredicate => 2,
        }
    }

    const fn kind(self) -> AccessChoiceResidualBurden {
        match self.kind_rank {
            0 => AccessChoiceResidualBurden::None,
            1 => AccessChoiceResidualBurden::PredicateOnly,
            _ => AccessChoiceResidualBurden::ScalarExpression,
        }
    }

    const fn is_empty(self) -> bool {
        self.kind_rank == 0 && self.predicate_term_count == 0
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

#[expect(
    clippy::too_many_arguments,
    reason = "access-choice scoring keeps candidate authority and query shape explicit"
)]
fn chosen_score_for_visible_indexes(
    family: AccessChoiceFamily,
    chosen_score_hint: crate::db::query::plan::planner::AccessCandidateScore,
    chosen_index_name: &str,
    visible_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&crate::db::query::plan::OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<crate::db::query::plan::planner::AccessCandidateScore, InternalError> {
    // Semantic rejection may use the existing shape hint; exhausted evaluation
    // must propagate instead of silently substituting that hint.
    if let Some(index) = visible_indexes
        .iter()
        .find(|index| index.name() == chosen_index_name)
        && let self::model::CandidateEvaluation::Eligible(score) = evaluate_index_candidate(
            family,
            index,
            schema_info,
            predicate,
            order,
            grouped,
            budget,
        )?
    {
        return Ok(score);
    }
    Ok(chosen_score_hint)
}

// Build one candidate access plan through the existing single-index planner
// entry so explain and reranking consume the same planner-owned route shape.
fn eligible_candidate_access_for_index(
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    index: &SemanticIndexAccessContract,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    use crate::db::query::plan::planner::PlannerError;

    match plan_access_selection_with_order_and_semantic_indexes(
        std::slice::from_ref(index),
        schema_info,
        plan.scalar_plan().predicate.as_ref(),
        plan.scalar_plan().order.as_ref(),
        plan.grouped_plan().is_some(),
        budget,
    ) {
        Ok(selection) => Ok(Some(selection.into_access())),
        // A semantic non-candidate is still optional; construction failure is
        // not missing evidence and must not select a partial/fallback result.
        Err(PlannerError::Plan(_)) => Ok(None),
        Err(PlannerError::Internal(error)) => Err(*error),
    }
}

// Project one verbose explain summary for an eligible candidate route using
// the same candidate score and residual profile used by planner ranking.
fn project_candidate_explain_summary(
    kind: AccessChoiceCandidateKind,
    index_name: String,
    score: crate::db::query::plan::planner::AccessCandidateScore,
    residual_burden: ResidualBurdenProfile,
) -> AccessChoiceCandidateExplainSummary {
    AccessChoiceCandidateExplainSummary {
        kind,
        index_name,
        exact: score.exact,
        filtered: score.filtered,
        range_bound_count: usize::from(score.range_bound_count),
        order_compatible: score.order_compatible,
        residual_burden: residual_burden.kind(),
        residual_predicate_terms: residual_burden.predicate_term_count,
        exact_prefix_entries: None,
    }
}

// Enumerate same-family, same-score competing index routes by rebuilding each
// candidate through the existing single-index planner entry and deriving its
// residual burden from borrowed scalar semantics and the candidate access.
// Keep only the best same-score alternative rather than retaining every plan.
// Visit all candidates even after finding an empty residual: a later failed
// candidate still cancels reranking under the existing fail-closed policy.
fn preferred_same_score_competing_access_by_residual_burden(
    visible_indexes: &[SemanticIndexAccessContract],
    schema_info: &SchemaInfo,
    plan: &AccessPlannedQuery,
    budget: &dyn ConstructionBudget,
) -> Result<Option<ResidualComparableCandidate>, InternalError> {
    let (family, chosen_index_name, chosen_score_hint) =
        chosen_access_shape_projection(&plan.access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return Ok(None);
    }

    let Some(chosen_index_name) = chosen_index_name else {
        return Ok(None);
    };
    let predicate = plan.scalar_plan().predicate.as_ref();
    let order = plan.scalar_plan().order.as_ref();
    let grouped = plan.grouped_plan().is_some();
    let chosen_score = chosen_score_for_visible_indexes(
        family,
        chosen_score_hint,
        chosen_index_name.name(),
        visible_indexes,
        schema_info,
        predicate,
        order,
        grouped,
        budget,
    )?;

    let chosen_burden = residual_burden_for_plan(plan);
    let mut best: Option<ResidualComparableCandidate> = None;
    for index in visible_indexes {
        if index.name() == chosen_index_name.name() {
            continue;
        }
        let self::model::CandidateEvaluation::Eligible(score) = evaluate_index_candidate(
            family,
            index,
            schema_info,
            predicate,
            order,
            grouped,
            budget,
        )?
        else {
            continue;
        };
        if score != chosen_score {
            continue;
        }

        let Some(candidate_access) =
            eligible_candidate_access_for_index(schema_info, plan, index, budget)?
        else {
            return Ok(None);
        };
        if candidate_access
            .selected_index_contract()
            .is_none_or(|contract| contract.name() != index.name())
        {
            continue;
        }

        let residual_burden = residual_burden_for_candidate(plan, &candidate_access);
        // Equal burden retains the first candidate in accepted-name order.
        if residual_burden < chosen_burden
            && best
                .as_ref()
                .is_none_or(|existing| residual_burden < existing.residual_burden)
        {
            best = Some(ResidualComparableCandidate {
                access: candidate_access,
                residual_burden,
            });
        }
    }

    Ok(best)
}

// Project one bounded residual burden category from the coupled logical+access
// plan without inventing numeric costs or selectivity math.
fn residual_burden_for_plan(plan: &AccessPlannedQuery) -> ResidualBurdenProfile {
    residual_burden_from_filter_facts(
        plan.residual_filter_shape(),
        plan.effective_execution_predicate().as_deref(),
    )
}

// Candidate routes have no finalized contract. Share the semantic derivation
// without copying the logical query, projection or candidate access plan.
fn residual_burden_for_candidate(
    plan: &AccessPlannedQuery,
    access: &AccessPlan<Value>,
) -> ResidualBurdenProfile {
    let (shape, predicate) = residual_filter_facts_for_access(plan.scalar_plan(), access);

    residual_burden_from_filter_facts(shape, predicate.as_ref())
}

fn residual_burden_from_filter_facts(
    shape: ResidualFilterShape,
    predicate: Option<&Predicate>,
) -> ResidualBurdenProfile {
    let predicate_term_count = predicate.map_or(0, count_predicate_terms);
    let kind_rank = ResidualBurdenProfile::kind_rank_for_residual_shape(shape);

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
