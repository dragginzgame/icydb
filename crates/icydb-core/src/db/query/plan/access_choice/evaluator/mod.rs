//! Module: db::query::plan::access_choice::evaluator
//! Responsibility: planner-owned access-choice candidate evaluation and ranking projection.
//! Does not own: access-path execution, route decisions, or explain rendering.
//! Boundary: exposes the evaluator boundary while keeping prefix/range/ranking logic in owner-local children.

mod prefix;
mod range;
mod ranking;

use crate::{
    db::{
        access::SemanticIndexAccessContract,
        predicate::Predicate,
        query::plan::{
            OrderSpec,
            access_choice::model::{
                AccessChoiceFamily, AccessChoiceRejectedReason, CandidateEvaluation,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, index::IndexModel},
};

#[cfg(test)]
pub(in crate::db::query::plan::access_choice) use prefix::{
    evaluate_multi_lookup_candidate, evaluate_prefix_compare_candidate,
};
#[cfg(test)]
pub(in crate::db::query::plan::access_choice) use range::evaluate_range_candidate;
pub(in crate::db::query::plan::access_choice) use ranking::{
    chosen_access_shape_projection, chosen_selection_reason, ranked_rejection_reason,
};

pub(super) fn sorted_indexes(indexes: &[&'static IndexModel]) -> Vec<&'static IndexModel> {
    crate::db::query::plan::planner::sorted_model_indexes(indexes)
}

#[derive(Clone)]
struct CandidateScoringIndex {
    contract: SemanticIndexAccessContract,
}

pub(super) fn evaluate_index_candidate(
    family: AccessChoiceFamily,
    index: &IndexModel,
    model: &EntityModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> CandidateEvaluation {
    let index_contract = SemanticIndexAccessContract::from_index(*index);
    let scoring_index = CandidateScoringIndex {
        contract: index_contract,
    };

    if matches!(family, AccessChoiceFamily::Range) && predicate.is_none() && order.is_some() {
        return evaluate_order_only_range_candidate(scoring_index, model, order, grouped);
    }

    let Some(predicate) = predicate else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::PredicateAbsent);
    };

    match family {
        AccessChoiceFamily::Prefix => augment_candidate_with_order_compatibility(
            prefix::evaluate_prefix_candidate(index, schema, predicate),
            model,
            order,
            scoring_index,
            grouped,
        ),
        AccessChoiceFamily::MultiLookup => {
            prefix::evaluate_multi_lookup_candidate(index, schema, predicate)
        }
        AccessChoiceFamily::Range => augment_candidate_with_order_compatibility(
            range::evaluate_range_candidate(index, schema, predicate),
            model,
            order,
            scoring_index,
            grouped,
        ),
        AccessChoiceFamily::NonIndex => {
            CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonIndexAccess)
        }
    }
}

// Project one order-only range-family candidate for explain when planner fell
// back from full-scan predicate planning onto deterministic visible-index
// ordering. The canonical score still carries order compatibility so explain
// can report why one visible index won the fallback.
fn evaluate_order_only_range_candidate(
    scoring_index: CandidateScoringIndex,
    model: &EntityModel,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> CandidateEvaluation {
    CandidateEvaluation::Eligible(candidate_score_with_order_compatibility(
        model,
        order,
        scoring_index,
        0,
        false,
        0,
        grouped,
    ))
}

fn augment_candidate_with_order_compatibility(
    evaluation: CandidateEvaluation,
    model: &EntityModel,
    order: Option<&OrderSpec>,
    scoring_index: CandidateScoringIndex,
    grouped: bool,
) -> CandidateEvaluation {
    match evaluation {
        CandidateEvaluation::Eligible(score) => {
            CandidateEvaluation::Eligible(candidate_score_with_order_compatibility(
                model,
                order,
                scoring_index,
                score.prefix_len,
                score.exact,
                score.range_bound_count,
                grouped,
            ))
        }
        CandidateEvaluation::Rejected(reason) => CandidateEvaluation::Rejected(reason),
    }
}

// Rebuild one candidate score with secondary-order compatibility projected
// from the visible order contract so order-only fallback and normal eligible
// candidate augmentation stay on the same scoring path.
fn candidate_score_with_order_compatibility(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    scoring_index: CandidateScoringIndex,
    prefix_len: usize,
    exact: bool,
    range_bound_count: u8,
    grouped: bool,
) -> crate::db::query::plan::planner::AccessCandidateScore {
    crate::db::query::plan::planner::access_candidate_score_from_index_contract(
        model,
        order,
        scoring_index.contract,
        prefix_len,
        exact,
        range_bound_count,
        grouped,
    )
}
