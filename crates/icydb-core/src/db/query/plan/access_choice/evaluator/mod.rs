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
        query::construction::ConstructionBudget,
        query::plan::{
            access_choice::model::{
                AccessChoiceFamily, AccessChoiceRejectedReason, CandidateEvaluation,
            },
            order_contract::CandidateOrderContract,
            planner::{
                access_candidate_score_from_index_contract, index_stream_is_complete_for_query,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
};

pub(in crate::db::query::plan::access_choice) use ranking::{
    CandidateRankingEvidence, chosen_access_shape_projection, ranked_rejection_reason,
};

pub(super) fn evaluate_index_candidate(
    family: AccessChoiceFamily,
    index: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
    has_order: bool,
    order: Option<&CandidateOrderContract>,
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    if !index_stream_is_complete_for_query(
        schema,
        index,
        predicate.unwrap_or(&Predicate::True),
        budget,
    )? {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::IndexMembershipUnproven,
        ));
    }

    // A supplied but incompatible order still admits this diagnostic candidate;
    // its score reports incompatibility rather than an absent predicate/order.
    if matches!(family, AccessChoiceFamily::Range) && predicate.is_none() && has_order {
        return Ok(CandidateEvaluation::Eligible(
            access_candidate_score_from_index_contract(order, index, 0, false, 0),
        ));
    }

    let Some(predicate) = predicate else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateAbsent,
        ));
    };

    Ok(match family {
        AccessChoiceFamily::Prefix => augment_candidate_with_order_compatibility(
            prefix::evaluate_prefix_candidate(index, schema, predicate, budget)?,
            index,
            order,
        ),
        AccessChoiceFamily::MultiLookup => augment_candidate_with_order_compatibility(
            prefix::evaluate_multi_lookup_candidate_from_contract(
                index, schema, predicate, budget,
            )?,
            index,
            order,
        ),
        AccessChoiceFamily::BranchSet => augment_candidate_with_order_compatibility(
            prefix::evaluate_branch_set_candidate_from_contract(index, schema, predicate, budget)?,
            index,
            order,
        ),
        AccessChoiceFamily::Range => augment_candidate_with_order_compatibility(
            range::evaluate_range_candidate_from_contract(index, schema, predicate, budget)?,
            index,
            order,
        ),
        AccessChoiceFamily::NonIndex => {
            CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonIndexAccess)
        }
    })
}

// Structural evidence keeps its owner; ordering borrows the pass's prepared facts.
fn augment_candidate_with_order_compatibility(
    evaluation: CandidateEvaluation,
    index: &SemanticIndexAccessContract,
    order: Option<&CandidateOrderContract>,
) -> CandidateEvaluation {
    match evaluation {
        CandidateEvaluation::Eligible(score) => {
            CandidateEvaluation::Eligible(access_candidate_score_from_index_contract(
                order,
                index,
                score.prefix_len,
                score.exact,
                score.range_bound_count,
            ))
        }
        CandidateEvaluation::Rejected(reason) => CandidateEvaluation::Rejected(reason),
    }
}
