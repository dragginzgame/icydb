//! Module: db::query::plan::planner::ranking
//! Responsibility: canonical deterministic candidate ranking for planner-visible index selection.
//! Does not own: predicate eligibility derivation or execution semantics.
//! Boundary: shared ranking contract consumed by planner selection and planner-choice explain.

use crate::{
    db::query::plan::{OrderSpec, index_order_terms},
    model::{entity::EntityModel, index::IndexModel},
};

///
/// AccessCandidateScore
///
/// AccessCandidateScore carries the canonical deterministic comparison inputs
/// for one planner-visible index candidate.
/// Planner selection and planner-choice explain both consume this score so
/// deterministic tie-break policy does not drift across those surfaces.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::query::plan) struct AccessCandidateScore {
    pub(in crate::db::query::plan) prefix_len: usize,
    pub(in crate::db::query::plan) exact: bool,
    pub(in crate::db::query::plan) order_compatible: bool,
}

impl AccessCandidateScore {
    /// Construct one canonical candidate score from deterministic planner
    /// ranking inputs.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(
        prefix_len: usize,
        exact: bool,
        order_compatible: bool,
    ) -> Self {
        Self {
            prefix_len,
            exact,
            order_compatible,
        }
    }
}

// Compare two candidate scores under one family-specific exact-match policy.
// The remaining structural tie-breaker on index name stays at the call site so
// local loops can keep their existing selected payload ownership.
#[must_use]
pub(in crate::db::query::plan) const fn access_candidate_score_outranks(
    candidate: AccessCandidateScore,
    best: AccessCandidateScore,
    exact_priority: bool,
) -> bool {
    if candidate.prefix_len != best.prefix_len {
        return candidate.prefix_len > best.prefix_len;
    }
    if exact_priority && candidate.exact != best.exact {
        return candidate.exact;
    }
    if candidate.order_compatible != best.order_compatible {
        return candidate.order_compatible;
    }

    false
}

// Project whether one index candidate can preserve the canonical deterministic
// secondary ordering contract after consuming `prefix_len` equality-bound key
// items.
#[must_use]
pub(in crate::db::query::plan) fn candidate_satisfies_secondary_order(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    index: &IndexModel,
    prefix_len: usize,
    grouped: bool,
) -> bool {
    if grouped {
        return grouped_order_matches_index(order, index, prefix_len);
    }

    let Some(order_contract) = order
        .and_then(|order| order.deterministic_secondary_order_contract(model.primary_key.name))
    else {
        return false;
    };

    let index_terms = index_order_terms(index);

    order_contract.matches_index_suffix(&index_terms, prefix_len)
        || order_contract.matches_index_full(&index_terms)
}

// Grouped access planning preserves declared grouped ORDER BY terms directly
// instead of routing through the scalar `..., primary_key` tie-break contract.
// Once an equality prefix is fixed by predicate planning, either the full index
// order or the remaining suffix can still satisfy the grouped order.
fn grouped_order_matches_index(
    order: Option<&OrderSpec>,
    index: &IndexModel,
    prefix_len: usize,
) -> bool {
    let Some(order) = order else {
        return false;
    };
    let Some(direction) = order
        .fields
        .first()
        .map(crate::db::query::plan::OrderTerm::direction)
    else {
        return false;
    };
    if order
        .fields
        .iter()
        .any(|term| term.direction() != direction)
    {
        return false;
    }

    let order_terms = order
        .fields
        .iter()
        .map(crate::db::query::plan::OrderTerm::rendered_label)
        .collect::<Vec<_>>();
    let index_terms = index_order_terms(index);

    order_terms == index_terms
        || (prefix_len <= index_terms.len() && order_terms == index_terms[prefix_len..])
}
