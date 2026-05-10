//! Module: db::query::plan::planner::ranking
//! Responsibility: canonical deterministic candidate ranking for planner-visible index selection.
//! Does not own: predicate eligibility derivation or execution semantics.
//! Boundary: shared ranking contract consumed by planner selection and planner-choice explain.

use crate::{
    db::{
        access::SemanticIndexAccessContract,
        query::plan::{
            OrderSpec, deterministic_secondary_index_order_satisfied,
            deterministic_secondary_index_order_terms_satisfied, grouped_index_order_satisfied,
            grouped_index_order_terms_satisfied, index_key_item_order_terms,
        },
    },
    model::{entity::EntityModel, index::IndexModel},
};
use std::ops::Bound;

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
    pub(in crate::db::query::plan) filtered: bool,
    pub(in crate::db::query::plan) range_bound_count: u8,
    pub(in crate::db::query::plan) order_compatible: bool,
}

impl AccessCandidateScore {
    /// Construct one canonical candidate score from deterministic planner
    /// ranking inputs.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(
        prefix_len: usize,
        exact: bool,
        filtered: bool,
        range_bound_count: u8,
        order_compatible: bool,
    ) -> Self {
        Self {
            prefix_len,
            exact,
            filtered,
            range_bound_count,
            order_compatible,
        }
    }
}

///
/// AndFamilyCandidateScore
///
/// Canonical deterministic comparison inputs for `Predicate::And` family-level
/// access selection after child recursion has already surfaced concrete access
/// candidates. This keeps family competition explicit instead of encoding the
/// same route wins as ad hoc early returns in the planner body.
///

///
/// AndFamilyPriorityClass
///
/// AndFamilyPriorityClass freezes the bounded high-priority non-index family
/// winners that must outrank broader secondary-family access candidates during
/// `AND` family comparison.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::query::plan) enum AndFamilyPriorityClass {
    #[default]
    Ordinary,
    SingletonPrimaryKey,
    ConflictingPrimaryKeyChildren,
    ExplicitEmpty,
}

impl AndFamilyPriorityClass {
    #[must_use]
    const fn rank(self) -> u8 {
        match self {
            Self::Ordinary => 0,
            Self::SingletonPrimaryKey => 1,
            Self::ConflictingPrimaryKeyChildren => 2,
            Self::ExplicitEmpty => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::query::plan) struct AndFamilyCandidateScore {
    pub(in crate::db::query::plan) priority_class: AndFamilyPriorityClass,
    pub(in crate::db::query::plan) preferred_on_required_order: bool,
    pub(in crate::db::query::plan) family_rank: u8,
}

impl AndFamilyCandidateScore {
    /// Construct one canonical family-level score from deterministic planner
    /// comparison inputs.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(
        priority_class: AndFamilyPriorityClass,
        preferred_on_required_order: bool,
        family_rank: u8,
    ) -> Self {
        Self {
            priority_class,
            preferred_on_required_order,
            family_rank,
        }
    }
}

// Compare two `AND`-family candidates under the existing bounded planner
// policy. This preserves the current winner ordering while consolidating the
// decision into one explicit comparison path.
#[must_use]
pub(in crate::db::query::plan) const fn and_family_candidate_score_outranks(
    candidate: AndFamilyCandidateScore,
    best: AndFamilyCandidateScore,
) -> bool {
    if candidate.priority_class.rank() != best.priority_class.rank() {
        return candidate.priority_class.rank() > best.priority_class.rank();
    }
    if candidate.preferred_on_required_order != best.preferred_on_required_order {
        return candidate.preferred_on_required_order;
    }
    if candidate.family_rank != best.family_rank {
        return candidate.family_rank > best.family_rank;
    }

    false
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
    if candidate.filtered != best.filtered {
        return candidate.filtered;
    }
    if candidate.range_bound_count != best.range_bound_count {
        return candidate.range_bound_count > best.range_bound_count;
    }
    if candidate.order_compatible != best.order_compatible {
        return candidate.order_compatible;
    }

    false
}

/// Return how many sides of one planner-visible range are bounded.
#[must_use]
pub(in crate::db::query::plan) const fn range_bound_count<T>(
    lower: &Bound<T>,
    upper: &Bound<T>,
) -> u8 {
    let mut count = 0u8;
    if !matches!(lower, Bound::Unbounded) {
        count = count.saturating_add(1);
    }
    if !matches!(upper, Bound::Unbounded) {
        count = count.saturating_add(1);
    }

    count
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
        let Some(order_contract) = order.and_then(OrderSpec::grouped_index_order_contract) else {
            return false;
        };

        return grouped_index_order_satisfied(&order_contract, index, prefix_len);
    }

    let Some(order_contract) = order
        .and_then(|order| order.deterministic_secondary_order_contract(model.primary_key.name))
    else {
        return false;
    };

    deterministic_secondary_index_order_satisfied(&order_contract, index, prefix_len)
}

/// Project whether one selected access contract can preserve the canonical
/// deterministic secondary ordering contract after consumed prefix items.
#[must_use]
pub(in crate::db::query::plan) fn selected_index_contract_satisfies_secondary_order(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    index: SemanticIndexAccessContract,
    prefix_len: usize,
    grouped: bool,
) -> bool {
    let index_terms = index_key_item_order_terms(index.key_items());

    if grouped {
        let Some(order_contract) = order.and_then(OrderSpec::grouped_index_order_contract) else {
            return false;
        };

        return grouped_index_order_terms_satisfied(
            &order_contract,
            index_terms.as_slice(),
            prefix_len,
        );
    }

    let Some(order_contract) = order
        .and_then(|order| order.deterministic_secondary_order_contract(model.primary_key.name))
    else {
        return false;
    };

    deterministic_secondary_index_order_terms_satisfied(
        &order_contract,
        index_terms.as_slice(),
        prefix_len,
    )
}

/// Build one planner candidate score from the reduced semantic index contract
/// when candidate eligibility still carries a temporary generated bridge.
#[must_use]
pub(in crate::db::query::plan) fn access_candidate_score_from_index_contract(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    index: SemanticIndexAccessContract,
    prefix_len: usize,
    exact: bool,
    range_bound_count: u8,
    grouped: bool,
) -> AccessCandidateScore {
    AccessCandidateScore::new(
        prefix_len,
        exact,
        index.is_filtered(),
        range_bound_count,
        selected_index_contract_satisfies_secondary_order(model, order, index, prefix_len, grouped),
    )
}
