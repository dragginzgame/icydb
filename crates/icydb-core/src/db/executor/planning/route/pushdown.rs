//! Module: executor::planning::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned access-shape assessment over validated logical+access plans.

use crate::db::{
    access::{AccessPathKind, AccessShapeFacts, IndexShapeDetails, SemanticIndexKeyItemsRef},
    direction::Direction,
    executor::route::{
        IndexPrefixChildExpansionHint, PushdownApplicability, SecondaryOrderPushdownRejection,
    },
    query::plan::{
        AccessPlannedQuery, DeterministicSecondaryOrderContract, LogicalPushdownEligibility,
        OrderDirection, PlannerRouteProfile,
        access_satisfies_deterministic_secondary_order_contract,
        deterministic_secondary_index_key_items_order_compatibility,
    },
};

const MAX_INDEX_PREFIX_CHILD_EXPANSION_PREFIXES: usize = 32;

fn validated_secondary_order_contract(
    planner_route_profile: &PlannerRouteProfile,
) -> Option<&DeterministicSecondaryOrderContract> {
    secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
        .then_some(())?;

    planner_route_profile.secondary_order_contract()
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access-shape facts. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract(
    access_shape_facts: &AccessShapeFacts,
    planner_route_profile: &PlannerRouteProfile,
) -> PushdownApplicability {
    let Some(order_contract) = validated_secondary_order_contract(planner_route_profile) else {
        return PushdownApplicability::NotApplicable;
    };

    secondary_order_pushdown_applicability(access_shape_facts, order_contract)
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    order_contract: &DeterministicSecondaryOrderContract,
    index_name: &str,
    key_items: SemanticIndexKeyItemsRef<'_>,
    prefix_len: usize,
) -> PushdownApplicability {
    let compatibility = deterministic_secondary_index_key_items_order_compatibility(
        order_contract,
        key_items,
        prefix_len,
    );
    if compatibility.is_satisfied() {
        return PushdownApplicability::Eligible {
            index: index_name.to_string(),
            prefix_len,
        };
    }

    PushdownApplicability::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name.to_string(),
            prefix_len,
            expected_suffix: compatibility.index_suffix_terms(prefix_len),
            expected_full: compatibility.index_terms().to_vec(),
            actual: order_contract.non_primary_key_terms().to_vec(),
        },
    )
}

/// Derive secondary ORDER BY pushdown applicability from route-owned access
/// access-shape facts and one planner-owned deterministic ORDER BY contract.
#[must_use]
fn secondary_order_pushdown_applicability(
    access_shape_facts: &AccessShapeFacts,
    order_contract: &DeterministicSecondaryOrderContract,
) -> PushdownApplicability {
    if !access_shape_facts.is_single_path() {
        if let Some(details) = access_shape_facts.first_index_range_details() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: details.name().to_string(),
                    prefix_len: details.slot_arity(),
                },
            );
        }

        return PushdownApplicability::NotApplicable;
    }

    if let Some(details) = access_shape_facts.single_path_index_prefix_details() {
        let index_name = details.name();
        let prefix_len = details.slot_arity();
        if prefix_len > details.key_arity() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len,
                    index_field_len: details.key_arity(),
                },
            );
        }
        return match_secondary_order_pushdown_core(
            order_contract,
            index_name,
            details.key_items(),
            prefix_len,
        );
    }

    if let Some(details) = access_shape_facts.single_path_index_range_details() {
        let index_name = details.name();
        let prefix_len = details.slot_arity();
        if prefix_len > details.key_arity() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len,
                    index_field_len: details.key_arity(),
                },
            );
        }
        let applicability = match_secondary_order_pushdown_core(
            order_contract,
            index_name,
            details.key_items(),
            prefix_len,
        );
        return match applicability {
            PushdownApplicability::Eligible { .. } => applicability,
            PushdownApplicability::Rejected(_) => PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: index_name.to_string(),
                    prefix_len,
                },
            ),
            PushdownApplicability::NotApplicable => PushdownApplicability::NotApplicable,
        };
    }

    PushdownApplicability::NotApplicable
}

/// Return true when this access shape supports index-range limit pushdown for
/// the supplied planner-owned deterministic ORDER BY contract.
#[must_use]
pub(super) fn index_range_limit_pushdown_shape_supported_for_order_contract(
    access_shape_facts: &AccessShapeFacts,
    order_contract: Option<&DeterministicSecondaryOrderContract>,
    order_present: bool,
) -> bool {
    if !access_shape_facts.is_single_path() {
        return false;
    }
    let Some(details) = access_shape_facts.single_path_index_range_details() else {
        return false;
    };
    let prefix_len = details.slot_arity();

    if !order_present {
        return true;
    }
    let Some(order_contract) = order_contract else {
        return false;
    };
    deterministic_secondary_index_key_items_order_compatibility(
        order_contract,
        details.key_items(),
        prefix_len,
    )
    .is_satisfied()
}

/// Return whether planner logical pushdown eligibility allows route-level
/// secondary-order contracts to remain active.
pub(super) const fn secondary_order_contract_active(
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> bool {
    logical_pushdown_eligibility.secondary_order_allowed()
        && !logical_pushdown_eligibility.requires_full_materialization()
}

/// Return whether access traversal already satisfies the logical `ORDER BY`
/// contract under planner-owned pushdown eligibility decisions.
pub(in crate::db::executor) fn access_order_satisfied_by_route_mode(
    plan: &AccessPlannedQuery,
) -> bool {
    let access_shape_facts = plan.access_shape_facts();

    access_order_satisfied_by_route_mode_with_access_shape_facts(plan, &access_shape_facts)
}

pub(super) fn access_order_satisfied_by_route_mode_with_access_shape_facts(
    plan: &AccessPlannedQuery,
    access_shape_facts: &AccessShapeFacts,
) -> bool {
    let logical = plan.scalar_plan();
    let Some(order) = logical.order.as_ref() else {
        return false;
    };
    let planner_route_profile = plan.planner_route_profile();
    let has_order_fields = !order.fields.is_empty();
    // `ORDER BY primary_key` is satisfied by access shapes whose final stream
    // order is already primary-key ordered. Most secondary index paths are
    // ordered by their index keys, not by the primary key. The narrow exception
    // is an ASC index-prefix family whose consumed prefix leaves the primary
    // key as the exact remaining index suffix, so each prefix stream can be
    // merged by decoded primary key without a materialized sort.
    let primary_key_order_satisfied = order
        .primary_key_only_direction_fields(&plan.primary_key_names())
        .is_some_and(|direction| {
            let direction = match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            };

            access_preserves_primary_key_order_for_route_direction_with_access_shape_facts(
                plan,
                access_shape_facts,
                direction,
                true,
            )
        });
    let secondary_pushdown_eligible = validated_secondary_order_contract(planner_route_profile)
        .is_some_and(|order_contract| {
            access_satisfies_deterministic_secondary_order_contract(
                access_shape_facts,
                order_contract,
            )
        });

    has_order_fields && (primary_key_order_satisfied || secondary_pushdown_eligible)
}

/// Return whether the selected access route can produce primary-key order
/// without relying on metadata-backed child-prefix expansion.
#[must_use]
pub(in crate::db::executor) fn access_preserves_primary_key_order_without_child_expansion(
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> bool {
    let access_shape_facts = plan.access_shape_facts();

    access_preserves_primary_key_order_for_route_direction_with_access_shape_facts(
        plan,
        &access_shape_facts,
        direction,
        false,
    )
}

fn access_preserves_primary_key_order_for_route_direction_with_access_shape_facts(
    plan: &AccessPlannedQuery,
    access_shape_facts: &AccessShapeFacts,
    direction: Direction,
    allow_child_expansion: bool,
) -> bool {
    let access_uses_index = access_shape_facts
        .single_path_index_prefix_details()
        .is_some()
        || access_shape_facts
            .single_path_index_range_details()
            .is_some();
    if !access_uses_index {
        return true;
    }

    matches!(direction, Direction::Asc)
        && (index_prefix_family_preserves_primary_key_suffix_order(
            access_shape_facts,
            plan.primary_key_names().as_slice(),
        ) || (allow_child_expansion
            && ordered_child_prefix_expansion_target_for_primary_key_direction(
                plan,
                access_shape_facts,
                direction,
            )
            .is_some()))
}

fn index_prefix_family_preserves_primary_key_suffix_order(
    access_shape_facts: &AccessShapeFacts,
    primary_key_names: &[&str],
) -> bool {
    let Some(single_path) = access_shape_facts.single_path_facts() else {
        return false;
    };
    if !matches!(
        single_path.kind(),
        AccessPathKind::IndexPrefix
            | AccessPathKind::IndexMultiLookup
            | AccessPathKind::IndexBranchSet
    ) {
        return false;
    }

    let Some(details) = single_path.index_prefix_details() else {
        return false;
    };

    index_suffix_matches_primary_key_order(&details, primary_key_names)
}

fn index_suffix_matches_primary_key_order(
    index: &IndexShapeDetails,
    primary_key_names: &[&str],
) -> bool {
    index_suffix_matches_primary_key_order_from_prefix(index, index.slot_arity(), primary_key_names)
}

/// Return the expanded prefix length when a sparse multi-lookup prefix can be
/// expanded by exactly one exact child slot so the remaining suffix is the
/// primary-key order suffix.
#[must_use]
pub(in crate::db::executor::planning::route) fn ordered_child_prefix_expansion_target_for_route(
    plan: &AccessPlannedQuery,
    access_shape_facts: &AccessShapeFacts,
) -> Option<usize> {
    let direction = plan
        .scalar_plan()
        .order
        .as_ref()?
        .primary_key_only_direction_fields(&plan.primary_key_names())?;
    if !matches!(direction, OrderDirection::Asc) {
        return None;
    }

    ordered_child_prefix_expansion_target_for_primary_key_direction(
        plan,
        access_shape_facts,
        Direction::Asc,
    )
}

fn ordered_child_prefix_expansion_target_for_primary_key_direction(
    plan: &AccessPlannedQuery,
    access_shape_facts: &AccessShapeFacts,
    direction: Direction,
) -> Option<usize> {
    if !matches!(direction, Direction::Asc) {
        return None;
    }

    let single_path = access_shape_facts.single_path_facts()?;
    if !matches!(single_path.kind(), AccessPathKind::IndexMultiLookup) {
        return None;
    }

    let details = single_path.index_prefix_details()?;
    let expanded_prefix_len = details.slot_arity().checked_add(1)?;
    if expanded_prefix_len >= details.key_arity() {
        return None;
    }

    index_suffix_matches_primary_key_order_from_prefix(
        &details,
        expanded_prefix_len,
        plan.primary_key_names().as_slice(),
    )
    .then_some(expanded_prefix_len)
}

#[must_use]
pub(in crate::db::executor) fn index_prefix_child_expansion_hint_for_plan(
    plan: &AccessPlannedQuery,
) -> Option<IndexPrefixChildExpansionHint> {
    ordered_child_prefix_expansion_target_for_route(plan, &plan.access_shape_facts()).map(
        |target_prefix_len| {
            IndexPrefixChildExpansionHint::new(
                target_prefix_len,
                MAX_INDEX_PREFIX_CHILD_EXPANSION_PREFIXES,
            )
        },
    )
}

fn index_suffix_matches_primary_key_order_from_prefix(
    index: &IndexShapeDetails,
    prefix_len: usize,
    primary_key_names: &[&str],
) -> bool {
    let key_arity = index.key_arity();
    if prefix_len > key_arity {
        return false;
    }
    if prefix_len == key_arity {
        return !primary_key_names.is_empty();
    }
    if key_arity.saturating_sub(prefix_len) != primary_key_names.len() {
        return false;
    }

    primary_key_names
        .iter()
        .enumerate()
        .all(|(offset, name)| index.key_field_at(prefix_len + offset) == Some(*name))
}
