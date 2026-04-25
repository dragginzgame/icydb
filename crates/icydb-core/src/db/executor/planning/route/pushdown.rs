//! Module: db::executor::planning::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::{
    db::{
        access::AccessCapabilities,
        executor::route::{PushdownApplicability, SecondaryOrderPushdownRejection},
        query::plan::{
            AccessPlannedQuery, DeterministicSecondaryOrderContract, LogicalPushdownEligibility,
            PlannerRouteProfile, access_satisfies_deterministic_secondary_order_contract,
            deterministic_secondary_index_order_compatibility,
        },
    },
    model::index::IndexModel,
};

fn validated_secondary_order_contract(
    planner_route_profile: &PlannerRouteProfile,
) -> Option<&DeterministicSecondaryOrderContract> {
    secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
        .then_some(())?;

    planner_route_profile.secondary_order_contract()
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access capabilities. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract(
    access_capabilities: &AccessCapabilities,
    planner_route_profile: &PlannerRouteProfile,
) -> PushdownApplicability {
    let Some(order_contract) = validated_secondary_order_contract(planner_route_profile) else {
        return PushdownApplicability::NotApplicable;
    };

    secondary_order_pushdown_applicability(access_capabilities, order_contract)
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    order_contract: &DeterministicSecondaryOrderContract,
    index_name: &'static str,
    index: &IndexModel,
    prefix_len: usize,
) -> PushdownApplicability {
    let compatibility =
        deterministic_secondary_index_order_compatibility(order_contract, index, prefix_len);
    if compatibility.is_satisfied() {
        return PushdownApplicability::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    PushdownApplicability::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: compatibility.index_suffix_terms(prefix_len),
            expected_full: compatibility.index_terms().to_vec(),
            actual: order_contract.non_primary_key_terms().to_vec(),
        },
    )
}

/// Derive secondary ORDER BY pushdown applicability from route-owned access
/// capabilities and one planner-owned deterministic ORDER BY contract.
#[must_use]
pub(in crate::db) fn secondary_order_pushdown_applicability(
    access_capabilities: &AccessCapabilities,
    order_contract: &DeterministicSecondaryOrderContract,
) -> PushdownApplicability {
    if !access_capabilities.is_single_path() {
        if let Some(details) = access_capabilities.first_index_range_details() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: details.index().name(),
                    prefix_len: details.slot_arity(),
                },
            );
        }

        return PushdownApplicability::NotApplicable;
    }

    if let Some(details) = access_capabilities.single_path_index_prefix_details() {
        let index = details.index();
        let prefix_len = details.slot_arity();
        if prefix_len > index.fields().len() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len,
                    index_field_len: index.fields().len(),
                },
            );
        }
        return match_secondary_order_pushdown_core(
            order_contract,
            index.name(),
            &index,
            prefix_len,
        );
    }

    if let Some(details) = access_capabilities.single_path_index_range_details() {
        let index = details.index();
        let prefix_len = details.slot_arity();
        if prefix_len > index.fields().len() {
            return PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len,
                    index_field_len: index.fields().len(),
                },
            );
        }
        let applicability =
            match_secondary_order_pushdown_core(order_contract, index.name(), &index, prefix_len);
        return match applicability {
            PushdownApplicability::Eligible { .. } => applicability,
            PushdownApplicability::Rejected(_) => PushdownApplicability::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: index.name(),
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
pub(in crate::db::executor::planning::route) fn index_range_limit_pushdown_shape_supported_for_order_contract(
    access_capabilities: &AccessCapabilities,
    order_contract: Option<&DeterministicSecondaryOrderContract>,
    order_present: bool,
) -> bool {
    if !access_capabilities.is_single_path() {
        return false;
    }
    let Some(details) = access_capabilities.single_path_index_range_details() else {
        return false;
    };
    let index = details.index();
    let prefix_len = details.slot_arity();

    if !order_present {
        return true;
    }
    let Some(order_contract) = order_contract else {
        return false;
    };
    deterministic_secondary_index_order_compatibility(order_contract, &index, prefix_len)
        .is_satisfied()
}

/// Return whether planner logical pushdown eligibility allows route-level
/// secondary-order contracts to remain active.
pub(in crate::db::executor) const fn secondary_order_contract_active(
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> bool {
    logical_pushdown_eligibility.secondary_order_allowed()
        && !logical_pushdown_eligibility.requires_full_materialization()
}

/// Return whether access traversal already satisfies the logical `ORDER BY`
/// contract under planner-owned pushdown eligibility decisions.
pub(in crate::db::executor) fn access_order_satisfied_by_route_contract(
    plan: &AccessPlannedQuery,
) -> bool {
    let access_capabilities = plan.access_capabilities();

    access_order_satisfied_by_route_contract_with_capabilities(plan, &access_capabilities)
}

pub(in crate::db::executor::planning::route) fn access_order_satisfied_by_route_contract_with_capabilities(
    plan: &AccessPlannedQuery,
    access_capabilities: &AccessCapabilities,
) -> bool {
    let logical = plan.scalar_plan();
    let Some(order) = logical.order.as_ref() else {
        return false;
    };
    let planner_route_profile = plan.planner_route_profile();
    let has_order_fields = !order.fields.is_empty();
    // `ORDER BY primary_key` is satisfied by access shapes whose final stream
    // order is already primary-key ordered. Secondary index paths stay ordered,
    // but that order is owned by the index key, so they must not claim PK-order
    // satisfaction merely because they are monotonic.
    let access_uses_index = access_capabilities
        .single_path_index_prefix_details()
        .is_some()
        || access_capabilities
            .single_path_index_range_details()
            .is_some();
    let primary_key_order_satisfied =
        order.is_primary_key_only(plan.primary_key_name()) && !access_uses_index;
    let secondary_pushdown_eligible = validated_secondary_order_contract(planner_route_profile)
        .is_some_and(|order_contract| {
            access_satisfies_deterministic_secondary_order_contract(
                access_capabilities,
                order_contract,
            )
        });

    has_order_fields && (primary_key_order_satisfied || secondary_pushdown_eligible)
}
