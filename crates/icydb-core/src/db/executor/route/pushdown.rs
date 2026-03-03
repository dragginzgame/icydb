//! Module: db::executor::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::{
    db::{
        access::{
            PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        direction::Direction,
        executor::{derive_access_capabilities, route::direction_from_order},
        query::plan::{
            AccessPlannedQuery, LogicalPushdownEligibility, OrderDirection, ScalarPlan,
            lower_executable_access_plan,
        },
    },
    model::entity::EntityModel,
};

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
        .collect()
}

// Core matcher for secondary ORDER BY pushdown eligibility.
fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    let Some((last_field, last_direction)) = order_fields.last() else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if *last_field != model.primary_key.name {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    let expected_direction = *last_direction;
    for (field, direction) in order_fields {
        if *direction != expected_direction {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: (*field).to_string(),
                },
            );
        }
    }

    let actual_non_pk_len = order_fields.len().saturating_sub(1);
    let matches_expected_suffix = actual_non_pk_len
        == index_fields.len().saturating_sub(prefix_len)
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().skip(prefix_len).copied())
            .all(|(actual, expected)| actual == expected);
    let matches_expected_full = actual_non_pk_len == index_fields.len()
        && order_fields
            .iter()
            .take(actual_non_pk_len)
            .map(|(field, _)| *field)
            .zip(index_fields.iter().copied())
            .all(|(actual, expected)| actual == expected);
    if matches_expected_suffix || matches_expected_full {
        return SecondaryOrderPushdownEligibility::Eligible {
            index: index_name,
            prefix_len,
        };
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
            index: index_name,
            prefix_len,
            expected_suffix: index_fields
                .iter()
                .skip(prefix_len)
                .map(|field| (*field).to_string())
                .collect(),
            expected_full: index_fields
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
            actual: order_fields
                .iter()
                .take(actual_non_pk_len)
                .map(|(field, _)| (*field).to_string())
                .collect(),
        },
    )
}

// Evaluate pushdown eligibility for ORDER BY + single index-prefix shapes.
fn assess_secondary_order_pushdown_for_applicable_shape(
    model: &EntityModel,
    order_fields: &[(&str, Direction)],
    index_name: &'static str,
    index_fields: &[&'static str],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    match_secondary_order_pushdown_core(model, order_fields, index_name, index_fields, prefix_len)
}

fn validated_secondary_order_fields_for_contract<'a>(
    model: &EntityModel,
    logical: &'a ScalarPlan,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> Option<Vec<(&'a str, Direction)>> {
    if !logical_pushdown_eligibility.secondary_order_allowed()
        || logical_pushdown_eligibility.requires_full_materialization()
    {
        return None;
    }

    let order_fields = logical
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields))?;

    debug_assert!(
        !order_fields.is_empty(),
        "planner-pushed secondary-order eligibility requires at least one ORDER BY field",
    );
    let (last_field, expected_direction) = order_fields.last()?;
    debug_assert_eq!(
        *last_field, model.primary_key.name,
        "planner-pushed secondary-order eligibility requires primary-key tie-break field",
    );
    debug_assert!(
        order_fields
            .iter()
            .all(|(_, direction)| *direction == *expected_direction),
        "planner-pushed secondary-order eligibility requires one uniform ORDER BY direction",
    );

    Some(order_fields)
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access capabilities. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> PushdownApplicability {
    let Some(order_fields) = validated_secondary_order_fields_for_contract(
        model,
        plan.scalar_plan(),
        logical_pushdown_eligibility,
    ) else {
        return PushdownApplicability::NotApplicable;
    };

    let executable_plan = lower_executable_access_plan(&plan.access);
    let access_capabilities = derive_access_capabilities(&executable_plan);
    let Some(single_path) = access_capabilities.single_path() else {
        if let Some(details) = access_capabilities.first_index_range_details() {
            return PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: details.index().name,
                    prefix_len: details.slot_arity(),
                },
            ));
        }

        return PushdownApplicability::NotApplicable;
    };

    if let Some(details) = single_path.index_prefix_details() {
        let index = details.index();
        let prefix_len = details.slot_arity();
        if prefix_len > index.fields.len() {
            return PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len,
                    index_field_len: index.fields.len(),
                },
            ));
        }

        return PushdownApplicability::Applicable(
            assess_secondary_order_pushdown_for_applicable_shape(
                model,
                &order_fields,
                index.name,
                index.fields,
                prefix_len,
            ),
        );
    }

    if let Some(details) = single_path.index_range_details() {
        return PushdownApplicability::Applicable(SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: details.index().name,
                prefix_len: details.slot_arity(),
            },
        ));
    }

    PushdownApplicability::NotApplicable
}
