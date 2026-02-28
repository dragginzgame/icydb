//! Module: query::plan::pushdown
//! Responsibility: secondary-index ORDER BY pushdown eligibility matrix.
//! Does not own: logical-plan contracts or executor routing policy.
//! Boundary: planner/explain/executor consume this semantic eligibility surface.

use crate::{
    db::{
        access::{
            AccessPlan, PushdownApplicability, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        direction::Direction,
        query::plan::{AccessPlannedQuery, OrderDirection, ScalarPlan},
    },
    model::entity::EntityModel,
};

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction.as_direction()))
        .collect()
}

fn applicability_from_eligibility(
    eligibility: SecondaryOrderPushdownEligibility,
) -> PushdownApplicability {
    match eligibility {
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy
            | SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        ) => PushdownApplicability::NotApplicable,
        other => PushdownApplicability::Applicable(other),
    }
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

// Evaluate secondary ORDER BY pushdown over one access-planned query.
fn assess_secondary_order_pushdown_for_plan<K>(
    model: &EntityModel,
    order_fields: Option<&[(&str, Direction)]>,
    access_plan: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let Some(order_fields) = order_fields else {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    };
    if order_fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }

    let Some(access) = access_plan.as_path() else {
        if let Some((index, prefix_len)) = access_plan.first_index_range_details() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: index.name,
                    prefix_len,
                },
            );
        }

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
        );
    };
    if let Some((index, values)) = access.as_index_prefix() {
        if values.len() > index.fields.len() {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: values.len(),
                    index_field_len: index.fields.len(),
                },
            );
        }

        return assess_secondary_order_pushdown_for_applicable_shape(
            model,
            order_fields,
            index.name,
            index.fields,
            values.len(),
        );
    }
    if let Some((index, prefix_len)) = access.index_range_details() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: index.name,
                prefix_len,
            },
        );
    }

    SecondaryOrderPushdownEligibility::Rejected(
        SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
    )
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown_from_parts<K>(
    model: &EntityModel,
    logical: &ScalarPlan,
    access: &AccessPlan<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = logical
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_for_plan(model, order_fields.as_deref(), access)
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    assess_secondary_order_pushdown_from_parts(model, plan.scalar_plan(), &plan.access)
}

/// Derive pushdown applicability from one plan already validated by planner semantics.
pub(in crate::db) fn derive_secondary_pushdown_applicability_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let logical = plan.scalar_plan();
    debug_assert!(
        !matches!(logical.order.as_ref(), Some(order) if order.fields.is_empty()),
        "validated plan must not contain an empty ORDER BY specification",
    );

    applicability_from_eligibility(assess_secondary_order_pushdown(model, plan))
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    derive_secondary_pushdown_applicability_validated(model, plan)
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
#[cfg(test)]
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    derive_secondary_pushdown_applicability_validated(model, plan)
}
