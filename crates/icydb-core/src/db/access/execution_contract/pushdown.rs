//! Module: db::access::execution_contract::pushdown
//! Responsibility: module-local ownership and contracts for db::access::execution_contract::pushdown.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::plan::{SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection},
        query::plan::OrderSpec,
    },
    model::entity::EntityModel,
};

// Core matcher for secondary ORDER BY pushdown eligibility.
pub(in crate::db::access::execution_contract) fn match_secondary_order_pushdown_core(
    model: &EntityModel,
    order: &OrderSpec,
    index_name: &'static str,
    index_order_terms: &[String],
    prefix_len: usize,
) -> SecondaryOrderPushdownEligibility {
    if order.fields.is_empty() {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::NoOrderBy,
        );
    }
    if !order.has_exact_primary_key_tie_break(model.primary_key.name) {
        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                field: model.primary_key.name.to_string(),
            },
        );
    }

    if order
        .deterministic_secondary_order_direction(model.primary_key.name)
        .is_none()
    {
        let Some((_, expected_direction)) = order.fields.last() else {
            return SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::NoOrderBy,
            );
        };
        let field = order
            .fields
            .iter()
            .find(|(_, direction)| direction != expected_direction)
            .map_or_else(
                || model.primary_key.name.to_string(),
                |(field, _)| field.clone(),
            );

        return SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::MixedDirectionNotEligible { field },
        );
    }

    let matches_expected_suffix = order.matches_expected_term_sequence_plus_primary_key(
        index_order_terms
            .iter()
            .skip(prefix_len)
            .map(String::as_str),
        model.primary_key.name,
    );
    let matches_expected_full = order.matches_expected_term_sequence_plus_primary_key(
        index_order_terms.iter().map(String::as_str),
        model.primary_key.name,
    );
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
            expected_suffix: index_order_terms.iter().skip(prefix_len).cloned().collect(),
            expected_full: index_order_terms.to_vec(),
            actual: order
                .fields
                .iter()
                .take(order.fields.len().saturating_sub(1))
                .map(|(field, _)| field.clone())
                .collect(),
        },
    )
}
