//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        query::plan::{
            DeterministicSecondaryIndexOrderMatch, GroupedIndexOrderMatch, OrderSpec,
            index_order_terms,
        },
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::ops::Bound;

/// Select one whole-index range scan when canonical ORDER BY already matches a
/// deterministic secondary index traversal contract.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order(
    model: &EntityModel,
    candidate_indexes: &[&'static IndexModel],
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    let grouped_order_contract = grouped
        .then_some(order)
        .flatten()
        .and_then(OrderSpec::grouped_index_order_contract);
    let scalar_order_contract = (!grouped)
        .then_some(order)
        .flatten()
        .and_then(|order| order.deterministic_secondary_order_contract(model.primary_key.name));

    // Order-driven access fallback is only valid when the canonical ORDER BY
    // already carries one uniform-direction `..., primary_key` tie-break
    // shape. The caller prefilters candidate indexes so filtered guards are
    // checked once at the planner entry boundary.
    for index in candidate_indexes {
        let index_terms = index_order_terms(index);
        if grouped {
            let Some(order_contract) = grouped_order_contract.as_ref() else {
                continue;
            };
            if matches!(
                order_contract.classify_index_match(&index_terms, 0),
                GroupedIndexOrderMatch::None
            ) {
                continue;
            }
        } else {
            let Some(order_contract) = scalar_order_contract.as_ref() else {
                continue;
            };
            if matches!(
                order_contract.classify_index_match(&index_terms, 0),
                DeterministicSecondaryIndexOrderMatch::None
            ) {
                continue;
            }
        }

        // Encode one whole-index ordered scan as an unbounded index-range with
        // zero equality prefix. The first index slot becomes the range anchor
        // while lower layers own forward vs reverse traversal from ORDER BY.
        let spec = SemanticIndexRangeSpec::new(
            **index,
            vec![0usize],
            Vec::new(),
            Bound::Unbounded,
            Bound::Unbounded,
        );

        return Some(AccessPlan::index_range(spec));
    }

    None
}
