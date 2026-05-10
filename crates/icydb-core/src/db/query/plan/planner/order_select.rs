//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        query::plan::{
            AcceptedPlannerFieldPathIndex, OrderSpec,
            deterministic_secondary_index_order_satisfied,
            deterministic_secondary_index_order_terms_satisfied, grouped_index_order_satisfied,
            grouped_index_order_terms_satisfied,
        },
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::ops::Bound;

/// Select one whole-index range scan for generated/model-only planning.
/// Runtime accepted planning must use `index_range_from_order_with_accepted_indexes`.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order_for_generated_model_only(
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

    for &index in candidate_indexes {
        if grouped {
            let Some(order_contract) = grouped_order_contract.as_ref() else {
                continue;
            };
            if !grouped_index_order_satisfied(order_contract, index, 0) {
                continue;
            }
        } else {
            let Some(order_contract) = scalar_order_contract.as_ref() else {
                continue;
            };
            if !deterministic_secondary_index_order_satisfied(order_contract, index, 0) {
                continue;
            }
        }

        return Some(whole_index_ordered_range_scan(index));
    }

    None
}

/// Select one whole-index range scan, using accepted field-path index contracts
/// for field-path indexes and the explicit generated lane for expression
/// indexes until accepted expression contracts exist.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order_with_accepted_indexes(
    model: &EntityModel,
    candidate_indexes: &[&'static IndexModel],
    accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],
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
    for &index in candidate_indexes {
        let selected_index = if let Some(accepted) =
            accepted_field_path_index_for_bridge(accepted_field_path_indexes, index)
        {
            let accepted_order_terms = accepted.order_terms();
            if grouped {
                let Some(order_contract) = grouped_order_contract.as_ref() else {
                    continue;
                };
                if !grouped_index_order_terms_satisfied(order_contract, &accepted_order_terms, 0) {
                    continue;
                }
            } else {
                let Some(order_contract) = scalar_order_contract.as_ref() else {
                    continue;
                };
                if !deterministic_secondary_index_order_terms_satisfied(
                    order_contract,
                    &accepted_order_terms,
                    0,
                ) {
                    continue;
                }
            }

            accepted.generated_index_bridge()
        } else {
            if !index.has_expression_key_items() {
                continue;
            }
            if grouped {
                let Some(order_contract) = grouped_order_contract.as_ref() else {
                    continue;
                };
                if !grouped_index_order_satisfied(order_contract, index, 0) {
                    continue;
                }
            } else {
                let Some(order_contract) = scalar_order_contract.as_ref() else {
                    continue;
                };
                if !deterministic_secondary_index_order_satisfied(order_contract, index, 0) {
                    continue;
                }
            }

            index
        };

        return Some(whole_index_ordered_range_scan(selected_index));
    }

    None
}

fn whole_index_ordered_range_scan(index: &'static IndexModel) -> AccessPlan<Value> {
    // Encode one whole-index ordered scan as an unbounded index-range with
    // zero equality prefix. The first index slot becomes the range anchor
    // while lower layers own forward vs reverse traversal from ORDER BY.
    let spec = SemanticIndexRangeSpec::new(
        *index,
        vec![0usize],
        Vec::new(),
        Bound::Unbounded,
        Bound::Unbounded,
    );

    AccessPlan::index_range(spec)
}

fn accepted_field_path_index_for_bridge<'a>(
    accepted_field_path_indexes: &'a [AcceptedPlannerFieldPathIndex],
    index: &IndexModel,
) -> Option<&'a AcceptedPlannerFieldPathIndex> {
    accepted_field_path_indexes
        .iter()
        .find(|accepted| accepted.generated_index_bridge().name() == index.name())
}
