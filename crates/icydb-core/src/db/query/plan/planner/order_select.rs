//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract, SemanticIndexRangeSpec},
        predicate::Predicate,
        query::plan::{
            OrderSpec, deterministic_secondary_index_key_items_satisfied,
            grouped_index_key_items_satisfied, planner::index_stream_is_complete_for_query,
        },
        schema::SchemaInfo,
    },
    value::Value,
};
use std::ops::Bound;

/// Select one whole-index range scan from accepted semantic index contracts.
///
/// Accepted-schema construction has already reduced each candidate to its
/// semantic contract, so ordinary planning and access-choice reranking share
/// this authority without reopening generated model metadata.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order_with_semantic_indexes(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    let grouped_order_contract = grouped
        .then_some(order)
        .flatten()
        .and_then(OrderSpec::grouped_index_order_contract);
    let scalar_order_contract = (!grouped).then_some(order).flatten().and_then(|order| {
        order.deterministic_secondary_order_contract_fields(schema.shared_primary_key_names())
    });

    for index in candidate_indexes {
        if !index_stream_is_complete_for_query(schema, index, query_predicate) {
            continue;
        }
        let satisfied = if grouped {
            grouped_order_contract.as_ref().is_some_and(|contract| {
                grouped_index_key_items_satisfied(contract, index.key_items(), 0)
            })
        } else {
            scalar_order_contract.as_ref().is_some_and(|contract| {
                deterministic_secondary_index_key_items_satisfied(contract, index.key_items(), 0)
            })
        };
        if satisfied {
            return Some(whole_index_ordered_range_scan_from_contract(index.clone()));
        }
    }

    None
}

fn whole_index_ordered_range_scan_from_contract(
    index: SemanticIndexAccessContract,
) -> AccessPlan<Value> {
    // Encode one whole-index ordered scan as an unbounded index-range with
    // zero equality prefix. The first index slot becomes the range anchor
    // while lower layers own forward vs reverse traversal from ORDER BY.
    let spec = SemanticIndexRangeSpec::from_access_contract(
        index,
        vec![0usize],
        Vec::new(),
        Bound::Unbounded,
        Bound::Unbounded,
    );

    AccessPlan::index_range(spec)
}
