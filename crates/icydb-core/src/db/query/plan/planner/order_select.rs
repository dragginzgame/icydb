//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract, SemanticIndexRangeSpec},
        predicate::Predicate,
        query::plan::{
            OrderSpec, deterministic_secondary_index_order_terms_satisfied,
            grouped_index_order_terms_satisfied, index_key_item_order_terms,
        },
        schema::SchemaInfo,
    },
    value::Value,
};
use std::ops::Bound;

use super::index_select::predicate_implies_predicate_for_planner;

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
        let primary_key_names = ordered_primary_key_names_from_schema(schema);
        order.deterministic_secondary_order_contract_fields(primary_key_names.as_slice())
    });

    for index in candidate_indexes {
        if !index_stream_is_complete_for_query(schema, index, query_predicate) {
            continue;
        }
        let index_order_terms = index_key_item_order_terms(index.key_items());
        let satisfied = if grouped {
            grouped_order_contract.as_ref().is_some_and(|contract| {
                grouped_index_order_terms_satisfied(contract, &index_order_terms, 0)
            })
        } else {
            scalar_order_contract.as_ref().is_some_and(|contract| {
                deterministic_secondary_index_order_terms_satisfied(contract, &index_order_terms, 0)
            })
        };
        if satisfied {
            return Some(whole_index_ordered_range_scan_from_contract(index.clone()));
        }
    }

    None
}

pub(super) fn index_stream_is_complete_for_query(
    schema: &SchemaInfo,
    index: &SemanticIndexAccessContract,
    query_predicate: &Predicate,
) -> bool {
    (0..index.key_arity()).all(|slot| {
        index.key_item_at(slot).is_some_and(|key_item| {
            let field = key_item.field();
            !schema
                .accepted_query_field_is_omittable(field)
                .unwrap_or(true)
                || predicate_implies_predicate_for_planner(
                    query_predicate,
                    &Predicate::IsNotNull {
                        field: field.to_string(),
                    },
                )
        })
    })
}

fn ordered_primary_key_names_from_schema(schema: &SchemaInfo) -> Vec<&str> {
    schema
        .primary_key_names()
        .iter()
        .map(String::as_str)
        .collect()
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
