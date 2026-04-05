//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        predicate::Predicate,
        query::plan::{OrderSpec, index_order_terms, planner::sorted_indexes},
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::Bound;

/// Select one whole-index range scan when canonical ORDER BY already matches a
/// deterministic secondary index traversal contract.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    query_predicate: Option<&Predicate>,
) -> Option<AccessPlan<Value>> {
    let order = order?;

    // Order-driven access fallback is only valid when the canonical ORDER BY
    // already carries one uniform-direction `..., primary_key` tie-break shape.
    order.deterministic_secondary_order_direction(model.primary_key.name)?;

    // Filtered indexes remain eligible only when the full query predicate
    // implies their guard. When no predicate exists, evaluate against `True`
    // so filtered indexes fail closed instead of being scanned unconditionally.
    let true_predicate = Predicate::True;
    let query_predicate = query_predicate.unwrap_or(&true_predicate);

    for index in sorted_indexes(model, query_predicate) {
        let index_terms = index_order_terms(index);
        if !order.matches_expected_term_sequence_plus_primary_key(
            index_terms.iter().map(String::as_str),
            model.primary_key.name,
        ) {
            continue;
        }

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

        return Some(AccessPlan::index_range(spec));
    }

    None
}
