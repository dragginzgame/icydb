//! Module: db::query::plan::planner::order_select
//! Responsibility: planner-owned order-driven access fallback selection.
//! Does not own: predicate analysis, logical-order canonicalization, or runtime traversal.
//! Boundary: derives secondary index range candidates when predicate planning alone would full-scan.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{OrderSpec, index_order_terms, planner::sorted_indexes},
        schema::SchemaInfo,
    },
    model::{
        entity::EntityModel,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    value::Value,
};
use std::ops::Bound;

/// Select one whole-index range scan when canonical ORDER BY already matches a
/// deterministic secondary index traversal contract.
#[must_use]
pub(in crate::db::query::plan::planner) fn index_range_from_order(
    model: &EntityModel,
    schema: &SchemaInfo,
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
        if predicate_blocks_order_index_range_fallback(schema, query_predicate, index) {
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

// Keep order-only fallback conservative when the query predicate contains one
// strict text range-like compare against a raw field key of the candidate
// index. Those predicates intentionally fail closed on the compare/range lanes,
// so the order-only fallback must not silently reintroduce the same raw-field
// index-range shape under a different route label.
fn predicate_blocks_order_index_range_fallback(
    schema: &SchemaInfo,
    predicate: &Predicate,
    index: &IndexModel,
) -> bool {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .any(|child| predicate_blocks_order_index_range_fallback(schema, child, index)),
        Predicate::Not(inner) => predicate_blocks_order_index_range_fallback(schema, inner, index),
        Predicate::Compare(cmp) => compare_blocks_order_index_range_fallback(schema, cmp, index),
        Predicate::True
        | Predicate::False
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => false,
    }
}

fn compare_blocks_order_index_range_fallback(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    index: &IndexModel,
) -> bool {
    if cmp.coercion.id != CoercionId::Strict
        || !matches!(
            cmp.op,
            CompareOp::StartsWith | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        )
    {
        return false;
    }

    let Some(field_type) = schema.field(cmp.field.as_str()) else {
        return false;
    };
    if !field_type.is_text() {
        return false;
    }

    index_contains_raw_field_key(index, cmp.field.as_str())
}

fn index_contains_raw_field_key(index: &IndexModel, field: &str) -> bool {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => fields.contains(&field),
        IndexKeyItemsRef::Items(items) => items
            .iter()
            .any(|item| matches!(item, IndexKeyItem::Field(index_field) if *index_field == field)),
    }
}
