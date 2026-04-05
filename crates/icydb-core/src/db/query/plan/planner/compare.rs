//! Module: query::plan::planner::compare
//! Responsibility: planner compare-predicate access-path planning and index-range lowering.
//! Does not own: runtime comparator enforcement or continuation resume execution details.
//! Boundary: derives compare-driven `AccessPlan` semantics from schema/predicate contracts.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        index::next_text_prefix,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{
            OrderSpec,
            key_item_match::{
                eq_lookup_value_for_key_item, index_key_item_count, leading_index_key_item,
                starts_with_lookup_value_for_key_item,
            },
            planner::{
                index_literal_matches_schema,
                prefix::{index_multi_lookup_for_in, index_prefix_for_eq},
                sorted_indexes,
            },
        },
        schema::{FieldType, SchemaInfo, literal_matches_type},
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::Bound;

pub(super) fn plan_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
) -> AccessPlan<Value> {
    if cmp.coercion.id == CoercionId::Strict
        && cmp.field == model.primary_key.name
        && let Some(field_type) = schema.field(model.primary_key.name)
        && let Some(path) = plan_pk_compare(field_type, &cmp.value, cmp.op)
    {
        return path;
    }

    match cmp.op {
        CompareOp::Eq => {
            if !matches!(
                cmp.coercion.id,
                CoercionId::Strict | CoercionId::TextCasefold
            ) {
                return AccessPlan::full_scan();
            }
            if let Some(paths) = index_prefix_for_eq(
                model,
                schema,
                &cmp.field,
                &cmp.value,
                cmp.coercion.id,
                query_predicate,
                order,
            ) {
                return paths;
            }
        }
        CompareOp::In => {
            if !matches!(
                cmp.coercion.id,
                CoercionId::Strict | CoercionId::TextCasefold
            ) {
                return AccessPlan::full_scan();
            }
            if let Value::List(items) = &cmp.value {
                // Access canonicalization owns IN-list set normalization
                // (sorting/dedup and singleton collapse).
                // `IN ()` is a constant-empty predicate: no row can satisfy it.
                // Lower directly to an empty access shape instead of full-scan fallback.
                if items.is_empty() {
                    return AccessPlan::by_keys(Vec::new());
                }
                if let Some(paths) = index_multi_lookup_for_in(
                    model,
                    schema,
                    &cmp.field,
                    items,
                    cmp.coercion.id,
                    query_predicate,
                ) {
                    return AccessPlan::union(paths);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            if !matches!(
                cmp.coercion.id,
                CoercionId::Strict | CoercionId::TextCasefold
            ) {
                return AccessPlan::full_scan();
            }
            let Some(field_type) = schema.field(&cmp.field) else {
                return AccessPlan::full_scan();
            };
            if cmp.coercion.id == CoercionId::Strict && !field_type.is_orderable() {
                return AccessPlan::full_scan();
            }
            if cmp.coercion.id == CoercionId::TextCasefold && !field_type.is_text() {
                return AccessPlan::full_scan();
            }
            if let Some(path) = plan_ordered_compare(model, schema, cmp, query_predicate) {
                return path;
            }
        }
        CompareOp::StartsWith => {
            if !matches!(
                cmp.coercion.id,
                CoercionId::Strict | CoercionId::TextCasefold
            ) {
                return AccessPlan::full_scan();
            }

            // Keep the starts-with split explicit:
            // - raw field-key text prefixes now lower onto the same bounded
            //   semantic range contract as equivalent `>=`/`< next_prefix`
            //   forms
            // - expression-key lookups still keep their lower-bounded shape
            //   because the derived expression ordering does not yet expose one
            //   tighter planner-owned upper-bound contract
            if let Some(path) = plan_starts_with_compare(model, schema, cmp, query_predicate) {
                return path;
            }
        }
        _ => {
            // NOTE: Other non-equality comparisons do not currently map to key access paths.
        }
    }

    AccessPlan::full_scan()
}

fn plan_pk_compare(
    field_type: &FieldType,
    value: &Value,
    op: CompareOp,
) -> Option<AccessPlan<Value>> {
    if !field_type.is_keyable() {
        return None;
    }

    match op {
        CompareOp::Eq => {
            if !literal_matches_type(value, field_type) {
                return None;
            }

            Some(AccessPlan::by_key(value.clone()))
        }
        CompareOp::In => {
            let Value::List(items) = value else {
                return None;
            };

            // Keep planner semantic-only: PK IN literal-set canonicalization is
            // performed by access-plan canonicalization.
            for item in items {
                if !literal_matches_type(item, field_type) {
                    return None;
                }
            }

            Some(AccessPlan::by_keys(items.clone()))
        }
        _ => {
            // NOTE: Only Eq/In comparisons can be expressed as key access paths.
            None
        }
    }
}

fn plan_starts_with_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    query_predicate: &Predicate,
) -> Option<AccessPlan<Value>> {
    // This helper owns the shared starts-with range lowering contract for both
    // raw field keys and the expression-key casefold path.
    let field_type = schema.field(&cmp.field)?;
    if !field_type.is_text() {
        return None;
    }
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);
    for index in sorted_indexes(model, query_predicate) {
        let Some(leading_key_item) = leading_index_key_item(index) else {
            continue;
        };
        let Some(prefix) = starts_with_lookup_value_for_key_item(
            leading_key_item,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        ) else {
            continue;
        };

        let lower = Bound::Included(Value::Text(prefix.clone()));
        // Expression-key components are length-prefixed in raw key framing.
        // A semantic `next_prefix` upper bound can exclude longer matching values,
        // so expression starts-with lowers to a safe lower-bounded envelope and
        // relies on residual predicate filtering for exact prefix semantics.
        let upper = if matches!(
            leading_key_item,
            crate::model::index::IndexKeyItem::Expression(_)
        ) {
            Bound::Unbounded
        } else {
            strict_text_prefix_upper_bound(&prefix)
        };

        let semantic_range =
            SemanticIndexRangeSpec::new(*index, vec![0usize], Vec::new(), lower, upper);
        return Some(AccessPlan::index_range(semantic_range));
    }

    None
}

fn plan_ordered_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    query_predicate: &Predicate,
) -> Option<AccessPlan<Value>> {
    // Ordered bounds must reuse the same canonical literal-lowering authority
    // as Eq/In/prefix matching so expression-key comparisons stay aligned with
    // the stored normalized index value order.
    let literal_compatible = index_literal_matches_schema(schema, &cmp.field, &cmp.value);

    for index in sorted_indexes(model, query_predicate) {
        let Some(leading_key_item) = leading_index_key_item(index) else {
            continue;
        };
        if index_key_item_count(index) != 1 {
            continue;
        }

        let Some(bound_value) = eq_lookup_value_for_key_item(
            leading_key_item,
            cmp.field.as_str(),
            &cmp.value,
            cmp.coercion.id,
            literal_compatible,
        ) else {
            continue;
        };

        match leading_key_item {
            crate::model::index::IndexKeyItem::Field(_) => {
                if cmp.coercion.id != CoercionId::Strict
                    || index.fields().first() != Some(&cmp.field.as_str())
                    || !index.is_field_indexable(cmp.field.as_str(), cmp.op)
                {
                    continue;
                }
            }
            crate::model::index::IndexKeyItem::Expression(_) => {
                if cmp.coercion.id != CoercionId::TextCasefold {
                    continue;
                }
            }
        }

        let (lower, upper) = match cmp.op {
            CompareOp::Gt => (Bound::Excluded(bound_value), Bound::Unbounded),
            CompareOp::Gte => (Bound::Included(bound_value), Bound::Unbounded),
            CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(bound_value)),
            CompareOp::Lte => (Bound::Unbounded, Bound::Included(bound_value)),
            _ => unreachable!("ordered compare helper must receive one of Gt/Gte/Lt/Lte"),
        };
        let semantic_range =
            SemanticIndexRangeSpec::new(*index, vec![0usize], Vec::new(), lower, upper);

        return Some(AccessPlan::index_range(semantic_range));
    }

    None
}

fn strict_text_prefix_upper_bound(prefix: &str) -> Bound<Value> {
    next_text_prefix(prefix).map_or(Bound::Unbounded, |next_prefix| {
        Bound::Excluded(Value::Text(next_prefix))
    })
}
