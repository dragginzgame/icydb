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
            key_item_match::{leading_index_key_item, starts_with_lookup_value_for_key_item},
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
            ) {
                return AccessPlan::union(paths);
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
            if cmp.coercion.id != CoercionId::Strict {
                return AccessPlan::full_scan();
            }
            // Single compare predicates only map directly to one-field indexes.
            // Composite prefix+range extraction remains AND-group driven.
            if index_literal_matches_schema(schema, &cmp.field, &cmp.value) {
                let (lower, upper) = match cmp.op {
                    CompareOp::Gt => (Bound::Excluded(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Gte => (Bound::Included(cmp.value.clone()), Bound::Unbounded),
                    CompareOp::Lt => (Bound::Unbounded, Bound::Excluded(cmp.value.clone())),
                    CompareOp::Lte => (Bound::Unbounded, Bound::Included(cmp.value.clone())),
                    _ => unreachable!("range arm must be one of Gt/Gte/Lt/Lte"),
                };

                for index in sorted_indexes(model, query_predicate) {
                    if index.fields().len() == 1
                        && index.fields()[0] == cmp.field.as_str()
                        && index.is_field_indexable(&cmp.field, cmp.op)
                    {
                        let semantic_range = SemanticIndexRangeSpec::new(
                            *index,
                            vec![0usize],
                            Vec::new(),
                            lower,
                            upper,
                        );

                        return AccessPlan::index_range(semantic_range);
                    }
                }
            }
        }
        CompareOp::StartsWith => {
            if !matches!(
                cmp.coercion.id,
                CoercionId::Strict | CoercionId::TextCasefold
            ) {
                return AccessPlan::full_scan();
            }
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

fn strict_text_prefix_upper_bound(prefix: &str) -> Bound<Value> {
    next_text_prefix(prefix).map_or(Bound::Unbounded, |next_prefix| {
        Bound::Excluded(Value::Text(next_prefix))
    })
}
