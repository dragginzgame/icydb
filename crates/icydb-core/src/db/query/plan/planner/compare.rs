//! Module: query::plan::planner::compare
//! Responsibility: planner compare-predicate access-path planning and index-range lowering.
//! Does not own: runtime comparator enforcement or continuation resume execution details.
//! Boundary: derives compare-driven `AccessPlan` semantics from schema/predicate contracts.

use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::planner::{
            index_literal_matches_schema,
            prefix::{index_multi_lookup_for_in, index_prefix_for_eq},
            sorted_indexes,
        },
        schema::{SchemaInfo, literal_matches_type},
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
    if cmp.coercion.id != CoercionId::Strict {
        return AccessPlan::full_scan();
    }

    if is_primary_key_model(schema, model, &cmp.field)
        && let Some(path) = plan_pk_compare(schema, model, cmp)
    {
        return path;
    }

    match cmp.op {
        CompareOp::Eq => {
            if let Some(paths) =
                index_prefix_for_eq(model, schema, &cmp.field, &cmp.value, query_predicate)
            {
                return AccessPlan::union(paths);
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                // Access canonicalization owns IN-list set normalization
                // (sorting/dedup and singleton collapse).
                // `IN ()` is a constant-empty predicate: no row can satisfy it.
                // Lower directly to an empty access shape instead of full-scan fallback.
                if items.is_empty() {
                    return AccessPlan::by_keys(Vec::new());
                }
                if let Some(paths) =
                    index_multi_lookup_for_in(model, schema, &cmp.field, items, query_predicate)
                {
                    return AccessPlan::union(paths);
                }
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
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
    schema: &SchemaInfo,
    model: &EntityModel,
    cmp: &ComparePredicate,
) -> Option<AccessPlan<Value>> {
    match cmp.op {
        CompareOp::Eq => {
            if !value_matches_pk_model(schema, model, &cmp.value) {
                return None;
            }

            Some(AccessPlan::by_key(cmp.value.clone()))
        }
        CompareOp::In => {
            let Value::List(items) = &cmp.value else {
                return None;
            };

            // Keep planner semantic-only: PK IN literal-set canonicalization is
            // performed by access-plan canonicalization.
            for item in items {
                if !value_matches_pk_model(schema, model, item) {
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

fn is_primary_key_model(schema: &SchemaInfo, model: &EntityModel, field: &str) -> bool {
    field == model.primary_key.name && schema.field(field).is_some()
}

fn value_matches_pk_model(schema: &SchemaInfo, model: &EntityModel, value: &Value) -> bool {
    let field = model.primary_key.name;
    let Some(field_type) = schema.field(field) else {
        return false;
    };

    field_type.is_keyable() && literal_matches_type(value, field_type)
}

fn plan_starts_with_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    query_predicate: &Predicate,
) -> Option<AccessPlan<Value>> {
    if !index_literal_matches_schema(schema, &cmp.field, &cmp.value) {
        return None;
    }

    let Value::Text(prefix) = &cmp.value else {
        return None;
    };
    if prefix.is_empty() {
        return None;
    }

    let lower = Bound::Included(Value::Text(prefix.clone()));
    let upper = strict_text_prefix_upper_bound(prefix);
    for index in sorted_indexes(model, query_predicate) {
        if index.fields().first() != Some(&cmp.field.as_str())
            || !index.is_field_indexable(cmp.field.as_str(), CompareOp::StartsWith)
        {
            continue;
        }

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

fn next_text_prefix(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    for index in (0..chars.len()).rev() {
        let Some(next_char) = next_unicode_scalar(chars[index]) else {
            continue;
        };
        chars.truncate(index);
        chars.push(next_char);
        return Some(chars.into_iter().collect());
    }

    None
}

fn next_unicode_scalar(value: char) -> Option<char> {
    if value == char::MAX {
        return None;
    }

    let mut next = u32::from(value).saturating_add(1);
    if (0xD800..=0xDFFF).contains(&next) {
        next = 0xE000;
    }

    char::from_u32(next)
}
