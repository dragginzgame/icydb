use crate::{
    db::{
        access::{AccessPlan, SemanticIndexRangeSpec},
        predicate::{CoercionId, CompareOp, ComparePredicate, SchemaInfo, literal_matches_type},
        query::plan::planner::{
            index_literal_matches_schema, prefix::index_prefix_for_eq, sorted_indexes,
        },
    },
    model::entity::EntityModel,
    value::Value,
};
use std::ops::Bound;

pub(super) fn plan_compare(
    model: &EntityModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
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
            if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, &cmp.value) {
                return AccessPlan::union(paths);
            }
        }
        CompareOp::In => {
            if let Value::List(items) = &cmp.value {
                let mut plans = Vec::new();
                for item in items {
                    if let Some(paths) = index_prefix_for_eq(model, schema, &cmp.field, item) {
                        plans.extend(paths);
                    }
                }
                if !plans.is_empty() {
                    return AccessPlan::union(plans);
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

                for index in sorted_indexes(model) {
                    if index.fields.len() == 1
                        && index.fields[0] == cmp.field.as_str()
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

            for item in items {
                if !value_matches_pk_model(schema, model, item) {
                    return None;
                }
            }
            // NOTE: key order is canonicalized during access-plan normalization.
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
