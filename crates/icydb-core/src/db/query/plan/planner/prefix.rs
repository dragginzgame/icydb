use crate::{
    db::{
        access::AccessPlan,
        predicate::{CoercionId, CompareOp, Predicate, SchemaInfo},
        query::plan::planner::{
            index_literal_matches_schema, index_select::better_index, sorted_indexes,
        },
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};

pub(super) fn index_prefix_for_eq(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> Option<Vec<AccessPlan<Value>>> {
    if !index_literal_matches_schema(schema, field, value) {
        return None;
    }

    let mut out = Vec::new();
    for index in sorted_indexes(model) {
        if index.fields.first() != Some(&field) || !index.is_field_indexable(field, CompareOp::Eq) {
            continue;
        }
        out.push(AccessPlan::index_prefix(*index, vec![value.clone()]));
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(super) fn index_prefix_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPlan<Value>> {
    // Cache literal/schema compatibility once per equality literal so index
    // candidate selection does not repeat schema checks on every index iteration.
    let mut field_values = Vec::new();

    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.op != CompareOp::Eq {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            continue;
        }
        field_values.push(CachedEqLiteral {
            field: cmp.field.as_str(),
            value: &cmp.value,
            compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(usize, bool, &IndexModel, Vec<Value>)> = None;
    for index in sorted_indexes(model) {
        let mut prefix = Vec::new();
        for field in index.fields {
            // NOTE: duplicate equality predicates on the same field are assumed
            // to have been validated upstream (no conflict). Planner picks the first.
            let Some(cached) = field_values.iter().find(|cached| cached.field == *field) else {
                break;
            };
            if !index.is_field_indexable(field, CompareOp::Eq) || !cached.compatible {
                prefix.clear();
                break;
            }
            prefix.push(cached.value.clone());
        }

        if prefix.is_empty() {
            continue;
        }

        let exact = prefix.len() == index.fields.len();
        match &best {
            None => best = Some((prefix.len(), exact, index, prefix)),
            Some((best_len, best_exact, best_index, _)) => {
                if better_index(
                    (prefix.len(), exact, index),
                    (*best_len, *best_exact, best_index),
                ) {
                    best = Some((prefix.len(), exact, index, prefix));
                }
            }
        }
    }

    best.map(|(_, _, index, values)| AccessPlan::index_prefix(*index, values))
}

///
/// CachedEqLiteral
///
/// Equality literal plus its precomputed planner-side schema compatibility.
///

struct CachedEqLiteral<'a> {
    field: &'a str,
    value: &'a Value,
    compatible: bool,
}
