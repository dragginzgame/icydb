//! Module: query::plan::planner::prefix
//! Responsibility: planner prefix/multi-lookup access-path derivation from predicate equality sets.
//! Does not own: runtime index traversal execution or continuation resume behavior.
//! Boundary: maps prefix-capable predicates into planner-owned access plan candidates.

use crate::{
    db::{
        access::AccessPlan,
        predicate::{CoercionId, CompareOp, Predicate},
        query::plan::{
            OrderSpec, index_order_terms,
            key_item_match::{
                eq_lookup_value_for_key_item, index_key_item_count, leading_index_key_item,
            },
            planner::{index_literal_matches_schema, sorted_indexes},
        },
        schema::SchemaInfo,
    },
    model::{
        entity::EntityModel,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    value::Value,
};

fn leading_index_prefix_lookup_value(
    index: &IndexModel,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    let key_item = leading_index_key_item(index)?;
    eq_lookup_value_for_key_item(key_item, field, value, coercion, literal_compatible)
}

pub(super) fn index_prefix_for_eq(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
) -> Option<AccessPlan<Value>> {
    let literal_compatible = index_literal_matches_schema(schema, field, value);

    let mut best: Option<(bool, &'static IndexModel, Value)> = None;
    for index in sorted_indexes(model, query_predicate) {
        let Some(lookup_value) =
            leading_index_prefix_lookup_value(index, field, value, coercion, literal_compatible)
        else {
            continue;
        };

        let order_match = prefix_candidate_satisfies_secondary_order(model, order, index, 1);
        match best {
            None => best = Some((order_match, index, lookup_value)),
            Some((best_order_match, best_index, _))
                if order_match && !best_order_match
                    || (order_match == best_order_match && index.name() < best_index.name()) =>
            {
                best = Some((order_match, index, lookup_value));
            }
            _ => {}
        }
    }

    best.map(|(_, index, lookup_value)| AccessPlan::index_prefix(*index, vec![lookup_value]))
}

pub(super) fn index_multi_lookup_for_in(
    model: &EntityModel,
    schema: &SchemaInfo,
    field: &str,
    values: &[Value],
    coercion: CoercionId,
    query_predicate: &Predicate,
) -> Option<Vec<AccessPlan<Value>>> {
    let mut out = Vec::new();
    for index in sorted_indexes(model, query_predicate) {
        let mut lookup_values = Vec::with_capacity(values.len());
        for value in values {
            let literal_compatible = index_literal_matches_schema(schema, field, value);
            let Some(lookup_value) = leading_index_prefix_lookup_value(
                index,
                field,
                value,
                coercion,
                literal_compatible,
            ) else {
                lookup_values.clear();
                break;
            };

            lookup_values.push(lookup_value);
        }
        if lookup_values.is_empty() {
            continue;
        }

        out.push(AccessPlan::index_multi_lookup(*index, lookup_values));
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(super) fn index_prefix_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
    query_predicate: &Predicate,
    order: Option<&OrderSpec>,
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
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            continue;
        }
        field_values.push(CachedEqLiteral {
            field: cmp.field.as_str(),
            value: &cmp.value,
            coercion: cmp.coercion.id,
            compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(usize, bool, bool, &IndexModel, Vec<Value>)> = None;
    for index in sorted_indexes(model, query_predicate) {
        let Some(prefix) = build_index_eq_prefix(index, &field_values) else {
            continue;
        };
        if prefix.is_empty() {
            continue;
        }

        let exact = prefix.len() == index_key_item_count(index);
        let order_match =
            prefix_candidate_satisfies_secondary_order(model, order, index, prefix.len());
        match &best {
            None => best = Some((prefix.len(), exact, order_match, index, prefix)),
            Some((best_len, best_exact, best_order_match, best_index, _)) => {
                if prefix.len() > *best_len
                    || (prefix.len() == *best_len && exact && !*best_exact)
                    || (prefix.len() == *best_len
                        && exact == *best_exact
                        && order_match
                        && !*best_order_match)
                    || (prefix.len() == *best_len
                        && exact == *best_exact
                        && order_match == *best_order_match
                        && index.name() < best_index.name())
                {
                    best = Some((prefix.len(), exact, order_match, index, prefix));
                }
            }
        }
    }

    best.map(|(_, _, _, index, values)| AccessPlan::index_prefix(*index, values))
}

// Prefix ranking preserves the old selectivity-first policy and only uses
// ORDER BY satisfaction as a deterministic tie-break for equally selective
// candidates.
fn prefix_candidate_satisfies_secondary_order(
    model: &EntityModel,
    order: Option<&OrderSpec>,
    index: &IndexModel,
    prefix_len: usize,
) -> bool {
    let Some(order) = order else {
        return false;
    };
    if order
        .deterministic_secondary_order_direction(model.primary_key.name)
        .is_none()
    {
        return false;
    }

    let index_terms = index_order_terms(index);

    order.matches_expected_term_sequence_plus_primary_key(
        index_terms.iter().skip(prefix_len).map(String::as_str),
        model.primary_key.name,
    ) || order.matches_expected_term_sequence_plus_primary_key(
        index_terms.iter().map(String::as_str),
        model.primary_key.name,
    )
}

///
/// CachedEqLiteral
///
/// Equality literal plus its precomputed planner-side schema compatibility.
///

struct CachedEqLiteral<'a> {
    field: &'a str,
    value: &'a Value,
    coercion: CoercionId,
    compatible: bool,
}

fn build_index_eq_prefix(
    index: &IndexModel,
    field_values: &[CachedEqLiteral<'_>],
) -> Option<Vec<Value>> {
    let mut prefix = Vec::new();
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            for &field in fields {
                let key_item = IndexKeyItem::Field(field);
                let mut matched: Option<Value> = None;
                for cached in field_values {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cached.field,
                        cached.value,
                        cached.coercion,
                        cached.compatible,
                    ) else {
                        continue;
                    };

                    if let Some(existing) = &matched
                        && existing != &candidate
                    {
                        return None;
                    }
                    matched = Some(candidate);
                }

                let Some(value) = matched else {
                    break;
                };
                prefix.push(value);
            }
        }
        IndexKeyItemsRef::Items(items) => {
            for &key_item in items {
                let mut matched: Option<Value> = None;
                for cached in field_values {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cached.field,
                        cached.value,
                        cached.coercion,
                        cached.compatible,
                    ) else {
                        continue;
                    };

                    if let Some(existing) = &matched
                        && existing != &candidate
                    {
                        return None;
                    }
                    matched = Some(candidate);
                }

                let Some(value) = matched else {
                    break;
                };
                prefix.push(value);
            }
        }
    }

    Some(prefix)
}
