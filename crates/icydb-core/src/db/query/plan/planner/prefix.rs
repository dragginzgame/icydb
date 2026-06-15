//! Module: query::plan::planner::prefix
//! Responsibility: planner prefix/multi-lookup access-path derivation from predicate equality sets.
//! Does not own: runtime index traversal execution or continuation resume behavior.
//! Boundary: maps prefix-capable predicates into planner-owned access plan candidates.

use crate::{
    db::{
        access::{
            AccessPlan, MAX_INDEX_BRANCH_SET_VALUES, SemanticIndexAccessContract,
            SemanticIndexKeyItemRef, SemanticIndexKeyItemsRef,
        },
        predicate::{CoercionId, CompareOp, Predicate},
        query::plan::{
            OrderDirection, OrderSpec,
            key_item_match::eq_lookup_value_for_key_item,
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_literal_matches_schema,
                selected_index_contract_satisfies_secondary_order,
            },
        },
        schema::SchemaInfo,
    },
    model::{entity::EntityModel, index::IndexKeyItemsRef},
    value::{Value, canonicalize_value_set},
};

fn leading_index_prefix_lookup_value(
    index_contract: &SemanticIndexAccessContract,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Option<Value> {
    let key_item = index_contract.key_item_at(0)?;
    eq_lookup_value_for_key_item(key_item, field, value, coercion, literal_compatible)
}

// This helper now carries one explicit planner-visible index slice in addition
// to the existing schema/field/order inputs so callers can keep lifecycle
// gating at the planner boundary instead of reopening store state here.
#[expect(
    clippy::too_many_arguments,
    reason = "planner prefix access keeps field/value/order inputs explicit at this boundary"
)]
pub(super) fn index_prefix_for_eq(
    _model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    let literal_compatible = index_literal_matches_schema(schema, field, value);

    let mut best: Option<(AccessCandidateScore, SemanticIndexAccessContract, Value)> = None;
    for index in candidate_indexes {
        let Some(lookup_value) =
            leading_index_prefix_lookup_value(index, field, value, coercion, literal_compatible)
        else {
            continue;
        };

        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index.clone(),
            1,
            index.key_arity() == 1,
            0,
            grouped,
        );
        match best {
            None => best = Some((score, index.clone(), lookup_value)),
            Some((best_score, best_index, _))
                if access_candidate_score_outranks(score, best_score, true)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), lookup_value));
            }
            _ => {}
        }
    }

    best.map(|(_, index, lookup_value)| {
        AccessPlan::index_prefix_from_contract(index, vec![lookup_value])
    })
}

pub(super) fn index_multi_lookup_for_in(
    _model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    field: &str,
    values: &[Value],
    coercion: CoercionId,
) -> Option<Vec<AccessPlan<Value>>> {
    // Cache schema/literal compatibility once per `IN` item so candidate-index
    // selection does not repeat the same field-type check for every index.
    let cached_values = values
        .iter()
        .map(|value| (value, index_literal_matches_schema(schema, field, value)))
        .collect::<Vec<_>>();

    let mut out = Vec::new();
    for index in candidate_indexes {
        let mut lookup_values = Vec::with_capacity(values.len());
        for (value, literal_compatible) in &cached_values {
            let Some(lookup_value) = leading_index_prefix_lookup_value(
                index,
                field,
                value,
                coercion,
                *literal_compatible,
            ) else {
                lookup_values.clear();
                break;
            };

            lookup_values.push(lookup_value);
        }
        if lookup_values.is_empty() {
            continue;
        }

        out.push(AccessPlan::index_multi_lookup_from_contract(
            index.clone(),
            lookup_values,
        ));
    }

    if out.is_empty() { None } else { Some(out) }
}

pub(super) fn index_prefix_from_and(
    _model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
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

    let mut best: Option<(
        AccessCandidateScore,
        SemanticIndexAccessContract,
        Vec<Value>,
    )> = None;
    for index in candidate_indexes {
        let Some(prefix) = build_index_eq_prefix(index, &field_values) else {
            continue;
        };
        if prefix.is_empty() {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index.clone(),
            prefix.len(),
            prefix.len() == index.key_arity(),
            0,
            grouped,
        );
        match &best {
            None => best = Some((score, index.clone(), prefix)),
            Some((best_score, best_index, _))
                if access_candidate_score_outranks(score, *best_score, true)
                    || (score == *best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), prefix));
            }
            Some(_) => {}
        }
    }

    best.map(|(_, index, values)| AccessPlan::index_prefix_from_contract(index, values))
}

pub(super) fn index_branch_set_from_and(
    _model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    let order = order?;
    if grouped || !primary_key_asc_order(schema, order) {
        return None;
    }

    let mut eq_values = Vec::new();
    let mut in_values = Vec::new();
    collect_branch_set_literals(schema, children, &mut eq_values, &mut in_values);
    if eq_values.is_empty() || in_values.is_empty() {
        return None;
    }

    let mut best: Option<(
        AccessCandidateScore,
        SemanticIndexAccessContract,
        Vec<Value>,
        Vec<Value>,
    )> = None;
    for index in candidate_indexes {
        let Some(fixed_values) = build_index_eq_prefix(index, &eq_values) else {
            continue;
        };
        if fixed_values.is_empty() {
            continue;
        }

        let branch_slot = fixed_values.len();
        let Some(branch_key_item) = index.key_item_at(branch_slot) else {
            continue;
        };
        let Some(branch_values) = build_index_branch_values(branch_key_item, &in_values) else {
            continue;
        };
        if branch_values.len() < 2 || branch_values.len() > MAX_INDEX_BRANCH_SET_VALUES {
            continue;
        }

        let branch_prefix_len = branch_slot.saturating_add(1);
        if !selected_index_contract_satisfies_secondary_order(
            schema,
            Some(order),
            index.clone(),
            branch_prefix_len,
            false,
        ) {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            schema,
            Some(order),
            index.clone(),
            branch_prefix_len,
            false,
            0,
            false,
        );
        match &best {
            None => best = Some((score, index.clone(), fixed_values, branch_values)),
            Some((best_score, best_index, _, _))
                if access_candidate_score_outranks(score, *best_score, true)
                    || (score == *best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), fixed_values, branch_values));
            }
            Some(_) => {}
        }
    }

    best.map(|(_, index, fixed_values, branch_values)| {
        AccessPlan::index_branch_set_from_contract(index, fixed_values, branch_values)
    })
}

fn primary_key_asc_order(schema: &SchemaInfo, order: &OrderSpec) -> bool {
    let primary_key_names: Vec<&str> = schema
        .primary_key_names()
        .iter()
        .map(String::as_str)
        .collect();

    order.primary_key_only_direction_fields(primary_key_names.as_slice())
        == Some(OrderDirection::Asc)
}

fn collect_branch_set_literals<'a>(
    schema: &SchemaInfo,
    children: &'a [Predicate],
    eq_values: &mut Vec<CachedEqLiteral<'a>>,
    in_values: &mut Vec<CachedInLiteral<'a>>,
) {
    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            continue;
        }
        match cmp.op {
            CompareOp::Eq => {
                eq_values.push(CachedEqLiteral {
                    field: cmp.field.as_str(),
                    value: &cmp.value,
                    coercion: cmp.coercion.id,
                    compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
                });
            }
            CompareOp::In => {
                let Value::List(values) = &cmp.value else {
                    continue;
                };
                in_values.push(CachedInLiteral {
                    field: cmp.field.as_str(),
                    values: values
                        .iter()
                        .map(|value| CachedInValue {
                            value,
                            compatible: index_literal_matches_schema(schema, &cmp.field, value),
                        })
                        .collect(),
                    coercion: cmp.coercion.id,
                });
            }
            _ => {}
        }
    }
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

struct CachedInLiteral<'a> {
    field: &'a str,
    values: Vec<CachedInValue<'a>>,
    coercion: CoercionId,
}

struct CachedInValue<'a> {
    value: &'a Value,
    compatible: bool,
}

fn build_index_eq_prefix(
    index_contract: &SemanticIndexAccessContract,
    field_values: &[CachedEqLiteral<'_>],
) -> Option<Vec<Value>> {
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => build_index_eq_prefix_for_items(
            fields
                .iter()
                .map(|field| SemanticIndexKeyItemRef::Field(field.as_str())),
            field_values,
        ),
        SemanticIndexKeyItemsRef::Accepted(items) => {
            build_index_eq_prefix_for_items(items.iter().map(|item| item.as_ref()), field_values)
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            build_index_eq_prefix_for_items(
                fields.iter().copied().map(SemanticIndexKeyItemRef::Field),
                field_values,
            )
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
            build_index_eq_prefix_for_items(items.iter().copied().map(Into::into), field_values)
        }
    }
}

// Field-only indexes and mixed field/expression indexes both use the same
// equality-prefix assembly contract; only the key-item iterator differs.
fn build_index_eq_prefix_for_items<'a, I>(
    key_items: I,
    field_values: &[CachedEqLiteral<'_>],
) -> Option<Vec<Value>>
where
    I: IntoIterator<Item = SemanticIndexKeyItemRef<'a>>,
{
    let mut prefix = Vec::new();
    for key_item in key_items {
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

    Some(prefix)
}

fn build_index_branch_values(
    key_item: SemanticIndexKeyItemRef<'_>,
    in_values: &[CachedInLiteral<'_>],
) -> Option<Vec<Value>> {
    let mut matched: Option<Vec<Value>> = None;
    for cached in in_values {
        if key_item.field() != cached.field {
            continue;
        }

        let mut branch_values = Vec::with_capacity(cached.values.len());
        for cached_value in &cached.values {
            if !cached_value.compatible {
                return None;
            }
            let lookup_value = eq_lookup_value_for_key_item(
                key_item,
                cached.field,
                cached_value.value,
                cached.coercion,
                true,
            )?;
            branch_values.push(lookup_value);
        }
        canonicalize_value_set(&mut branch_values);
        if branch_values.is_empty() {
            return None;
        }

        if let Some(existing) = &matched
            && existing != &branch_values
        {
            return None;
        }
        matched = Some(branch_values);
    }

    matched
}
