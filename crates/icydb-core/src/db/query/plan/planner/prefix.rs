//! Module: query::plan::planner::prefix
//! Responsibility: planner prefix/multi-lookup access-path derivation from predicate equality sets.
//! Does not own: runtime index traversal execution or continuation resume behavior.
//! Boundary: maps prefix-capable predicates into planner-owned access plan candidates.

use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, MAX_INDEX_BRANCH_SET_VALUES, SemanticIndexAccessContract,
            SemanticIndexKeyItemRef,
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::construction::ConstructionBudget,
        query::plan::{
            OrderDirection, OrderSpec,
            key_item_match::{
                copy_lookup_value_for_key_item, eq_lookup_value_for_key_item,
                key_item_matches_field_and_coercion, key_item_supports_lookup_value,
            },
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_field_literal_matcher,
                index_literal_matches_schema, selected_index_contract_satisfies_secondary_order,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::{Value, canonicalize_value_set},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

fn leading_index_prefix_lookup_value(
    index_contract: &SemanticIndexAccessContract,
    field: &str,
    value: &Value,
    coercion: CoercionId,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Value>, InternalError> {
    let Some(key_item) = index_contract.key_item_at(0) else {
        return Ok(None);
    };
    copy_lookup_value_for_key_item(key_item, field, value, coercion, true, budget)
}

// This helper now carries one explicit planner-visible index slice in addition
// to the existing schema/field/order inputs so callers can keep lifecycle
// gating at the planner boundary instead of reopening store state here.
pub(super) fn index_prefix_for_eq(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    let (field, value, coercion) = (cmp.field(), cmp.value(), cmp.coercion.id);
    let literal_compatible = index_literal_matches_schema(schema, field, value);
    if !literal_compatible {
        return Ok(None);
    }
    let Some(index) =
        best_leading_lookup_index(candidate_indexes, schema, order, grouped, budget, |key| {
            key_item_supports_lookup_value(key, field, value, coercion, true)
        })?
    else {
        return Ok(None);
    };
    let mut values = budget.vec_with_capacity(1)?;
    let Some(value) = leading_index_prefix_lookup_value(index, field, value, coercion, budget)?
    else {
        return Ok(None);
    };
    values.push(value);
    budget.charge(
        Resource::TemporaryBytes,
        size_of::<AccessPath<Value>>() as u64,
    )?;
    Ok(Some(AccessPlan::index_prefix_from_contract(
        index.clone(),
        values,
    )))
}

pub(super) fn index_multi_lookup_for_in(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
    values: &[Value],
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    let (field, coercion) = (cmp.field(), cmp.coercion.id);
    if values.is_empty() {
        return Ok(None);
    }

    let matcher = index_field_literal_matcher(schema, field);
    if !values.iter().all(|value| matcher.matches(value)) {
        return Ok(None);
    }
    let Some(index) =
        best_leading_lookup_index(candidate_indexes, schema, order, grouped, budget, |key| {
            // Strict fields need no second literal walk after shared schema admission.
            // Expression eligibility still uses the canonical per-value shape gate.
            key_item_matches_field_and_coercion(key, field, coercion)
                && (!key.is_expression()
                    || values.iter().all(|value| {
                        key_item_supports_lookup_value(key, field, value, coercion, true)
                    }))
        })?
    else {
        return Ok(None);
    };
    let mut lookup_values = budget.vec_with_capacity(values.len())?;
    for value in values {
        let Some(value) = leading_index_prefix_lookup_value(index, field, value, coercion, budget)?
        else {
            return Ok(None);
        };
        lookup_values.push(value);
    }
    budget.charge(
        Resource::TemporaryBytes,
        size_of::<AccessPath<Value>>() as u64,
    )?;
    Ok(Some(AccessPlan::index_multi_lookup_from_contract(
        index.clone(),
        lookup_values,
    )))
}

// Rank borrowed identities before building any operand payload. Both lookup
// families use the same structural score and accepted-name tie-break.
fn best_leading_lookup_index<'a>(
    candidate_indexes: &'a [SemanticIndexAccessContract],
    schema: &SchemaInfo,
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
    supports: impl Fn(SemanticIndexKeyItemRef<'_>) -> bool,
) -> Result<Option<&'a SemanticIndexAccessContract>, InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let mut best: Option<(AccessCandidateScore, &SemanticIndexAccessContract)> = None;
    for index in candidate_indexes {
        let Some(key) = index.key_item_at(0) else {
            continue;
        };
        if !supports(key) {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index,
            1,
            index.key_arity() == 1,
            0,
            grouped,
        );
        match &best {
            None => best = Some((score, index)),
            Some((best_score, best_index))
                if access_candidate_score_outranks(score, *best_score, true)
                    || (score == *best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index));
            }
            Some(_) => {}
        }
    }

    Ok(best.map(|(_, index)| index))
}

pub(super) fn index_prefix_from_and(
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
        &SemanticIndexAccessContract,
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
            index,
            prefix.len(),
            prefix.len() == index.key_arity(),
            0,
            grouped,
        );
        match &best {
            None => best = Some((score, index, prefix)),
            Some((best_score, best_index, _))
                if access_candidate_score_outranks(score, *best_score, true)
                    || (score == *best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index, prefix));
            }
            Some(_) => {}
        }
    }

    best.map(|(_, index, values)| AccessPlan::index_prefix_from_contract(index.clone(), values))
}

pub(super) fn index_branch_set_from_and(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<AccessPlan<Value>> {
    index_branch_set_from_and_with_cap(
        candidate_indexes,
        schema,
        children,
        order,
        grouped,
        MAX_INDEX_BRANCH_SET_VALUES,
        true,
    )
}

pub(in crate::db::query) fn count_cardinality_index_branch_set_from_and(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    max_branch_values: usize,
) -> Option<AccessPlan<Value>> {
    // This semantic plan is proof input for exact metadata only. Its cap is
    // deliberately independent of executable branch-plan admission, and the
    // caller must not route it into row execution.
    index_branch_set_from_and_with_cap(
        candidate_indexes,
        schema,
        children,
        None,
        false,
        max_branch_values,
        false,
    )
}

fn index_branch_set_from_and_with_cap(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
    max_branch_values: usize,
    record_shared_branch_cap: bool,
) -> Option<AccessPlan<Value>> {
    let _ = record_shared_branch_cap;
    if grouped || order.is_some_and(|order| !primary_key_asc_order(schema, order)) {
        return None;
    }

    let mut eq_values = Vec::new();
    let mut in_values = Vec::new();
    let mut excluded_values = Vec::new();
    collect_branch_set_literals(
        schema,
        children,
        &mut eq_values,
        &mut in_values,
        &mut excluded_values,
    );
    if eq_values.is_empty() || in_values.is_empty() {
        return None;
    }

    let mut best: Option<(
        AccessCandidateScore,
        &SemanticIndexAccessContract,
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
        let mut branch_values = branch_values;
        prune_branch_values_by_exclusions(branch_key_item, &mut branch_values, &excluded_values);
        if branch_values.is_empty() || branch_values.len() > max_branch_values {
            continue;
        }

        let branch_prefix_len = branch_slot.saturating_add(1);
        if order.is_some()
            && !selected_index_contract_satisfies_secondary_order(
                schema,
                order,
                index,
                branch_prefix_len,
                false,
            )
        {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index,
            branch_prefix_len,
            false,
            0,
            false,
        );
        match &best {
            None => best = Some((score, index, fixed_values, branch_values)),
            Some((best_score, best_index, _, _))
                if access_candidate_score_outranks(score, *best_score, true)
                    || (score == *best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index, fixed_values, branch_values));
            }
            Some(_) => {}
        }
    }

    best.map(|(_, index, fixed_values, branch_values)| {
        if let [branch_value] = branch_values.as_slice() {
            let mut values = fixed_values;
            values.push(branch_value.clone());
            AccessPlan::index_prefix_from_contract(index.clone(), values)
        } else {
            AccessPlan::index_branch_set_from_contract(index.clone(), fixed_values, branch_values)
        }
    })
}

fn primary_key_asc_order(schema: &SchemaInfo, order: &OrderSpec) -> bool {
    order.primary_key_only_direction_fields(schema.primary_key_names()) == Some(OrderDirection::Asc)
}

fn collect_branch_set_literals<'a>(
    schema: &SchemaInfo,
    children: &'a [Predicate],
    eq_values: &mut Vec<CachedEqLiteral<'a>>,
    in_values: &mut Vec<CachedSetLiteral<'a>>,
    excluded_values: &mut Vec<CachedSetLiteral<'a>>,
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
                in_values.push(CachedSetLiteral {
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
            CompareOp::Ne => {
                excluded_values.push(CachedSetLiteral {
                    field: cmp.field.as_str(),
                    values: vec![CachedInValue {
                        value: &cmp.value,
                        compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
                    }],
                    coercion: cmp.coercion.id,
                });
            }
            CompareOp::NotIn => {
                let Value::List(values) = &cmp.value else {
                    continue;
                };
                excluded_values.push(CachedSetLiteral {
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

struct CachedSetLiteral<'a> {
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
    build_index_eq_prefix_for_items(index_contract.key_items(), field_values)
}

fn build_index_eq_prefix_for_items(
    key_items: &[crate::db::access::SemanticIndexKeyItem],
    field_values: &[CachedEqLiteral<'_>],
) -> Option<Vec<Value>> {
    let mut prefix = Vec::new();
    for key_item in key_items {
        let key_item = key_item.as_ref();
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
    in_values: &[CachedSetLiteral<'_>],
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

fn prune_branch_values_by_exclusions(
    key_item: SemanticIndexKeyItemRef<'_>,
    branch_values: &mut Vec<Value>,
    excluded_values: &[CachedSetLiteral<'_>],
) {
    branch_values.retain(|branch_value| {
        !excluded_values.iter().any(|excluded| {
            if key_item.field() != excluded.field {
                return false;
            }
            excluded.values.iter().any(|excluded_value| {
                if !excluded_value.compatible {
                    return false;
                }
                eq_lookup_value_for_key_item(
                    key_item,
                    excluded.field,
                    excluded_value.value,
                    excluded.coercion,
                    true,
                )
                .is_some_and(|lookup_value| lookup_value == *branch_value)
            })
        })
    });
}
