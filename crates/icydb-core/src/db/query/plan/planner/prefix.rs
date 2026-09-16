//! Module: query::plan::planner::prefix
//! Responsibility: planner prefix/multi-lookup access-path derivation from predicate equality sets.
//! Does not own: runtime index traversal execution or continuation resume behavior.
//! Boundary: maps prefix-capable predicates into planner-owned access plan candidates.

#[cfg(test)]
mod branch_tests;
#[cfg(test)]
mod equality_tests;
#[cfg(test)]
mod multi_lookup_tests;

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
                copy_lookup_value_for_key_item, key_item_matches_field_and_coercion,
                key_item_supports_lookup_value, lower_lookup_value_for_key_item,
            },
            order_contract::CandidateOrderContract,
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_field_literal_matcher,
                index_literal_matches_schema,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::{Value, canonicalize_value_set},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Cow;

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
            Ok(key_item_supports_lookup_value(
                key, field, value, coercion, true,
            ))
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
    for value in values {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        if !matcher.matches(value) {
            return Ok(None);
        }
    }
    let Some(index) =
        best_leading_lookup_index(candidate_indexes, schema, order, grouped, budget, |key| {
            // Strict fields need no second literal walk after shared schema admission.
            // Expression eligibility still uses the canonical per-value shape gate.
            if !key_item_matches_field_and_coercion(key, field, coercion) {
                return Ok(false);
            }
            if key.is_expression() {
                for value in values {
                    budget.charge(Resource::PredicateExpressionSteps, 1)?;
                    if !key_item_supports_lookup_value(key, field, value, coercion, true) {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
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
    supports: impl Fn(SemanticIndexKeyItemRef<'_>) -> Result<bool, InternalError>,
) -> Result<Option<&'a SemanticIndexAccessContract>, InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let mut best: Option<(AccessCandidateScore, &SemanticIndexAccessContract)> = None;
    let order_contract = CandidateOrderContract::prepare(schema, order, grouped, budget)?;
    for index in candidate_indexes {
        let Some(key) = index.key_item_at(0) else {
            continue;
        };
        if !supports(key)? {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            order_contract.as_ref(),
            index,
            1,
            index.key_arity() == 1,
            0,
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
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    // Cache literal/schema compatibility once per equality literal so index
    // candidate selection does not repeat schema checks on every index iteration.
    let mut field_values = Vec::new();
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;

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
        budget.reserve_vec(&mut field_values, 1)?;
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
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let order_contract = CandidateOrderContract::prepare(schema, order, grouped, budget)?;
    for index in candidate_indexes {
        let Some(prefix) = build_index_eq_prefix(index.key_items(), &field_values, budget)? else {
            continue;
        };
        if prefix.is_empty() {
            continue;
        }

        let score = access_candidate_score_from_index_contract(
            order_contract.as_ref(),
            index,
            prefix.len(),
            prefix.len() == index.key_arity(),
            0,
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

    best.map(|(_, index, values)| {
        budget.charge(
            Resource::TemporaryBytes,
            size_of::<AccessPath<Value>>() as u64,
        )?;
        Ok(AccessPlan::index_prefix_from_contract(
            index.clone(),
            values,
        ))
    })
    .transpose()
}

pub(super) fn index_branch_set_from_and(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    index_branch_set_from_and_with_cap(
        candidate_indexes,
        schema,
        children,
        order,
        grouped,
        MAX_INDEX_BRANCH_SET_VALUES,
        budget,
    )
}

pub(in crate::db::query) fn count_cardinality_index_branch_set_from_and(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    max_branch_values: usize,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
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
        budget,
    )
}

fn index_branch_set_from_and_with_cap(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
    max_branch_values: usize,
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    if grouped || order.is_some_and(|order| !primary_key_asc_order(schema, order)) {
        return Ok(None);
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
        budget,
    )?;
    if eq_values.is_empty() || in_values.is_empty() {
        return Ok(None);
    }

    let mut best: Option<(
        AccessCandidateScore,
        &SemanticIndexAccessContract,
        Vec<Value>,
        Vec<Value>,
    )> = None;
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    let order_contract = CandidateOrderContract::prepare(schema, order, grouped, budget)?;
    for index in candidate_indexes {
        let Some(fixed_values) = build_index_eq_prefix(index.key_items(), &eq_values, budget)?
        else {
            continue;
        };
        if fixed_values.is_empty() {
            continue;
        }

        let branch_slot = fixed_values.len();
        let Some(branch_key_item) = index.key_item_at(branch_slot) else {
            continue;
        };
        let Some(branch_values) = build_index_branch_values(branch_key_item, &in_values, budget)?
        else {
            continue;
        };
        let mut branch_values = branch_values;
        prune_branch_values_by_exclusions(
            branch_key_item,
            &mut branch_values,
            &excluded_values,
            budget,
        )?;
        if branch_values.is_empty() || branch_values.len() > max_branch_values {
            continue;
        }

        let branch_prefix_len = branch_slot.saturating_add(1);
        let score = access_candidate_score_from_index_contract(
            order_contract.as_ref(),
            index,
            branch_prefix_len,
            false,
            0,
        );
        // Eligibility and ranking consume the same ordering decision.
        if order.is_some() && !score.order_compatible {
            continue;
        }
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

    let Some((_, index, mut fixed_values, branch_values)) = best else {
        return Ok(None);
    };
    budget.charge(
        Resource::TemporaryBytes,
        size_of::<AccessPath<Value>>() as u64,
    )?;
    Ok(Some(if branch_values.len() == 1 {
        budget.reserve_vec(&mut fixed_values, 1)?;
        fixed_values.extend(branch_values);
        AccessPlan::index_prefix_from_contract(index.clone(), fixed_values)
    } else {
        AccessPlan::index_branch_set_from_contract(index.clone(), fixed_values, branch_values)
    }))
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
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
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
                budget.reserve_vec(eq_values, 1)?;
                eq_values.push(CachedEqLiteral {
                    field: cmp.field.as_str(),
                    value: &cmp.value,
                    coercion: cmp.coercion.id,
                    compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
                });
            }
            CompareOp::In | CompareOp::Ne | CompareOp::NotIn => {
                let values = match (&cmp.op, &cmp.value) {
                    (CompareOp::Ne, value) => std::slice::from_ref(value),
                    (_, Value::List(values)) => values.as_slice(),
                    _ => continue,
                };
                let destination = if cmp.op == CompareOp::In {
                    &mut *in_values
                } else {
                    &mut *excluded_values
                };
                budget.reserve_vec(destination, 1)?;
                budget.charge(Resource::PredicateExpressionSteps, values.len() as u64)?;
                let mut cached_values = budget.vec_with_capacity(values.len())?;
                cached_values.extend(values.iter().map(|value| CachedInValue {
                    value,
                    compatible: index_literal_matches_schema(schema, &cmp.field, value),
                }));
                destination.push(CachedSetLiteral {
                    field: cmp.field.as_str(),
                    values: cached_values,
                    coercion: cmp.coercion.id,
                });
            }
            _ => {}
        }
    }
    Ok(())
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
    key_items: &[crate::db::access::SemanticIndexKeyItem],
    field_values: &[CachedEqLiteral<'_>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<Vec<Value>>, InternalError> {
    // Bound key/literal visits up front, including slots after a possible gap.
    // Payload comparisons remain distinct from these structural visit units.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (key_items.len() as u64).saturating_mul((field_values.len() as u64).saturating_add(1)),
    )?;
    let mut prefix = Vec::new();
    for key_item in key_items {
        let key_item = key_item.as_ref();
        let mut matched: Option<Cow<'_, Value>> = None;
        for cached in field_values {
            let Some(candidate) = lower_lookup_value_for_key_item(
                key_item,
                cached.field,
                cached.value,
                cached.coercion,
                cached.compatible,
                budget,
            )?
            else {
                continue;
            };

            if let Some(existing) = &matched
                && !budget.values_equal(existing, &candidate)?
            {
                return Ok(None);
            }
            matched = Some(candidate);
        }

        let Some(value) = matched else {
            break;
        };
        // Preserve the last equal literal's representation, as before, but
        // borrow unchanged duplicates until one value is retained per slot.
        if prefix.is_empty() {
            prefix = budget.vec_with_capacity(key_items.len())?;
        }
        prefix.push(match value {
            Cow::Borrowed(value) => budget.copy_value(value)?,
            Cow::Owned(value) => value,
        });
    }

    Ok(Some(prefix))
}

fn build_index_branch_values(
    key_item: SemanticIndexKeyItemRef<'_>,
    in_values: &[CachedSetLiteral<'_>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<Vec<Value>>, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, in_values.len() as u64)?;
    let mut matched: Option<Vec<Value>> = None;
    for cached in in_values {
        if key_item.field() != cached.field {
            continue;
        }

        budget.charge(
            Resource::PredicateExpressionSteps,
            cached.values.len() as u64,
        )?;
        let mut branch_values = budget.vec_with_capacity(cached.values.len())?;
        for cached_value in &cached.values {
            if !cached_value.compatible {
                return Ok(None);
            }
            let Some(lookup_value) = copy_lookup_value_for_key_item(
                key_item,
                cached.field,
                cached_value.value,
                cached.coercion,
                true,
                budget,
            )?
            else {
                return Ok(None);
            };
            branch_values.push(lookup_value);
        }
        canonicalize_value_set(&mut branch_values);
        if branch_values.is_empty() {
            return Ok(None);
        }

        if let Some(existing) = &matched
            && !budget.value_slices_equal(existing, &branch_values)?
        {
            return Ok(None);
        }
        matched = Some(branch_values);
    }

    Ok(matched)
}

fn prune_branch_values_by_exclusions(
    key_item: SemanticIndexKeyItemRef<'_>,
    branch_values: &mut Vec<Value>,
    excluded_values: &[CachedSetLiteral<'_>],
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    budget.charge(
        Resource::PredicateExpressionSteps,
        excluded_values.len() as u64,
    )?;
    for excluded in excluded_values {
        if key_item.field() != excluded.field || branch_values.is_empty() {
            continue;
        }
        // Reserve the whole set's structural visits before pruning. Later
        // passes may inspect fewer branches; payload comparison is separate.
        budget.charge(
            Resource::PredicateExpressionSteps,
            (excluded.values.len() as u64)
                .saturating_mul((branch_values.len() as u64).saturating_add(1)),
        )?;
        for excluded_value in &excluded.values {
            if branch_values.is_empty() {
                break;
            }
            // Normalize each exclusion once, borrowing unchanged values rather
            // than constructing the same operand for every retained branch.
            let Some(lookup_value) = lower_lookup_value_for_key_item(
                key_item,
                excluded.field,
                excluded_value.value,
                excluded.coercion,
                excluded_value.compatible,
                budget,
            )?
            else {
                continue;
            };
            // Finish only compaction bookkeeping after exhaustion; no further
            // comparisons may run or a partially pruned candidate be published.
            let mut failure = None;
            branch_values.retain(|branch_value| {
                if failure.is_some() {
                    return true;
                }
                match budget.values_equal(lookup_value.as_ref(), branch_value) {
                    Ok(equal) => !equal,
                    Err(error) => {
                        failure = Some(error);
                        true
                    }
                }
            });
            if let Some(error) = failure {
                return Err(error);
            }
        }
    }
    Ok(())
}
