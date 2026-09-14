use crate::{
    db::{
        access::{
            AccessPath, AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItemRef,
            SemanticIndexRangeSpec,
        },
        index::{TextPrefixBoundMode, admit_text_prefix_bounds, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, Predicate, canonical_cmp},
        query::construction::ConstructionBudget,
        query::plan::{
            OrderSpec, field_key_contract_supports_operator,
            key_item_match::{
                copy_lookup_value_for_key_item, key_item_supports_starts_with_value,
                lower_lookup_value_for_key_item,
            },
            planner::{
                AccessCandidateScore, access_candidate_score_from_index_contract,
                access_candidate_score_outranks, index_literal_matches_schema,
                range::{
                    CachedCompare, IndexFieldConstraint, RangeConstraint,
                    bounds::{merge_range_constraint, merge_range_constraint_bounds},
                },
                range_bound_count,
            },
        },
        schema::{SchemaInfo, literal_matches_type},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::cmp::Ordering;

// Build one deterministic primary-key half-open range candidate from the
// primary-key subset of one canonical AND-group.
//
// Phase 1 intentionally keeps the same safe lower/upper-bound contract as the
// direct PK-range path, but no longer requires unrelated conjuncts to disappear
// first. That lets mixed `AND` planning keep the valid primary-key range
// candidate visible when sibling clauses still need residual or secondary-index
// handling.
pub(in crate::db::query::plan::planner) fn primary_key_range_from_and(
    schema: &SchemaInfo,
    children: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<Option<AccessPlan<Value>>, InternalError> {
    // KeyRange access is currently scalar-primary-key only. Composite
    // component ranges are deferred and must stay residual/full-scan unless a
    // secondary index can satisfy them.
    let Some(primary_key_name) = schema.scalar_primary_key_name() else {
        return Ok(None);
    };
    let Some(field_type) = schema.field(primary_key_name) else {
        return Ok(None);
    };
    if !field_type.is_keyable() {
        return Ok(None);
    }

    let mut lower = None;
    let mut upper = None;

    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.field != primary_key_name {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            return Ok(None);
        }
        if !literal_matches_type(&cmp.value, field_type) {
            return Ok(None);
        }

        match cmp.op {
            CompareOp::Gte if lower.is_none() => lower = Some(&cmp.value),
            CompareOp::Lt if upper.is_none() => upper = Some(&cmp.value),
            _ => return Ok(None),
        }
    }

    let (Some(start), Some(end)) = (lower, upper) else {
        return Ok(None);
    };
    if canonical_cmp(start, end) != Ordering::Less {
        return Ok(None);
    }

    // Only a complete, compatible primary range retains operand copies.
    let start = budget.copy_value(start)?;
    let end = budget.copy_value(end)?;
    budget.charge(
        Resource::TemporaryBytes,
        size_of::<AccessPath<Value>>() as u64,
    )?;
    Ok(Some(AccessPlan::key_range(start, end)))
}

// Build one deterministic secondary-range candidate from a normalized AND-group.
//
// Extraction contract:
// - Every child must be a Compare predicate.
// - Supported operators are Eq/Gt/Gte/Lt/Lte plus StartsWith.
// - For a chosen index: slots 0..k must be Eq, slot k must be Range,
//   slots after k must be unconstrained.
pub(in crate::db::query::plan::planner) fn index_range_from_and(
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
    budget: &dyn ConstructionBudget,
) -> Result<Option<SemanticIndexRangeSpec>, InternalError> {
    let mut compares = Vec::new();
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return Ok(None);
        };
        if !matches!(
            cmp.op,
            CompareOp::Eq
                | CompareOp::Gt
                | CompareOp::Gte
                | CompareOp::Lt
                | CompareOp::Lte
                | CompareOp::StartsWith
        ) {
            return Ok(None);
        }
        if !matches!(
            (cmp.op, cmp.coercion.id),
            (
                CompareOp::Eq
                    | CompareOp::StartsWith
                    | CompareOp::Gt
                    | CompareOp::Gte
                    | CompareOp::Lt
                    | CompareOp::Lte,
                CoercionId::Strict | CoercionId::TextCasefold
            )
        ) {
            return Ok(None);
        }
        budget.reserve_vec(&mut compares, 1)?;
        compares.push(CachedCompare {
            cmp,
            literal_compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(
        AccessCandidateScore,
        &SemanticIndexAccessContract,
        usize,
        Vec<Value>,
        RangeConstraint,
    )> = None;
    budget.charge(
        Resource::PredicateExpressionSteps,
        candidate_indexes.len() as u64,
    )?;
    for index in candidate_indexes {
        let Some((range_slot, prefix, range)) =
            index_range_candidate_for_index(index, schema, &compares, budget)?
        else {
            continue;
        };

        let prefix_len = prefix.len();
        let score = access_candidate_score_from_index_contract(
            schema,
            order,
            index,
            prefix_len,
            false,
            range_bound_count(&range.lower, &range.upper),
            grouped,
        );
        match best {
            None => best = Some((score, index, range_slot, prefix, range)),
            Some((best_score, best_index, _, _, _))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index, range_slot, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, range_slot, prefix, range)| {
        let mut field_slots = budget.vec_with_capacity(range_slot + 1)?;
        field_slots.extend(0..=range_slot);

        Ok(SemanticIndexRangeSpec::from_access_contract(
            index.clone(),
            field_slots,
            prefix,
            range.lower,
            range.upper,
        ))
    })
    .transpose()
}

// Extract an index-range candidate for one concrete index by walking canonical
// key slots directly instead of field names. That keeps mixed field/expression
// indexes on the same planner contract as field-only indexes.
fn index_range_candidate_for_index(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    compares: &[CachedCompare<'_>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<(usize, Vec<Value>, RangeConstraint)>, InternalError> {
    let key_items = index_contract.key_items();
    // Admit the index/compare walk once, including slots after an early gap.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (key_items.len() as u64).saturating_mul((compares.len() as u64).saturating_add(1)),
    )?;
    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    for (position, key_item) in key_items.iter().enumerate() {
        let key_item = key_item.as_ref();
        let Some(constraint) =
            key_item_constraint_for_index_slot(index_contract, schema, key_item, compares, budget)?
        else {
            return Ok(None);
        };
        if !consume_index_slot_constraint(
            &mut prefix,
            &mut range,
            &mut range_position,
            position,
            constraint,
            budget,
        )? {
            return Ok(None);
        }
    }

    let (Some(range_position), Some(range)) = (range_position, range) else {
        return Ok(None);
    };
    if prefix.len() >= index_contract.key_arity() {
        return Ok(None);
    }

    Ok(Some((range_position, prefix, range)))
}

// Consume one canonical slot constraint into the contiguous prefix/range
// extractor state machine.
fn consume_index_slot_constraint(
    prefix: &mut Vec<Value>,
    range: &mut Option<RangeConstraint>,
    range_position: &mut Option<usize>,
    position: usize,
    constraint: IndexFieldConstraint,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    Ok(match constraint {
        IndexFieldConstraint::Eq(value) if range.is_none() => {
            budget.reserve_vec(prefix, 1)?;
            prefix.push(value);
            true
        }
        IndexFieldConstraint::Range(candidate) if range.is_none() => {
            *range = Some(candidate);
            *range_position = Some(position);
            true
        }
        IndexFieldConstraint::None if range.is_none() => false,
        IndexFieldConstraint::None => true,
        _ => false,
    })
}

// Build the effective constraint class for one canonical index slot from the
// compare predicates that can lower onto that slot.
fn key_item_constraint_for_index_slot(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    key_item: SemanticIndexKeyItemRef<'_>,
    compares: &[CachedCompare<'_>],
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexFieldConstraint>, InternalError> {
    let mut constraint = IndexFieldConstraint::None;
    let Some(field_type) = schema.field(key_item.field()) else {
        return Ok(None);
    };

    for cached in compares {
        let cmp = cached.cmp;
        if cmp.field.as_str() != key_item.field() {
            continue;
        }
        if matches!(key_item, SemanticIndexKeyItemRef::Field(_))
            && cmp.coercion.id == CoercionId::Strict
            && !field_type.is_orderable()
        {
            return Ok(None);
        }

        match cmp.op {
            CompareOp::Eq => match &constraint {
                IndexFieldConstraint::None => {
                    let Some(candidate) = copy_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                        budget,
                    )?
                    else {
                        continue;
                    };
                    constraint = IndexFieldConstraint::Eq(candidate);
                }
                IndexFieldConstraint::Eq(existing) => {
                    let Some(candidate) = lower_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                        budget,
                    )?
                    else {
                        continue;
                    };
                    if existing != candidate.as_ref() {
                        return Ok(None);
                    }
                }
                IndexFieldConstraint::Range(_) => return Ok(None),
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let Some(merged) = merge_ordered_compare_constraint_for_key_item(
                    index_contract,
                    key_item,
                    cached,
                    constraint,
                    budget,
                )?
                else {
                    return Ok(None);
                };
                constraint = merged;
            }
            CompareOp::StartsWith => {
                let Some(candidate) = starts_with_range_for_key_item(key_item, cached, budget)?
                else {
                    continue;
                };
                let mut range = match constraint {
                    IndexFieldConstraint::None => RangeConstraint::default(),
                    IndexFieldConstraint::Eq(_) => return Ok(None),
                    IndexFieldConstraint::Range(existing) => existing,
                };
                if !merge_range_constraint_bounds(&mut range, candidate) {
                    return Ok(None);
                }
                constraint = IndexFieldConstraint::Range(range);
            }
            _ => return Ok(None),
        }
    }

    Ok(Some(constraint))
}

// Borrow raw text until constructing its interval; expression lowering and
// prefix output each use their existing semantic construction allowance.
fn starts_with_range_for_key_item(
    key_item: SemanticIndexKeyItemRef<'_>,
    cached: &CachedCompare<'_>,
    budget: &dyn ConstructionBudget,
) -> Result<Option<RangeConstraint>, InternalError> {
    let cmp = cached.cmp;
    if !key_item_supports_starts_with_value(
        key_item,
        cmp.field.as_str(),
        &cmp.value,
        cmp.coercion.id,
        cached.literal_compatible,
    ) {
        return Ok(None);
    }
    let Some(prefix) = lower_lookup_value_for_key_item(
        key_item,
        cmp.field.as_str(),
        &cmp.value,
        cmp.coercion.id,
        cached.literal_compatible,
        budget,
    )?
    else {
        return Ok(None);
    };
    let Value::Text(prefix) = prefix.as_ref() else {
        return Ok(None);
    };
    let mode = if key_item.is_expression() {
        TextPrefixBoundMode::LowerOnly
    } else {
        TextPrefixBoundMode::Strict
    };
    admit_text_prefix_bounds(prefix, mode, budget)?;
    Ok(starts_with_component_bounds(prefix, mode)
        .map(|(lower, upper)| RangeConstraint { lower, upper }))
}

// Merge one ordered compare onto one canonical key-item slot.
// This keeps Eq/In/prefix/range on the same canonical literal-lowering path
// for both raw field keys and the accepted TextCasefold expression keys.
fn merge_ordered_compare_constraint_for_key_item(
    index_contract: &SemanticIndexAccessContract,
    key_item: SemanticIndexKeyItemRef<'_>,
    cached: &CachedCompare<'_>,
    constraint: IndexFieldConstraint,
    budget: &dyn ConstructionBudget,
) -> Result<Option<IndexFieldConstraint>, InternalError> {
    let cmp = cached.cmp;
    let Some(candidate) = copy_lookup_value_for_key_item(
        key_item,
        cmp.field.as_str(),
        &cmp.value,
        cmp.coercion.id,
        cached.literal_compatible,
        budget,
    )?
    else {
        return Ok(None);
    };

    match key_item {
        SemanticIndexKeyItemRef::Field(_) => {
            if cmp.coercion.id != CoercionId::Strict
                || !field_key_contract_supports_operator(index_contract, key_item.field(), cmp.op)
            {
                return Ok(Some(constraint));
            }
        }
        SemanticIndexKeyItemRef::AcceptedExpression(_) => {
            if cmp.coercion.id != CoercionId::TextCasefold {
                return Ok(Some(constraint));
            }
        }
    }

    let mut range = match constraint {
        IndexFieldConstraint::None => RangeConstraint::default(),
        IndexFieldConstraint::Eq(_) => return Ok(None),
        IndexFieldConstraint::Range(existing) => existing,
    };
    if !merge_range_constraint(&mut range, cmp.op, candidate) {
        return Ok(None);
    }

    Ok(Some(IndexFieldConstraint::Range(range)))
}
