use crate::{
    db::{
        access::{
            AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItemRef,
            SemanticIndexKeyItemsRef, SemanticIndexRangeSpec,
        },
        index::{TextPrefixBoundMode, starts_with_component_bounds},
        predicate::{CoercionId, CompareOp, Predicate, canonical_cmp},
        query::plan::{
            OrderSpec,
            key_item_match::{eq_lookup_value_for_key_item, starts_with_lookup_value_for_key_item},
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
    model::{
        entity::EntityModel,
        index::{IndexKeyItem, IndexKeyItemsRef},
    },
    value::Value,
};
use std::cmp::Ordering;

// Build one deterministic primary-key half-open range candidate from the
// primary-key subset of one canonical AND-group.
//
// Phase 1 intentionally keeps the same safe lower/upper-bound contract as the
// older PK-range path, but no longer requires unrelated conjuncts to disappear
// first. That lets mixed `AND` planning keep the valid primary-key range
// candidate visible when sibling clauses still need residual or secondary-index
// handling.
pub(in crate::db::query::plan::planner) fn primary_key_range_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> Option<AccessPlan<Value>> {
    let field_type = schema.field(model.primary_key.name)?;
    if !field_type.is_keyable() {
        return None;
    }

    let mut lower = None::<Value>;
    let mut upper = None::<Value>;

    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.field != model.primary_key.name {
            continue;
        }
        if cmp.coercion.id != CoercionId::Strict {
            return None;
        }
        if !literal_matches_type(&cmp.value, field_type) {
            return None;
        }

        match cmp.op {
            CompareOp::Gte if lower.is_none() => lower = Some(cmp.value.clone()),
            CompareOp::Lt if upper.is_none() => upper = Some(cmp.value.clone()),
            _ => return None,
        }
    }

    let (Some(start), Some(end)) = (lower, upper) else {
        return None;
    };
    if canonical_cmp(&start, &end) != Ordering::Less {
        return None;
    }

    Some(AccessPlan::key_range(start, end))
}

// Build one deterministic secondary-range candidate from a normalized AND-group.
//
// Extraction contract:
// - Every child must be a Compare predicate.
// - Supported operators are Eq/Gt/Gte/Lt/Lte plus StartsWith.
// - For a chosen index: slots 0..k must be Eq, slot k must be Range,
//   slots after k must be unconstrained.
pub(in crate::db::query::plan::planner) fn index_range_from_and(
    model: &EntityModel,
    candidate_indexes: &[SemanticIndexAccessContract],
    schema: &SchemaInfo,
    children: &[Predicate],
    order: Option<&OrderSpec>,
    grouped: bool,
) -> Option<SemanticIndexRangeSpec> {
    let mut compares = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
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
            return None;
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
            return None;
        }
        compares.push(CachedCompare {
            cmp,
            literal_compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(
        AccessCandidateScore,
        SemanticIndexAccessContract,
        usize,
        Vec<Value>,
        RangeConstraint,
    )> = None;
    for index in candidate_indexes {
        let Some((range_slot, prefix, range)) =
            index_range_candidate_for_index(index, schema, &compares)
        else {
            continue;
        };

        let prefix_len = prefix.len();
        let score = access_candidate_score_from_index_contract(
            model,
            order,
            index.clone(),
            prefix_len,
            false,
            range_bound_count(&range.lower, &range.upper),
            grouped,
        );
        match best {
            None => best = Some((score, index.clone(), range_slot, prefix, range)),
            Some((best_score, best_index, _, _, _))
                if access_candidate_score_outranks(score, best_score, false)
                    || (score == best_score && index.name() < best_index.name()) =>
            {
                best = Some((score, index.clone(), range_slot, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, range_slot, prefix, range)| {
        let field_slots = (0..=range_slot).collect();

        SemanticIndexRangeSpec::from_access_contract(
            index,
            field_slots,
            prefix,
            range.lower,
            range.upper,
        )
    })
}

// Extract an index-range candidate for one concrete index by walking canonical
// key slots directly instead of field names. That keeps mixed field/expression
// indexes on the same planner contract as field-only indexes.
fn index_range_candidate_for_index(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    compares: &[CachedCompare<'_>],
) -> Option<(usize, Vec<Value>, RangeConstraint)> {
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => index_range_candidate_for_key_items(
            index_contract,
            schema,
            fields
                .iter()
                .map(|field| SemanticIndexKeyItemRef::Field(field.as_str())),
            compares,
        ),
        SemanticIndexKeyItemsRef::Accepted(items) => index_range_candidate_for_key_items(
            index_contract,
            schema,
            items.iter().map(|item| item.as_ref()),
            compares,
        ),
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            index_range_candidate_for_key_items(
                index_contract,
                schema,
                fields.iter().copied().map(SemanticIndexKeyItemRef::Field),
                compares,
            )
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
            index_range_candidate_for_key_items(
                index_contract,
                schema,
                items.iter().copied().map(Into::into),
                compares,
            )
        }
    }
}

// Field-only and mixed key-item indexes share the same prefix/range slot walk;
// only the source iterator for canonical key items differs.
fn index_range_candidate_for_key_items<'a, I>(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    key_items: I,
    compares: &[CachedCompare<'_>],
) -> Option<(usize, Vec<Value>, RangeConstraint)>
where
    I: IntoIterator<Item = SemanticIndexKeyItemRef<'a>>,
{
    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    for (position, key_item) in key_items.into_iter().enumerate() {
        let constraint =
            key_item_constraint_for_index_slot(index_contract, schema, key_item, compares)?;
        if !consume_index_slot_constraint(
            &mut prefix,
            &mut range,
            &mut range_position,
            position,
            constraint,
        ) {
            return None;
        }
    }

    let (Some(range_position), Some(range)) = (range_position, range) else {
        return None;
    };
    if prefix.len() >= index_contract.key_arity() {
        return None;
    }

    Some((range_position, prefix, range))
}

// Consume one canonical slot constraint into the contiguous prefix/range
// extractor state machine.
fn consume_index_slot_constraint(
    prefix: &mut Vec<Value>,
    range: &mut Option<RangeConstraint>,
    range_position: &mut Option<usize>,
    position: usize,
    constraint: IndexFieldConstraint,
) -> bool {
    match constraint {
        IndexFieldConstraint::Eq(value) if range.is_none() => {
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
    }
}

// Build the effective constraint class for one canonical index slot from the
// compare predicates that can lower onto that slot.
fn key_item_constraint_for_index_slot(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    key_item: SemanticIndexKeyItemRef<'_>,
    compares: &[CachedCompare<'_>],
) -> Option<IndexFieldConstraint> {
    let mut constraint = IndexFieldConstraint::None;
    let field_type = schema.field(key_item.field())?;

    for cached in compares {
        let cmp = cached.cmp;
        if cmp.field.as_str() != key_item.field() {
            continue;
        }
        if matches!(key_item, SemanticIndexKeyItemRef::Field(_))
            && cmp.coercion.id == CoercionId::Strict
            && !field_type.is_orderable()
        {
            return None;
        }

        match cmp.op {
            CompareOp::Eq => match &constraint {
                IndexFieldConstraint::None => {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                    ) else {
                        continue;
                    };
                    constraint = IndexFieldConstraint::Eq(candidate);
                }
                IndexFieldConstraint::Eq(existing) => {
                    let Some(candidate) = eq_lookup_value_for_key_item(
                        key_item,
                        cmp.field.as_str(),
                        &cmp.value,
                        cmp.coercion.id,
                        cached.literal_compatible,
                    ) else {
                        continue;
                    };
                    if existing != &candidate {
                        return None;
                    }
                }
                IndexFieldConstraint::Range(_) => return None,
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                merge_ordered_compare_constraint_for_key_item(
                    index_contract,
                    key_item,
                    cached,
                    &mut constraint,
                )?;
            }
            CompareOp::StartsWith => {
                let Some(prefix) = starts_with_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    &cmp.value,
                    cmp.coercion.id,
                    cached.literal_compatible,
                ) else {
                    continue;
                };

                let (lower, upper) = starts_with_component_bounds(
                    &prefix,
                    match key_item {
                        SemanticIndexKeyItemRef::Field(_) => TextPrefixBoundMode::Strict,
                        SemanticIndexKeyItemRef::Expression(_)
                        | SemanticIndexKeyItemRef::AcceptedExpression(_) => {
                            TextPrefixBoundMode::LowerOnly
                        }
                    },
                )?;
                let candidate = RangeConstraint { lower, upper };
                let mut range = match &constraint {
                    IndexFieldConstraint::None => candidate.clone(),
                    IndexFieldConstraint::Eq(_) => return None,
                    IndexFieldConstraint::Range(existing) => existing.clone(),
                };
                if !merge_range_constraint_bounds(&mut range, &candidate) {
                    return None;
                }
                constraint = IndexFieldConstraint::Range(range);
            }
            _ => return None,
        }
    }

    Some(constraint)
}

// Merge one ordered compare onto one canonical key-item slot.
// This keeps Eq/In/prefix/range on the same canonical literal-lowering path
// for both raw field keys and the accepted TextCasefold expression keys.
fn merge_ordered_compare_constraint_for_key_item(
    index_contract: &SemanticIndexAccessContract,
    key_item: SemanticIndexKeyItemRef<'_>,
    cached: &CachedCompare<'_>,
    constraint: &mut IndexFieldConstraint,
) -> Option<()> {
    let cmp = cached.cmp;
    let candidate = eq_lookup_value_for_key_item(
        key_item,
        cmp.field.as_str(),
        &cmp.value,
        cmp.coercion.id,
        cached.literal_compatible,
    )?;

    match key_item {
        SemanticIndexKeyItemRef::Field(_) => {
            if cmp.coercion.id != CoercionId::Strict
                || !field_key_contract_supports_operator(index_contract, key_item.field(), cmp.op)
            {
                return Some(());
            }
        }
        SemanticIndexKeyItemRef::Expression(_) | SemanticIndexKeyItemRef::AcceptedExpression(_) => {
            if cmp.coercion.id != CoercionId::TextCasefold {
                return Some(());
            }
        }
    }

    let mut range = match constraint {
        IndexFieldConstraint::None => RangeConstraint::default(),
        IndexFieldConstraint::Eq(_) => return None,
        IndexFieldConstraint::Range(existing) => existing.clone(),
    };
    if !merge_range_constraint(&mut range, cmp.op, &candidate) {
        return None;
    }

    *constraint = IndexFieldConstraint::Range(range);
    Some(())
}

fn field_key_contract_supports_operator(
    index_contract: &SemanticIndexAccessContract,
    field: &str,
    op: CompareOp,
) -> bool {
    if index_contract.has_expression_key_items() {
        return false;
    }
    if !contract_contains_field_key(index_contract, field) {
        return false;
    }

    matches!(
        op,
        CompareOp::Eq
            | CompareOp::In
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::StartsWith
    )
}

fn contract_contains_field_key(index_contract: &SemanticIndexAccessContract, field: &str) -> bool {
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => {
            fields.iter().any(|key_field| key_field == field)
        }
        SemanticIndexKeyItemsRef::Accepted(items) => items
            .iter()
            .any(|item| matches!(item.as_ref(), SemanticIndexKeyItemRef::Field(key_field) if key_field == field)),
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            fields.contains(&field)
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => items
            .iter()
            .any(|item| matches!(item, IndexKeyItem::Field(key_field) if key_field == &field)),
    }
}
