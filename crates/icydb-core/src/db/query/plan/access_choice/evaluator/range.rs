use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{
            access_choice::model::{
                AccessChoiceRejectedReason, CandidateEvaluation, CandidateScore, RangeCompareKind,
                RangeFieldConstraint,
            },
            key_item_match::{
                eq_lookup_value_for_key_item, index_key_item_at, index_key_item_count,
                key_item_matches_field_and_coercion, leading_index_key_item,
                starts_with_lookup_value_for_key_item,
            },
            planner::index_literal_matches_schema,
        },
        schema::SchemaInfo,
    },
    model::index::{IndexKeyItem, IndexModel},
};

pub(in crate::db::query::plan::access_choice) fn evaluate_range_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    match predicate {
        Predicate::Compare(cmp) => evaluate_range_compare_candidate(index, schema, cmp),
        Predicate::And(children) => evaluate_range_and_candidate(index, schema, children),
        _ => CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotRangeEligible,
        ),
    }
}

fn evaluate_range_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> CandidateEvaluation {
    let evaluation = match classify_single_range_compare_kind(cmp.op) {
        Some(RangeCompareKind::StartsWith) => {
            evaluate_starts_with_range_compare_candidate(index, schema, cmp)
        }
        Some(RangeCompareKind::Ordered) => {
            evaluate_ordered_range_compare_candidate(index, schema, cmp)
        }
        None => Err(AccessChoiceRejectedReason::OperatorNotRangeSupported),
    };

    match evaluation {
        Ok(()) => CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            order_compatible: false,
        }),
        Err(reason) => CandidateEvaluation::Rejected(reason),
    }
}

fn evaluate_range_and_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> CandidateEvaluation {
    let compares = match collect_range_and_compares(children) {
        Ok(compares) => compares,
        Err(reason) => return CandidateEvaluation::Rejected(reason),
    };

    match range_candidate_score_from_compares(index, schema, &compares) {
        Ok(score) => CandidateEvaluation::Eligible(score),
        Err(reason) => CandidateEvaluation::Rejected(reason),
    }
}

fn collect_range_and_compares(
    children: &[Predicate],
) -> Result<Vec<&ComparePredicate>, AccessChoiceRejectedReason> {
    let mut compares = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return Err(AccessChoiceRejectedReason::PredicateShapeNotRangeEligible);
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
            return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
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
                CoercionId::Strict | CoercionId::TextCasefold,
            )
        ) {
            return Err(AccessChoiceRejectedReason::NonStrictCoercion);
        }
        compares.push(cmp);
    }

    if compares.is_empty() {
        return Err(AccessChoiceRejectedReason::PredicateShapeNotRangeEligible);
    }

    Ok(compares)
}

fn range_candidate_score_from_compares(
    index: &IndexModel,
    schema: &SchemaInfo,
    compares: &[&ComparePredicate],
) -> Result<CandidateScore, AccessChoiceRejectedReason> {
    let mut prefix_len = 0usize;
    let mut range_seen = false;
    let mut has_range = false;

    for slot in 0..index_key_item_count(index) {
        let Some(key_item) = index_key_item_at(index, slot) else {
            return Err(AccessChoiceRejectedReason::MissingContiguousPrefixOrRange);
        };
        let constraint =
            classify_range_constraints_for_key_item(index, schema, key_item, compares)?;

        if !range_seen {
            if constraint.eq_value.is_some() {
                prefix_len = prefix_len.saturating_add(1);
                continue;
            }
            if constraint.has_range {
                range_seen = true;
                has_range = true;
                continue;
            }
            return Err(AccessChoiceRejectedReason::MissingContiguousPrefixOrRange);
        }

        if constraint.eq_value.is_some() || constraint.has_range {
            return Err(AccessChoiceRejectedReason::NonContiguousRangeConstraints);
        }
    }

    if !has_range {
        return Err(AccessChoiceRejectedReason::MissingRangeConstraint);
    }

    Ok(CandidateScore {
        prefix_len,
        exact: false,
        order_compatible: false,
    })
}

const fn classify_single_range_compare_kind(op: CompareOp) -> Option<RangeCompareKind> {
    match op {
        CompareOp::StartsWith => Some(RangeCompareKind::StartsWith),
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            Some(RangeCompareKind::Ordered)
        }
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::EndsWith => None,
    }
}

fn evaluate_starts_with_range_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<(), AccessChoiceRejectedReason> {
    if !matches!(
        cmp.coercion.id,
        CoercionId::Strict | CoercionId::TextCasefold
    ) {
        return Err(AccessChoiceRejectedReason::NonStrictCoercion);
    }
    let Some(leading_key_item) = leading_index_key_item(index) else {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if matches!(leading_key_item, IndexKeyItem::Expression(_))
        && cmp.coercion.id == CoercionId::Strict
    {
        return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
    }

    let literal_compatible = index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());

    if starts_with_lookup_value_for_key_item(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        literal_compatible,
    )
    .is_some()
    {
        return Ok(());
    }

    if !key_item_matches_field_and_coercion(leading_key_item, cmp.field.as_str(), cmp.coercion.id) {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }
    if !literal_compatible {
        return Err(AccessChoiceRejectedReason::LiteralIncompatible);
    }

    Err(AccessChoiceRejectedReason::StartsWithPrefixInvalid)
}

fn evaluate_ordered_range_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<(), AccessChoiceRejectedReason> {
    if !matches!(
        cmp.coercion.id,
        CoercionId::Strict | CoercionId::TextCasefold
    ) {
        return Err(AccessChoiceRejectedReason::NonStrictCoercion);
    }
    let Some(leading_key_item) = leading_index_key_item(index) else {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if matches!(leading_key_item, IndexKeyItem::Expression(_))
        && cmp.coercion.id == CoercionId::Strict
    {
        return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
    }
    let literal_compatible = index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());

    if eq_lookup_value_for_key_item(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        literal_compatible,
    )
    .is_none()
    {
        if !key_item_matches_field_and_coercion(
            leading_key_item,
            cmp.field.as_str(),
            cmp.coercion.id,
        ) {
            return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
        }
        if !literal_compatible {
            return Err(AccessChoiceRejectedReason::LiteralIncompatible);
        }

        return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
    }

    match leading_key_item {
        IndexKeyItem::Field(_) => {
            if cmp.coercion.id != CoercionId::Strict {
                return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
            }
            if index.fields().first() != Some(&cmp.field.as_str()) {
                return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
            }
            if !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
                return Err(AccessChoiceRejectedReason::OperatorNotSupported);
            }
        }
        IndexKeyItem::Expression(_) => {
            if cmp.coercion.id != CoercionId::TextCasefold {
                return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
            }
        }
    }

    if index_key_item_count(index) != 1 {
        return Err(AccessChoiceRejectedReason::SingleFieldRangeRequired);
    }

    Ok(())
}

fn classify_range_constraints_for_key_item(
    index: &IndexModel,
    schema: &SchemaInfo,
    key_item: IndexKeyItem,
    compares: &[&ComparePredicate],
) -> Result<RangeFieldConstraint, AccessChoiceRejectedReason> {
    let mut constraint = RangeFieldConstraint::default();

    for cmp in compares {
        if cmp.field.as_str() != key_item.field() {
            continue;
        }

        match cmp.op {
            CompareOp::Eq => {
                let literal_compatible =
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
                let Some(candidate) = eq_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    literal_compatible,
                ) else {
                    continue;
                };
                if constraint.has_range {
                    return Err(AccessChoiceRejectedReason::EqRangeConflict);
                }
                if let Some(existing) = constraint.eq_value.as_ref()
                    && existing != &candidate
                {
                    return Err(AccessChoiceRejectedReason::ConflictingEqConstraints);
                }
                constraint.eq_value = Some(candidate);
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let Some(_candidate) = eq_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()),
                ) else {
                    continue;
                };

                match key_item {
                    IndexKeyItem::Field(_) => {
                        if cmp.coercion.id != CoercionId::Strict {
                            continue;
                        }
                        if !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
                            return Err(AccessChoiceRejectedReason::OperatorNotSupported);
                        }
                    }
                    IndexKeyItem::Expression(_) => {
                        if cmp.coercion.id != CoercionId::TextCasefold {
                            continue;
                        }
                    }
                }
                if constraint.eq_value.is_some() {
                    return Err(AccessChoiceRejectedReason::EqRangeConflict);
                }
                constraint.has_range = true;
            }
            CompareOp::StartsWith => {
                if matches!(key_item, IndexKeyItem::Expression(_))
                    && cmp.coercion.id == CoercionId::Strict
                {
                    return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
                }
                let literal_compatible =
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
                if starts_with_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    literal_compatible,
                )
                .is_none()
                {
                    return Err(AccessChoiceRejectedReason::StartsWithPrefixInvalid);
                }
                if constraint.eq_value.is_some() {
                    return Err(AccessChoiceRejectedReason::EqRangeConflict);
                }
                constraint.has_range = true;
            }
            _ => return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported),
        }
    }

    Ok(constraint)
}
