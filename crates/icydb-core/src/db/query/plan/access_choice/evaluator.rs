use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            explain::ExplainAccessPath,
            plan::{
                access_choice::model::{
                    AccessChoiceFamily, AccessChoiceRejectedReason, AccessChoiceSelectedReason,
                    CandidateEvaluation, CandidateScore, RangeCompareKind, RangeFieldConstraint,
                },
                key_item_match::{
                    eq_lookup_value_for_key_item, index_key_item_count,
                    key_item_matches_field_and_coercion, leading_index_key_item,
                    starts_with_lookup_value_for_key_item,
                },
                planner::{index_literal_matches_schema, sorted_model_indexes},
            },
        },
        schema::SchemaInfo,
    },
    model::{
        entity::EntityModel,
        index::{IndexKeyItem, IndexModel},
    },
    value::Value,
};

pub(super) fn sorted_indexes(model: &EntityModel) -> Vec<&IndexModel> {
    sorted_model_indexes(model)
}

pub(super) const fn chosen_access_shape_projection(
    access: &ExplainAccessPath,
) -> (AccessChoiceFamily, Option<&str>, CandidateScore) {
    match access {
        ExplainAccessPath::ByKey { .. }
        | ExplainAccessPath::ByKeys { .. }
        | ExplainAccessPath::KeyRange { .. }
        | ExplainAccessPath::FullScan
        | ExplainAccessPath::Union(_)
        | ExplainAccessPath::Intersection(_) => (
            AccessChoiceFamily::NonIndex,
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            ..
        } => (
            AccessChoiceFamily::Prefix,
            Some(*name),
            CandidateScore {
                prefix_len: *prefix_len,
                exact: *prefix_len == fields.len(),
            },
        ),
        ExplainAccessPath::IndexMultiLookup { name, fields, .. } => (
            AccessChoiceFamily::MultiLookup,
            Some(*name),
            CandidateScore {
                prefix_len: 1,
                exact: fields.len() == 1,
            },
        ),
        ExplainAccessPath::IndexRange {
            name, prefix_len, ..
        } => (
            AccessChoiceFamily::Range,
            Some(*name),
            CandidateScore {
                prefix_len: *prefix_len,
                exact: false,
            },
        ),
    }
}

pub(super) fn evaluate_index_candidate(
    family: AccessChoiceFamily,
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: Option<&Predicate>,
) -> CandidateEvaluation {
    let Some(predicate) = predicate else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::PredicateAbsent);
    };

    match family {
        AccessChoiceFamily::Prefix => evaluate_prefix_candidate(index, schema, predicate),
        AccessChoiceFamily::MultiLookup => {
            evaluate_multi_lookup_candidate(index, schema, predicate)
        }
        AccessChoiceFamily::Range => evaluate_range_candidate(index, schema, predicate),
        AccessChoiceFamily::NonIndex => {
            CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonIndexAccess)
        }
    }
}

fn evaluate_prefix_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    match predicate {
        Predicate::Compare(cmp) => evaluate_prefix_compare_candidate(index, schema, cmp),
        Predicate::And(children) => evaluate_prefix_and_candidate(index, schema, children),
        _ => CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotPrefixEligible,
        ),
    }
}

pub(crate) fn evaluate_prefix_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> CandidateEvaluation {
    if !matches!(
        cmp.coercion.id,
        CoercionId::Strict | CoercionId::TextCasefold
    ) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonStrictCoercion);
    }
    if cmp.op != CompareOp::Eq {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotPrefixEq);
    }
    if !index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LiteralIncompatible);
    }
    let Some(leading_key_item) = leading_index_key_item(index) else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if eq_lookup_value_for_key_item(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        true,
    )
    .is_none()
    {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }

    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len: 1,
        exact: index_key_item_count(index) == 1,
    })
}

fn evaluate_prefix_and_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> CandidateEvaluation {
    let eq_constraints = collect_prefix_eq_constraints(schema, children);
    if eq_constraints.is_empty() {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NoEqConstraints);
    }

    let prefix_len = match evaluate_prefix_len_for_key_items(index, &eq_constraints) {
        Ok(prefix_len) => prefix_len,
        Err(reason) => return CandidateEvaluation::Rejected(reason),
    };
    if prefix_len == 0 {
        return CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::LeadingFieldUnconstrained,
        );
    }

    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len,
        exact: prefix_len == index_key_item_count(index),
    })
}

fn collect_prefix_eq_constraints<'a>(
    schema: &SchemaInfo,
    children: &'a [Predicate],
) -> Vec<(&'a str, &'a Value, CoercionId, bool)> {
    let mut out = Vec::new();
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
        out.push((
            cmp.field.as_str(),
            cmp.value(),
            cmp.coercion.id,
            index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()),
        ));
    }

    out
}

fn evaluate_prefix_len_for_key_items(
    index: &IndexModel,
    eq_constraints: &[(&str, &Value, CoercionId, bool)],
) -> Result<usize, AccessChoiceRejectedReason> {
    let mut prefix_len = 0usize;
    match index.key_items() {
        crate::model::index::IndexKeyItemsRef::Fields(fields) => {
            for &field in fields {
                let key_item = IndexKeyItem::Field(field);
                match match_eq_constraint_value_for_key_item(key_item, eq_constraints) {
                    Ok(Some(_)) => prefix_len = prefix_len.saturating_add(1),
                    Ok(None) => break,
                    Err(reason) => return Err(reason),
                }
            }
        }
        crate::model::index::IndexKeyItemsRef::Items(items) => {
            for &key_item in items {
                match match_eq_constraint_value_for_key_item(key_item, eq_constraints) {
                    Ok(Some(_)) => prefix_len = prefix_len.saturating_add(1),
                    Ok(None) => break,
                    Err(reason) => return Err(reason),
                }
            }
        }
    }

    Ok(prefix_len)
}

fn match_eq_constraint_value_for_key_item(
    key_item: IndexKeyItem,
    eq_constraints: &[(&str, &Value, CoercionId, bool)],
) -> Result<Option<Value>, AccessChoiceRejectedReason> {
    let mut matched: Option<Value> = None;
    let mut saw_incompatible = false;

    for (constraint_field, constraint_value, coercion, literal_compatible) in eq_constraints {
        if key_item.field() != *constraint_field {
            continue;
        }
        if !*literal_compatible {
            saw_incompatible = true;
            continue;
        }

        let Some(candidate) = eq_lookup_value_for_key_item(
            key_item,
            constraint_field,
            constraint_value,
            *coercion,
            true,
        ) else {
            continue;
        };

        if let Some(existing) = &matched
            && existing != &candidate
        {
            return Err(AccessChoiceRejectedReason::ConflictingEqConstraints);
        }
        matched = Some(candidate);
    }

    if matched.is_some() {
        return Ok(matched);
    }
    if saw_incompatible {
        return Err(AccessChoiceRejectedReason::LiteralIncompatible);
    }

    Ok(None)
}

pub(crate) fn evaluate_multi_lookup_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    let Predicate::Compare(cmp) = predicate else {
        return CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotMultiLookup,
        );
    };
    if !matches!(
        cmp.coercion.id,
        CoercionId::Strict | CoercionId::TextCasefold
    ) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonStrictCoercion);
    }
    if cmp.op != CompareOp::In {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotMultiLookupIn);
    }
    let Some(leading_key_item) = leading_index_key_item(index) else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if !key_item_matches_field_and_coercion(leading_key_item, cmp.field.as_str(), cmp.coercion.id) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }

    let Value::List(values) = cmp.value() else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::InLiteralNotList);
    };
    if values.is_empty() {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::InLiteralEmpty);
    }
    for value in values {
        let literal_compatible = index_literal_matches_schema(schema, cmp.field.as_str(), value);
        if eq_lookup_value_for_key_item(
            leading_key_item,
            cmp.field.as_str(),
            value,
            cmp.coercion.id,
            literal_compatible,
        )
        .is_none()
        {
            return CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::InLiteralIncompatible,
            );
        }
    }

    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len: 1,
        exact: index_key_item_count(index) == 1,
    })
}

pub(crate) fn evaluate_range_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    if index.has_expression_key_items()
        && !matches!(
            predicate,
            Predicate::Compare(cmp)
                if cmp.op == CompareOp::StartsWith && cmp.coercion.id == CoercionId::TextCasefold
        )
    {
        return CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::OperatorNotRangeSupported,
        );
    }

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
            CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        ) {
            return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
        }
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::NumericWiden
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

    for field in index.fields() {
        let constraint = classify_range_constraints_for_field(index, schema, field, compares)?;

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
        CoercionId::Strict | CoercionId::NumericWiden
    ) {
        return Err(AccessChoiceRejectedReason::NonStrictCoercion);
    }
    if index.fields().first() != Some(&cmp.field.as_str()) {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }
    if !index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()) {
        return Err(AccessChoiceRejectedReason::LiteralIncompatible);
    }
    if !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
        return Err(AccessChoiceRejectedReason::OperatorNotSupported);
    }
    if index.fields().len() != 1 {
        return Err(AccessChoiceRejectedReason::SingleFieldRangeRequired);
    }

    Ok(())
}

fn classify_range_constraints_for_field<'a>(
    index: &IndexModel,
    schema: &SchemaInfo,
    field: &str,
    compares: &[&'a ComparePredicate],
) -> Result<RangeFieldConstraint<'a>, AccessChoiceRejectedReason> {
    let mut constraint = RangeFieldConstraint::default();

    for cmp in compares {
        if cmp.field.as_str() != field {
            continue;
        }
        if !index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()) {
            return Err(AccessChoiceRejectedReason::LiteralIncompatible);
        }
        if !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
            return Err(AccessChoiceRejectedReason::OperatorNotSupported);
        }

        match cmp.op {
            CompareOp::Eq => {
                if constraint.has_range {
                    return Err(AccessChoiceRejectedReason::EqRangeConflict);
                }
                if let Some(existing) = constraint.eq_value
                    && existing != cmp.value()
                {
                    return Err(AccessChoiceRejectedReason::ConflictingEqConstraints);
                }
                constraint.eq_value = Some(cmp.value());
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
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

pub(super) fn chosen_selection_reason(
    family: AccessChoiceFamily,
    chosen_score: CandidateScore,
    eligible_other_scores: &[CandidateScore],
) -> AccessChoiceSelectedReason {
    if eligible_other_scores.is_empty() {
        return AccessChoiceSelectedReason::SingleCandidate;
    }

    if eligible_other_scores
        .iter()
        .all(|score| score.prefix_len < chosen_score.prefix_len)
    {
        return AccessChoiceSelectedReason::BestPrefixLen;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup
    ) && chosen_score.exact
        && eligible_other_scores
            .iter()
            .any(|score| score.prefix_len == chosen_score.prefix_len && !score.exact)
    {
        return AccessChoiceSelectedReason::ExactMatchPreferred;
    }

    AccessChoiceSelectedReason::LexicographicTiebreak
}

pub(super) const fn ranked_rejection_reason(
    family: AccessChoiceFamily,
    candidate: CandidateScore,
    chosen: CandidateScore,
) -> AccessChoiceRejectedReason {
    if candidate.prefix_len < chosen.prefix_len {
        return AccessChoiceRejectedReason::ShorterPrefix;
    }

    if matches!(
        family,
        AccessChoiceFamily::Prefix | AccessChoiceFamily::MultiLookup
    ) && !candidate.exact
        && chosen.exact
        && candidate.prefix_len == chosen.prefix_len
    {
        return AccessChoiceRejectedReason::ExactMatchPreferred;
    }

    AccessChoiceRejectedReason::LexicographicTiebreak
}
