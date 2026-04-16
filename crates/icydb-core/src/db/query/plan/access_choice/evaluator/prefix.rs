use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{
            access_choice::model::{
                AccessChoiceRejectedReason, CandidateEvaluation, CandidateScore,
            },
            key_item_match::{
                eq_lookup_value_for_key_item, key_item_matches_field_and_coercion,
                leading_index_key_item,
            },
            planner::index_literal_matches_schema,
        },
        schema::SchemaInfo,
    },
    model::index::{IndexKeyItem, IndexModel},
    value::Value,
};

pub(super) fn evaluate_prefix_candidate(
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

pub(in crate::db::query::plan::access_choice) fn evaluate_prefix_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> CandidateEvaluation {
    if let Err(reason) = ensure_lookup_coercion_supported(cmp.coercion.id) {
        return CandidateEvaluation::Rejected(reason);
    }
    if cmp.op != CompareOp::Eq {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotPrefixEq);
    }
    if !index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LiteralIncompatible);
    }
    let Ok(leading_key_item) =
        resolve_leading_lookup_key_item(index, cmp.field.as_str(), cmp.coercion.id)
    else {
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

    eligible_single_lookup_candidate(index)
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
        exact: prefix_len == crate::db::query::plan::key_item_match::index_key_item_count(index),
        order_compatible: false,
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

pub(in crate::db::query::plan::access_choice) fn evaluate_multi_lookup_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    let Predicate::Compare(cmp) = predicate else {
        return CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotMultiLookup,
        );
    };
    if let Err(reason) = ensure_lookup_coercion_supported(cmp.coercion.id) {
        return CandidateEvaluation::Rejected(reason);
    }
    if cmp.op != CompareOp::In {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotMultiLookupIn);
    }
    let Ok(leading_key_item) =
        resolve_leading_lookup_key_item(index, cmp.field.as_str(), cmp.coercion.id)
    else {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };

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

    eligible_single_lookup_candidate(index)
}

// Keep single-field lookup families on one shared coercion gate so prefix and
// multi-lookup evaluation do not drift on which coercions still qualify as
// deterministic leading-key lookups.
const fn ensure_lookup_coercion_supported(
    coercion: CoercionId,
) -> Result<(), AccessChoiceRejectedReason> {
    if matches!(coercion, CoercionId::Strict | CoercionId::TextCasefold) {
        return Ok(());
    }

    Err(AccessChoiceRejectedReason::NonStrictCoercion)
}

// Resolve the leading key item only when it still matches the requested field
// and coercion family, since both prefix and multi-lookup paths require the
// same leading-slot ownership before they inspect literal values.
fn resolve_leading_lookup_key_item(
    index: &IndexModel,
    field: &str,
    coercion: CoercionId,
) -> Result<IndexKeyItem, AccessChoiceRejectedReason> {
    let Some(leading_key_item) = leading_index_key_item(index) else {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if !key_item_matches_field_and_coercion(leading_key_item, field, coercion) {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }

    Ok(leading_key_item)
}

// Emit the canonical single-slot eligible score shared by exact prefix and
// multi-lookup candidates after the leading key item has matched.
const fn eligible_single_lookup_candidate(index: &IndexModel) -> CandidateEvaluation {
    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len: 1,
        exact: crate::db::query::plan::key_item_match::index_key_item_count(index) == 1,
        order_compatible: false,
    })
}
