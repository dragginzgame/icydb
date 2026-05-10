#[cfg(test)]
use crate::model::index::IndexModel;
use crate::{
    db::{
        access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef, SemanticIndexKeyItemsRef},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::plan::{
            access_choice::model::{
                AccessChoiceRejectedReason, CandidateEvaluation, CandidateScore,
            },
            key_item_match::{eq_lookup_value_for_key_item, key_item_matches_field_and_coercion},
            planner::index_literal_matches_schema,
        },
        schema::SchemaInfo,
    },
    model::index::{IndexKeyItem, IndexKeyItemsRef},
    value::Value,
};

pub(super) fn evaluate_prefix_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    match predicate {
        Predicate::Compare(cmp) => {
            evaluate_prefix_compare_candidate_from_contract(index_contract, schema, cmp)
        }
        Predicate::And(children) => evaluate_prefix_and_candidate(index_contract, schema, children),
        _ => CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotPrefixEligible,
        ),
    }
}

pub(super) fn evaluate_prefix_compare_candidate_from_contract(
    index_contract: &SemanticIndexAccessContract,
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
        resolve_leading_lookup_key_item(index_contract, cmp.field.as_str(), cmp.coercion.id)
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

    eligible_single_lookup_candidate(index_contract.clone())
}

fn evaluate_prefix_and_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> CandidateEvaluation {
    let eq_constraints = collect_prefix_eq_constraints(schema, children);
    if eq_constraints.is_empty() {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NoEqConstraints);
    }

    let prefix_len = match evaluate_prefix_len_for_key_items(index_contract, &eq_constraints) {
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
        exact: prefix_len == index_contract.key_arity(),
        filtered: index_contract.is_filtered(),
        range_bound_count: 0,
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
    index_contract: &SemanticIndexAccessContract,
    eq_constraints: &[(&str, &Value, CoercionId, bool)],
) -> Result<usize, AccessChoiceRejectedReason> {
    let mut prefix_len = 0usize;
    match index_contract.key_items() {
        SemanticIndexKeyItemsRef::Fields(fields) => {
            for field in fields {
                match match_eq_constraint_value_for_key_item(
                    SemanticIndexKeyItemRef::Field(field.as_str()),
                    eq_constraints,
                ) {
                    Ok(Some(_)) => prefix_len = prefix_len.saturating_add(1),
                    Ok(None) => break,
                    Err(reason) => return Err(reason),
                }
            }
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            for &field in fields {
                match match_eq_constraint_value_for_key_item(
                    IndexKeyItem::Field(field),
                    eq_constraints,
                ) {
                    Ok(Some(_)) => prefix_len = prefix_len.saturating_add(1),
                    Ok(None) => break,
                    Err(reason) => return Err(reason),
                }
            }
        }
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
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

fn match_eq_constraint_value_for_key_item<'a>(
    key_item: impl Into<SemanticIndexKeyItemRef<'a>>,
    eq_constraints: &[(&str, &Value, CoercionId, bool)],
) -> Result<Option<Value>, AccessChoiceRejectedReason> {
    let key_item = key_item.into();
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

pub(super) fn evaluate_multi_lookup_candidate_from_contract(
    index_contract: &SemanticIndexAccessContract,
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
        resolve_leading_lookup_key_item(index_contract, cmp.field.as_str(), cmp.coercion.id)
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

    eligible_single_lookup_candidate(index_contract.clone())
}

#[cfg(test)]
pub(in crate::db::query::plan::access_choice) fn evaluate_prefix_compare_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> CandidateEvaluation {
    evaluate_prefix_compare_candidate_from_contract(
        &SemanticIndexAccessContract::from_generated_index(*index),
        schema,
        cmp,
    )
}

#[cfg(test)]
pub(in crate::db::query::plan::access_choice) fn evaluate_multi_lookup_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    evaluate_multi_lookup_candidate_from_contract(
        &SemanticIndexAccessContract::from_generated_index(*index),
        schema,
        predicate,
    )
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
fn resolve_leading_lookup_key_item<'a>(
    index_contract: &'a SemanticIndexAccessContract,
    field: &str,
    coercion: CoercionId,
) -> Result<SemanticIndexKeyItemRef<'a>, AccessChoiceRejectedReason> {
    let Some(leading_key_item) = index_contract.key_item_at(0) else {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if !key_item_matches_field_and_coercion(leading_key_item, field, coercion) {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }

    Ok(leading_key_item)
}

// Emit the canonical single-slot eligible score shared by exact prefix and
// multi-lookup candidates after the leading key item has matched.
fn eligible_single_lookup_candidate(
    index_contract: SemanticIndexAccessContract,
) -> CandidateEvaluation {
    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len: 1,
        exact: index_contract.key_arity() == 1,
        filtered: index_contract.is_filtered(),
        range_bound_count: 0,
        order_compatible: false,
    })
}
