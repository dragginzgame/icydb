#[cfg(all(test, feature = "sql"))]
mod branch_tests;
#[cfg(all(test, feature = "sql"))]
mod equality_tests;
#[cfg(test)]
mod multi_lookup_tests;

use crate::{
    db::{
        access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::construction::ConstructionBudget,
        query::plan::{
            access_choice::model::{
                AccessChoiceRejectedReason, CandidateEvaluation, CandidateScore,
            },
            key_item_match::{
                key_item_matches_field_and_coercion, key_item_supports_lookup_value,
                lower_lookup_value_for_key_item,
            },
            planner::{
                MAX_INDEX_BRANCH_SET_VALUES, index_field_literal_matcher,
                index_literal_matches_schema,
            },
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::borrow::Cow;

pub(super) fn evaluate_prefix_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    Ok(match predicate {
        Predicate::Compare(cmp) => {
            evaluate_prefix_compare_candidate_from_contract(index_contract, schema, cmp)
        }
        Predicate::And(children) => {
            evaluate_prefix_and_candidate(index_contract, schema, children, budget)?
        }
        _ => CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotPrefixEligible,
        ),
    })
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
    if !key_item_supports_lookup_value(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        true,
    ) {
        return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }

    eligible_single_lookup_candidate(index_contract.clone())
}

fn evaluate_prefix_and_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    children: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    let eq_constraints = collect_prefix_eq_constraints(schema, children, budget)?;
    if eq_constraints.is_empty() {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::NoEqConstraints,
        ));
    }

    // Only the prefix length survives. Borrow unchanged duplicate operands and
    // admit expression conversion without building an owned prefix value list.
    // This batch covers slots/constraint visits, not payload comparisons.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (index_contract.key_arity() as u64)
            .saturating_mul((eq_constraints.len() as u64).saturating_add(1)),
    )?;
    let mut prefix_len = 0usize;
    for item in index_contract.key_items() {
        let key_item = item.as_ref();
        let mut matched: Option<Cow<'_, Value>> = None;
        let mut saw_incompatible = false;
        for (field, value, coercion, compatible) in &eq_constraints {
            if key_item.field() != *field {
                continue;
            }
            if !compatible {
                saw_incompatible = true;
                continue;
            }
            let Some(candidate) =
                lower_lookup_value_for_key_item(key_item, field, value, *coercion, true, budget)?
            else {
                continue;
            };
            if let Some(existing) = &matched
                && !budget.values_equal(existing, &candidate)?
            {
                return Ok(CandidateEvaluation::Rejected(
                    AccessChoiceRejectedReason::ConflictingEqConstraints,
                ));
            }
            matched = Some(candidate);
        }
        if matched.is_some() {
            prefix_len = prefix_len.saturating_add(1);
        } else if saw_incompatible {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::LiteralIncompatible,
            ));
        } else {
            break;
        }
    }
    if prefix_len == 0 {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::LeadingFieldUnconstrained,
        ));
    }

    Ok(CandidateEvaluation::Eligible(CandidateScore {
        prefix_len,
        exact: prefix_len == index_contract.key_arity(),
        filtered: index_contract.is_filtered(),
        range_bound_count: 0,
        order_compatible: false,
    }))
}

fn collect_prefix_eq_constraints<'a>(
    schema: &SchemaInfo,
    children: &'a [Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<Vec<(&'a str, &'a Value, CoercionId, bool)>, InternalError> {
    // Every child can supply an equality. Reserve backing and the child walk
    // before inspection; schema compatibility remains cached once per literal.
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    let mut out = budget.vec_with_capacity(children.len())?;
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

    Ok(out)
}

pub(super) fn evaluate_multi_lookup_candidate_from_contract(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    let Predicate::Compare(cmp) = predicate else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotMultiLookup,
        ));
    };
    if let Err(reason) = ensure_lookup_coercion_supported(cmp.coercion.id) {
        return Ok(CandidateEvaluation::Rejected(reason));
    }
    if cmp.op != CompareOp::In {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::OperatorNotMultiLookupIn,
        ));
    }
    let Ok(leading_key_item) =
        resolve_leading_lookup_key_item(index_contract, cmp.field.as_str(), cmp.coercion.id)
    else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::LeadingFieldMismatch,
        ));
    };

    let Value::List(values) = cmp.value() else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::InLiteralNotList,
        ));
    };
    if values.is_empty() {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::InLiteralEmpty,
        ));
    }
    let matcher = index_field_literal_matcher(schema, cmp.field.as_str());
    for value in values {
        budget.charge(Resource::PredicateExpressionSteps, 1)?;
        let literal_compatible = matcher.matches(value);
        if !key_item_supports_lookup_value(
            leading_key_item,
            cmp.field.as_str(),
            value,
            cmp.coercion.id,
            literal_compatible,
        ) {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::InLiteralIncompatible,
            ));
        }
    }

    Ok(eligible_single_lookup_candidate(index_contract.clone()))
}

pub(super) fn evaluate_branch_set_candidate_from_contract(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    let Predicate::And(children) = predicate else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotBranchSet,
        ));
    };

    let fixed_prefix_len =
        match evaluate_prefix_and_candidate(index_contract, schema, children, budget)? {
            CandidateEvaluation::Eligible(score) => score.prefix_len,
            rejected @ CandidateEvaluation::Rejected(_) => return Ok(rejected),
        };

    evaluate_branch_values(index_contract, fixed_prefix_len, schema, children, budget)
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

// Branch scoring needs set equality and cardinality, not retained owned operands.
// Canonicalize borrowed raw values alongside admitted expression output; keep
// the cap check after all matching clauses so diagnostic precedence is unchanged.
fn evaluate_branch_values(
    index_contract: &SemanticIndexAccessContract,
    fixed_prefix_len: usize,
    schema: &SchemaInfo,
    children: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    let Some(key_item) = index_contract.key_item_at(fixed_prefix_len) else {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::MissingContiguousPrefixOrRange,
        ));
    };
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    let mut matched: Option<Vec<Cow<'_, Value>>> = None;
    for child in children {
        let Predicate::Compare(cmp) = child else {
            continue;
        };
        if cmp.op != CompareOp::In || key_item.field() != cmp.field.as_str() {
            continue;
        }
        if let Err(reason) = ensure_lookup_coercion_supported(cmp.coercion.id) {
            return Ok(CandidateEvaluation::Rejected(reason));
        }
        let Value::List(values) = cmp.value() else {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::InLiteralNotList,
            ));
        };
        if values.is_empty() {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::InLiteralEmpty,
            ));
        }

        // Bound literal visits plus a worst-case equal-set slot walk. The
        // shared equality owner admits payloads; sorting remains separate.
        budget.charge(
            Resource::PredicateExpressionSteps,
            (values.len() as u64).saturating_mul(2),
        )?;
        let mut branch_values = budget.vec_with_capacity(values.len())?;
        let literal_matcher = index_field_literal_matcher(schema, cmp.field.as_str());
        for value in values {
            let literal_compatible = literal_matcher.matches(value);
            let Some(lookup_value) = lower_lookup_value_for_key_item(
                key_item,
                cmp.field.as_str(),
                value,
                cmp.coercion.id,
                literal_compatible,
                budget,
            )?
            else {
                return Ok(CandidateEvaluation::Rejected(
                    AccessChoiceRejectedReason::InLiteralIncompatible,
                ));
            };
            branch_values.push(lookup_value);
        }
        crate::value::canonicalize_value_set(&mut branch_values);
        if let Some(existing) = &matched
            && !budget.value_slices_equal(existing, &branch_values)?
        {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::ConflictingEqConstraints,
            ));
        }
        matched = Some(branch_values);
    }

    let branch_count = matched.as_ref().map_or(0, Vec::len);
    if !(2..=MAX_INDEX_BRANCH_SET_VALUES).contains(&branch_count) {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotBranchSet,
        ));
    }

    let prefix_len = fixed_prefix_len.saturating_add(1);
    Ok(CandidateEvaluation::Eligible(CandidateScore {
        prefix_len,
        exact: prefix_len == index_contract.key_arity(),
        filtered: index_contract.is_filtered(),
        range_bound_count: 0,
        order_compatible: false,
    }))
}
