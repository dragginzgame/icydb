mod constraints;
#[cfg(all(test, feature = "sql"))]
mod tests;

use crate::{
    db::{
        access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::construction::ConstructionBudget,
        query::plan::{
            access_choice::model::{
                AccessChoiceRejectedReason, CandidateEvaluation, CandidateScore, RangeCompareKind,
            },
            field_key_contract_supports_operator,
            key_item_match::{
                key_item_matches_field_and_coercion, key_item_supports_lookup_value,
                key_item_supports_starts_with_value,
            },
            planner::index_literal_matches_schema,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

use constraints::classify_range_constraints_for_key_item;

pub(super) fn evaluate_range_candidate_from_contract(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    predicate: &Predicate,
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    Ok(match predicate {
        Predicate::Compare(cmp) => evaluate_range_compare_candidate(index_contract, schema, cmp),
        Predicate::And(children) => {
            evaluate_range_and_candidate(index_contract, schema, children, budget)?
        }
        _ => CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::PredicateShapeNotRangeEligible,
        ),
    })
}

fn evaluate_range_compare_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> CandidateEvaluation {
    let evaluation = match classify_single_range_compare_kind(cmp.op) {
        Some(RangeCompareKind::StartsWith) => {
            evaluate_starts_with_range_compare_candidate(index_contract, schema, cmp)
        }
        Some(RangeCompareKind::Ordered) => {
            evaluate_ordered_range_compare_candidate(index_contract, schema, cmp)
        }
        None => Err(AccessChoiceRejectedReason::OperatorNotRangeSupported),
    };

    match evaluation {
        Ok(()) => CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            filtered: index_contract.is_filtered(),
            range_bound_count: single_range_compare_bound_count(index_contract, cmp.op),
            order_compatible: false,
        }),
        Err(reason) => CandidateEvaluation::Rejected(reason),
    }
}

fn evaluate_range_and_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    children: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    // Validate the whole conjunction before per-key classification so a later
    // unsupported clause retains precedence. No comparison-reference list is needed.
    budget.charge(Resource::PredicateExpressionSteps, children.len() as u64)?;
    if let Err(reason) = validate_range_and_compares(children) {
        return Ok(CandidateEvaluation::Rejected(reason));
    }

    range_candidate_score_from_compares(index_contract, schema, children, budget)
}

fn validate_range_and_compares(children: &[Predicate]) -> Result<(), AccessChoiceRejectedReason> {
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
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            return Err(AccessChoiceRejectedReason::NonStrictCoercion);
        }
    }

    if children.is_empty() {
        return Err(AccessChoiceRejectedReason::PredicateShapeNotRangeEligible);
    }

    Ok(())
}

fn range_candidate_score_from_compares(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    compares: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<CandidateEvaluation, InternalError> {
    // One conservative batch covers index slots and per-key comparison visits;
    // schema checks and payload comparisons remain separate work.
    budget.charge(
        Resource::PredicateExpressionSteps,
        (index_contract.key_arity() as u64)
            .saturating_mul((compares.len() as u64).saturating_add(1)),
    )?;
    let mut prefix_len = 0usize;
    let mut range_seen = false;
    let mut has_range = false;
    let mut range_bound_count = 0u8;

    for slot in 0..index_contract.key_arity() {
        let Some(key_item) = index_contract.key_item_at(slot) else {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::MissingContiguousPrefixOrRange,
            ));
        };
        let constraint = match classify_range_constraints_for_key_item(
            index_contract,
            schema,
            key_item,
            compares,
            budget,
        )? {
            Ok(constraint) => constraint,
            Err(reason) => return Ok(CandidateEvaluation::Rejected(reason)),
        };

        if !range_seen {
            if constraint.has_eq {
                prefix_len = prefix_len.saturating_add(1);
                continue;
            }
            if constraint.has_range {
                range_seen = true;
                has_range = true;
                range_bound_count = constraint.range_bound_count;
                continue;
            }
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::MissingContiguousPrefixOrRange,
            ));
        }

        if constraint.has_eq || constraint.has_range {
            return Ok(CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::NonContiguousRangeConstraints,
            ));
        }
    }

    if !has_range {
        return Ok(CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::MissingRangeConstraint,
        ));
    }

    Ok(CandidateEvaluation::Eligible(CandidateScore {
        prefix_len,
        exact: false,
        filtered: index_contract.is_filtered(),
        range_bound_count,
        order_compatible: false,
    }))
}

fn single_range_compare_bound_count(
    index_contract: &SemanticIndexAccessContract,
    op: CompareOp,
) -> u8 {
    match op {
        CompareOp::StartsWith
            if matches!(
                index_contract.key_item_at(0),
                Some(SemanticIndexKeyItemRef::Field(_))
            ) =>
        {
            2
        }
        CompareOp::StartsWith | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            1
        }
        _ => 0,
    }
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
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<(), AccessChoiceRejectedReason> {
    let (leading_key_item, literal_compatible) =
        prepare_single_range_compare_context(index_contract, schema, cmp)?;

    if key_item_supports_starts_with_value(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        literal_compatible,
    ) {
        return Ok(());
    }

    ensure_leading_lookup_match(
        leading_key_item,
        cmp.field.as_str(),
        cmp.coercion.id,
        literal_compatible,
    )?;

    Err(AccessChoiceRejectedReason::StartsWithPrefixInvalid)
}

fn evaluate_ordered_range_compare_candidate(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<(), AccessChoiceRejectedReason> {
    let (leading_key_item, literal_compatible) =
        prepare_single_range_compare_context(index_contract, schema, cmp)?;

    if !key_item_supports_lookup_value(
        leading_key_item,
        cmp.field.as_str(),
        cmp.value(),
        cmp.coercion.id,
        literal_compatible,
    ) {
        ensure_leading_lookup_match(
            leading_key_item,
            cmp.field.as_str(),
            cmp.coercion.id,
            literal_compatible,
        )?;

        return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
    }

    match leading_key_item {
        SemanticIndexKeyItemRef::Field(_) => {
            if cmp.coercion.id != CoercionId::Strict {
                return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
            }
            if !matches!(
                index_contract.key_item_at(0),
                Some(SemanticIndexKeyItemRef::Field(field)) if field == cmp.field.as_str()
            ) {
                return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
            }
            if !field_key_contract_supports_operator(index_contract, cmp.field.as_str(), cmp.op) {
                return Err(AccessChoiceRejectedReason::OperatorNotSupported);
            }
        }
        SemanticIndexKeyItemRef::AcceptedExpression(_) => {
            if cmp.coercion.id != CoercionId::TextCasefold {
                return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
            }
        }
    }

    Ok(())
}

// Prepare the shared single-clause range evaluation context once so starts-with
// and ordered range candidates keep the same coercion, leading-key, and
// literal-compatibility gates before they diverge on operator-specific checks.
fn prepare_single_range_compare_context<'a>(
    index_contract: &'a SemanticIndexAccessContract,
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<(SemanticIndexKeyItemRef<'a>, bool), AccessChoiceRejectedReason> {
    if !matches!(
        cmp.coercion.id,
        CoercionId::Strict | CoercionId::TextCasefold
    ) {
        return Err(AccessChoiceRejectedReason::NonStrictCoercion);
    }

    let Some(leading_key_item) = index_contract.key_item_at(0) else {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    };
    if leading_key_item.is_expression() && cmp.coercion.id == CoercionId::Strict {
        return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported);
    }

    Ok((
        leading_key_item,
        index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()),
    ))
}

// Validate the shared leading-key and literal gates after one operator-specific
// lookup attempt failed so the caller can return its own final operator reason
// without duplicating the mismatch checks.
fn ensure_leading_lookup_match(
    leading_key_item: SemanticIndexKeyItemRef<'_>,
    field: &str,
    coercion: CoercionId,
    literal_compatible: bool,
) -> Result<(), AccessChoiceRejectedReason> {
    if !key_item_matches_field_and_coercion(leading_key_item, field, coercion) {
        return Err(AccessChoiceRejectedReason::LeadingFieldMismatch);
    }
    if !literal_compatible {
        return Err(AccessChoiceRejectedReason::LiteralIncompatible);
    }

    Ok(())
}
