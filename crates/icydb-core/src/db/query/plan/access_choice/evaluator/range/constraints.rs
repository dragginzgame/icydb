//! Per-key range constraint classification for access-choice evaluation.

use crate::{
    db::{
        access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef},
        predicate::{CoercionId, CompareOp, Predicate},
        query::construction::ConstructionBudget,
        query::plan::{
            access_choice::model::{AccessChoiceRejectedReason, RangeFieldConstraint},
            field_key_contract_supports_operator,
            key_item_match::{
                key_item_supports_lookup_value, key_item_supports_starts_with_value,
                lower_lookup_value_for_key_item,
            },
            planner::index_literal_matches_schema,
        },
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

// This classifier keeps the full range-family rejection and bound-strength
// contract in one owner-local function so planner ranking and explain reasons
// do not drift across separate partial walkers.
#[expect(
    clippy::too_many_lines,
    reason = "range candidate classification keeps one explicit owner for rejection and bound-strength policy"
)]
pub(super) fn classify_range_constraints_for_key_item(
    index_contract: &SemanticIndexAccessContract,
    schema: &SchemaInfo,
    key_item: SemanticIndexKeyItemRef<'_>,
    compares: &[Predicate],
    budget: &dyn ConstructionBudget,
) -> Result<Result<RangeFieldConstraint, AccessChoiceRejectedReason>, InternalError> {
    // Construction failure is distinct from a successfully classified rejection.
    // Only fixed-size facts leave this owner; raw equality values stay borrowed.
    let mut constraint = RangeFieldConstraint::default();
    let mut eq_value: Option<Cow<'_, Value>> = None;
    let mut lower_bound_present = false;
    let mut upper_bound_present = false;

    for child in compares {
        let Predicate::Compare(cmp) = child else {
            return Ok(Err(
                AccessChoiceRejectedReason::PredicateShapeNotRangeEligible,
            ));
        };
        if cmp.field.as_str() != key_item.field() {
            continue;
        }

        match cmp.op {
            CompareOp::Eq => {
                let literal_compatible =
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
                let Some(candidate) = lower_lookup_value_for_key_item(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    literal_compatible,
                    budget,
                )?
                else {
                    continue;
                };
                if constraint.has_range {
                    return Ok(Err(AccessChoiceRejectedReason::EqRangeConflict));
                }
                if let Some(existing) = eq_value.as_ref()
                    && existing != &candidate
                {
                    return Ok(Err(AccessChoiceRejectedReason::ConflictingEqConstraints));
                }
                eq_value = Some(candidate);
            }
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                if !key_item_supports_lookup_value(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()),
                ) {
                    continue;
                }

                match key_item {
                    SemanticIndexKeyItemRef::Field(_) => {
                        if cmp.coercion.id != CoercionId::Strict {
                            continue;
                        }
                        if !field_key_contract_supports_operator(
                            index_contract,
                            cmp.field.as_str(),
                            cmp.op,
                        ) {
                            return Ok(Err(AccessChoiceRejectedReason::OperatorNotSupported));
                        }
                    }
                    SemanticIndexKeyItemRef::AcceptedExpression(_) => {
                        if cmp.coercion.id != CoercionId::TextCasefold {
                            continue;
                        }
                    }
                }
                if eq_value.is_some() {
                    return Ok(Err(AccessChoiceRejectedReason::EqRangeConflict));
                }
                constraint.has_range = true;
                if matches!(cmp.op, CompareOp::Gt | CompareOp::Gte) {
                    lower_bound_present = true;
                } else {
                    upper_bound_present = true;
                }
            }
            CompareOp::StartsWith => {
                if key_item.is_expression() && cmp.coercion.id == CoercionId::Strict {
                    return Ok(Err(AccessChoiceRejectedReason::OperatorNotRangeSupported));
                }
                let literal_compatible =
                    index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
                if !key_item_supports_starts_with_value(
                    key_item,
                    cmp.field.as_str(),
                    cmp.value(),
                    cmp.coercion.id,
                    literal_compatible,
                ) {
                    return Ok(Err(AccessChoiceRejectedReason::StartsWithPrefixInvalid));
                }
                if eq_value.is_some() {
                    return Ok(Err(AccessChoiceRejectedReason::EqRangeConflict));
                }
                constraint.has_range = true;
                constraint.range_bound_count =
                    if matches!(key_item, SemanticIndexKeyItemRef::Field(_)) {
                        2
                    } else {
                        1
                    };
            }
            _ => return Ok(Err(AccessChoiceRejectedReason::OperatorNotRangeSupported)),
        }
    }

    if constraint.has_range && constraint.range_bound_count == 0 {
        constraint.range_bound_count = 1;
        if lower_bound_present && upper_bound_present {
            constraint.range_bound_count = 2;
        }
    }

    constraint.has_eq = eq_value.is_some();
    Ok(Ok(constraint))
}
