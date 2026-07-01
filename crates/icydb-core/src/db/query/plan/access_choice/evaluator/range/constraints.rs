//! Per-key range constraint classification for access-choice evaluation.

use crate::db::{
    access::{SemanticIndexAccessContract, SemanticIndexKeyItemRef},
    predicate::{CoercionId, CompareOp, ComparePredicate},
    query::plan::{
        access_choice::model::{AccessChoiceRejectedReason, RangeFieldConstraint},
        field_key_contract_supports_operator,
        key_item_match::{eq_lookup_value_for_key_item, starts_with_lookup_value_for_key_item},
        planner::index_literal_matches_schema,
    },
    schema::SchemaInfo,
};

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
    compares: &[&ComparePredicate],
) -> Result<RangeFieldConstraint, AccessChoiceRejectedReason> {
    let mut constraint = RangeFieldConstraint::default();
    let mut lower_bound_present = false;
    let mut upper_bound_present = false;

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
                    SemanticIndexKeyItemRef::Field(_) => {
                        if cmp.coercion.id != CoercionId::Strict {
                            continue;
                        }
                        if !field_key_contract_supports_operator(
                            index_contract,
                            cmp.field.as_str(),
                            cmp.op,
                        ) {
                            return Err(AccessChoiceRejectedReason::OperatorNotSupported);
                        }
                    }
                    SemanticIndexKeyItemRef::Expression(_)
                    | SemanticIndexKeyItemRef::AcceptedExpression(_) => {
                        if cmp.coercion.id != CoercionId::TextCasefold {
                            continue;
                        }
                    }
                }
                if constraint.eq_value.is_some() {
                    return Err(AccessChoiceRejectedReason::EqRangeConflict);
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
                constraint.range_bound_count =
                    if matches!(key_item, SemanticIndexKeyItemRef::Field(_)) {
                        2
                    } else {
                        1
                    };
            }
            _ => return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported),
        }
    }

    if constraint.has_range && constraint.range_bound_count == 0 {
        constraint.range_bound_count = 1;
        if lower_bound_present && upper_bound_present {
            constraint.range_bound_count = 2;
        }
    }

    Ok(constraint)
}
