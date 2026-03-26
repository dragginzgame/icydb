//! Module: query::plan::access_choice
//! Responsibility: planner-owned access-choice explain metadata projection.
//! Does not own: access-path execution, route decisions, or explain rendering.
//! Boundary: derives deterministic candidate/rejection metadata from planning contracts.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            explain::ExplainAccessPath,
            plan::{
                AccessPlannedQuery,
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
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    value::Value,
};

///
/// AccessChoiceExplainRejected
///
/// Planner-projected rejected index candidate plus deterministic reason code.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessChoiceExplainRejected {
    pub(in crate::db) index_name: &'static str,
    pub(in crate::db) reason: AccessChoiceRejectedReason,
}

impl AccessChoiceExplainRejected {
    #[must_use]
    pub(in crate::db) fn render(&self) -> String {
        format!("index:{}={}", self.index_name, self.reason.code())
    }
}

///
/// AccessChoiceExplainSnapshot
///
/// Planner-owned access-choice explain projection consumed by executor
/// descriptor assembly without re-deriving planner ranking policies.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessChoiceExplainSnapshot {
    pub(in crate::db) chosen_label: String,
    pub(in crate::db) chosen_reason: AccessChoiceSelectedReason,
    pub(in crate::db) alternatives: Vec<&'static str>,
    pub(in crate::db) rejected: Vec<AccessChoiceExplainRejected>,
}

///
/// AccessChoiceSelectedReason
///
/// Canonical reason code taxonomy for selected access candidates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessChoiceSelectedReason {
    NonIndexAccess,
    SelectedIndexUnavailable,
    SchemaUnavailable,
    SingleCandidate,
    BestPrefixLen,
    ExactMatchPreferred,
    LexicographicTiebreak,
}

impl AccessChoiceSelectedReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::NonIndexAccess => "non_index_access",
            Self::SelectedIndexUnavailable => "selected_index_unavailable",
            Self::SchemaUnavailable => "schema_unavailable",
            Self::SingleCandidate => "single_candidate",
            Self::BestPrefixLen => "best_prefix_len",
            Self::ExactMatchPreferred => "exact_match_preferred",
            Self::LexicographicTiebreak => "lexicographic_tiebreak",
        }
    }
}

///
/// AccessChoiceRejectedReason
///
/// Canonical reason code taxonomy for rejected access candidates.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessChoiceRejectedReason {
    PredicateAbsent,
    NonIndexAccess,
    PredicateShapeNotPrefixEligible,
    PredicateShapeNotMultiLookup,
    PredicateShapeNotRangeEligible,
    NonStrictCoercion,
    OperatorNotPrefixEq,
    OperatorNotMultiLookupIn,
    OperatorNotRangeSupported,
    OperatorNotSupported,
    LeadingFieldMismatch,
    LiteralIncompatible,
    InLiteralNotList,
    InLiteralEmpty,
    InLiteralIncompatible,
    SingleFieldRangeRequired,
    StartsWithPrefixInvalid,
    EqRangeConflict,
    ConflictingEqConstraints,
    NoEqConstraints,
    LeadingFieldUnconstrained,
    MissingContiguousPrefixOrRange,
    NonContiguousRangeConstraints,
    MissingRangeConstraint,
    ShorterPrefix,
    ExactMatchPreferred,
    LexicographicTiebreak,
}

impl AccessChoiceRejectedReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::PredicateAbsent => "predicate_absent",
            Self::NonIndexAccess => "non_index_access",
            Self::PredicateShapeNotPrefixEligible => "predicate_shape_not_prefix_eligible",
            Self::PredicateShapeNotMultiLookup => "predicate_shape_not_multi_lookup",
            Self::PredicateShapeNotRangeEligible => "predicate_shape_not_range_eligible",
            Self::NonStrictCoercion => "non_strict_coercion",
            Self::OperatorNotPrefixEq => "operator_not_prefix_eq",
            Self::OperatorNotMultiLookupIn => "operator_not_multi_lookup_in",
            Self::OperatorNotRangeSupported => "operator_not_range_supported",
            Self::OperatorNotSupported => "operator_not_supported",
            Self::LeadingFieldMismatch => "leading_field_mismatch",
            Self::LiteralIncompatible => "literal_incompatible",
            Self::InLiteralNotList => "in_literal_not_list",
            Self::InLiteralEmpty => "in_literal_empty",
            Self::InLiteralIncompatible => "in_literal_incompatible",
            Self::SingleFieldRangeRequired => "single_field_range_required",
            Self::StartsWithPrefixInvalid => "startswith_prefix_invalid",
            Self::EqRangeConflict => "eq_range_conflict",
            Self::ConflictingEqConstraints => "conflicting_eq_constraints",
            Self::NoEqConstraints => "no_eq_constraints",
            Self::LeadingFieldUnconstrained => "leading_field_unconstrained",
            Self::MissingContiguousPrefixOrRange => "missing_contiguous_prefix_or_range",
            Self::NonContiguousRangeConstraints => "non_contiguous_range_constraints",
            Self::MissingRangeConstraint => "missing_range_constraint",
            Self::ShorterPrefix => "shorter_prefix",
            Self::ExactMatchPreferred => "exact_match_preferred",
            Self::LexicographicTiebreak => "lexicographic_tiebreak",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccessChoiceFamily {
    NonIndex,
    Prefix,
    MultiLookup,
    Range,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateScore {
    prefix_len: usize,
    exact: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateEvaluation {
    Eligible(CandidateScore),
    Rejected(AccessChoiceRejectedReason),
}

///
/// project_access_choice_explain_snapshot
///
/// Project planner-owned access-choice candidate metadata for EXPLAIN.
/// This keeps alternative/rejection reporting aligned to planner predicates
/// instead of model-only index hints.
///

#[must_use]
pub(in crate::db) fn project_access_choice_explain_snapshot(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    access: &ExplainAccessPath,
) -> AccessChoiceExplainSnapshot {
    // Phase 1: classify chosen access family and seed non-index fallbacks.
    let (family, chosen_label, chosen_index_name, chosen_score_hint) =
        chosen_access_shape_projection(access);
    if matches!(family, AccessChoiceFamily::NonIndex) {
        return AccessChoiceExplainSnapshot {
            chosen_label,
            chosen_reason: AccessChoiceSelectedReason::NonIndexAccess,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    }

    let Some(chosen_index_name) = chosen_index_name else {
        return AccessChoiceExplainSnapshot {
            chosen_label,
            chosen_reason: AccessChoiceSelectedReason::SelectedIndexUnavailable,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    };

    let Ok(schema_info) = SchemaInfo::from_entity_model(model) else {
        return AccessChoiceExplainSnapshot {
            chosen_label,
            chosen_reason: AccessChoiceSelectedReason::SchemaUnavailable,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        };
    };

    let predicate = plan.scalar_plan().predicate.as_ref();
    let mut chosen_score = chosen_score_hint;
    let mut alternatives = Vec::new();
    let mut rejected = Vec::new();
    let mut eligible_other_scores = Vec::new();

    // Phase 2: walk deterministic model order once so chosen-score recovery
    // and alternative/rejection projection stay under one evaluation owner.
    for index in sorted_model_indexes(model) {
        let index_name = index.name();
        match evaluate_index_candidate(family, index, &schema_info, predicate) {
            CandidateEvaluation::Eligible(score) if index_name == chosen_index_name => {
                chosen_score = score;
            }
            CandidateEvaluation::Eligible(score) => {
                alternatives.push(index_name);
                eligible_other_scores.push(score);
                rejected.push(AccessChoiceExplainRejected {
                    index_name,
                    reason: ranked_rejection_reason(family, score, chosen_score),
                });
            }
            CandidateEvaluation::Rejected(reason) => {
                rejected.push(AccessChoiceExplainRejected { index_name, reason });
            }
        }
    }

    // Phase 3: derive deterministic winner/rejection reason codes from the
    // one-pass candidate evaluation results above.
    AccessChoiceExplainSnapshot {
        chosen_label,
        chosen_reason: chosen_selection_reason(family, chosen_score, &eligible_other_scores),
        alternatives,
        rejected,
    }
}

fn chosen_access_shape_projection(
    access: &ExplainAccessPath,
) -> (AccessChoiceFamily, String, Option<&str>, CandidateScore) {
    match access {
        ExplainAccessPath::ByKey { .. } => (
            AccessChoiceFamily::NonIndex,
            "by_key".to_string(),
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
        ExplainAccessPath::ByKeys { .. } => (
            AccessChoiceFamily::NonIndex,
            "by_keys".to_string(),
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
        ExplainAccessPath::KeyRange { .. } => (
            AccessChoiceFamily::NonIndex,
            "key_range".to_string(),
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
            format!("index:{name}"),
            Some(*name),
            CandidateScore {
                prefix_len: *prefix_len,
                exact: *prefix_len == fields.len(),
            },
        ),
        ExplainAccessPath::IndexMultiLookup { name, fields, .. } => (
            AccessChoiceFamily::MultiLookup,
            format!("index:{name}"),
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
            format!("index:{name}"),
            Some(*name),
            CandidateScore {
                prefix_len: *prefix_len,
                exact: false,
            },
        ),
        ExplainAccessPath::FullScan => (
            AccessChoiceFamily::NonIndex,
            "full_scan".to_string(),
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
        ExplainAccessPath::Union(_) => (
            AccessChoiceFamily::NonIndex,
            "union".to_string(),
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
        ExplainAccessPath::Intersection(_) => (
            AccessChoiceFamily::NonIndex,
            "intersection".to_string(),
            None,
            CandidateScore {
                prefix_len: 0,
                exact: true,
            },
        ),
    }
}

fn evaluate_index_candidate(
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

fn evaluate_prefix_compare_candidate(
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
        IndexKeyItemsRef::Fields(fields) => {
            for &field in fields {
                let key_item = IndexKeyItem::Field(field);
                match match_eq_constraint_value_for_key_item(key_item, eq_constraints) {
                    Ok(Some(_)) => prefix_len = prefix_len.saturating_add(1),
                    Ok(None) => break,
                    Err(reason) => return Err(reason),
                }
            }
        }
        IndexKeyItemsRef::Items(items) => {
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

fn evaluate_multi_lookup_candidate(
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

fn evaluate_range_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> CandidateEvaluation {
    // Range-family candidate scoring remains fail-closed for expression-key indexes,
    // except one bounded starts-with casefold path that mirrors planner semantics.
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
    if !matches!(
        cmp.op,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte | CompareOp::StartsWith
    ) {
        return CandidateEvaluation::Rejected(
            AccessChoiceRejectedReason::OperatorNotRangeSupported,
        );
    }
    if cmp.op == CompareOp::StartsWith {
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonStrictCoercion);
        }
        let Some(leading_key_item) = leading_index_key_item(index) else {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
        };
        let literal_compatible =
            index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value());
        if starts_with_lookup_value_for_key_item(
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
                return CandidateEvaluation::Rejected(
                    AccessChoiceRejectedReason::LeadingFieldMismatch,
                );
            }
            if !literal_compatible {
                return CandidateEvaluation::Rejected(
                    AccessChoiceRejectedReason::LiteralIncompatible,
                );
            }

            return CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::StartsWithPrefixInvalid,
            );
        }
    } else {
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::NumericWiden
        ) {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::NonStrictCoercion);
        }
        if index.fields().first() != Some(&cmp.field.as_str()) {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch);
        }
        if !index_literal_matches_schema(schema, cmp.field.as_str(), cmp.value()) {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LiteralIncompatible);
        }
        if !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
            return CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotSupported);
        }
        if index.fields().len() != 1 {
            return CandidateEvaluation::Rejected(
                AccessChoiceRejectedReason::SingleFieldRangeRequired,
            );
        }
    }

    CandidateEvaluation::Eligible(CandidateScore {
        prefix_len: 0,
        exact: true,
    })
}

fn evaluate_range_and_candidate(
    index: &IndexModel,
    schema: &SchemaInfo,
    children: &[Predicate],
) -> CandidateEvaluation {
    // Phase 1: validate one range-eligible AND shape and gather compare clauses.
    let compares = match collect_range_and_compares(children) {
        Ok(compares) => compares,
        Err(reason) => return CandidateEvaluation::Rejected(reason),
    };

    // Phase 2: classify one candidate index constraint matrix from compare clauses.
    // Phase 2: score the candidate directly from one field-ordered constraint walk.
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

    // Evaluate each index field in order so range scoring never allocates a
    // temporary per-field matrix just to recover the final prefix shape.
    for field in index.fields() {
        let mut eq_value: Option<&Value> = None;
        let mut has_range_field = false;

        for cmp in compares {
            if cmp.field.as_str() != *field {
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
                    if has_range_field {
                        return Err(AccessChoiceRejectedReason::EqRangeConflict);
                    }
                    if let Some(existing) = eq_value
                        && existing != cmp.value()
                    {
                        return Err(AccessChoiceRejectedReason::ConflictingEqConstraints);
                    }
                    eq_value = Some(cmp.value());
                }
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                    if eq_value.is_some() {
                        return Err(AccessChoiceRejectedReason::EqRangeConflict);
                    }
                    has_range_field = true;
                }
                _ => return Err(AccessChoiceRejectedReason::OperatorNotRangeSupported),
            }
        }

        if !range_seen {
            if eq_value.is_some() {
                prefix_len = prefix_len.saturating_add(1);
                continue;
            }
            if has_range_field {
                range_seen = true;
                has_range = true;
                continue;
            }
            return Err(AccessChoiceRejectedReason::MissingContiguousPrefixOrRange);
        }

        if eq_value.is_some() || has_range_field {
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

fn chosen_selection_reason(
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

const fn ranked_rejection_reason(
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
