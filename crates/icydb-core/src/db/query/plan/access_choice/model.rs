//! Module: db::query::plan::access_choice::model
//! Responsibility: module-local ownership and contracts for db::query::plan::access_choice::model.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

pub(super) use crate::db::query::plan::planner::AccessCandidateScore as CandidateScore;

///
/// AccessChoiceExplainSnapshot
///
/// Planner-owned access-choice explain projection consumed by executor
/// descriptor assembly without re-deriving planner ranking policies.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessChoiceExplainSnapshot {
    pub(in crate::db) chosen_reason: AccessChoiceSelectedReason,
    pub(in crate::db) alternatives: Vec<&'static str>,
    pub(in crate::db) rejected: Vec<String>,
}

impl AccessChoiceExplainSnapshot {
    /// Construct one planner snapshot for non-index or composite access paths.
    #[must_use]
    pub(in crate::db) const fn non_index_access() -> Self {
        Self {
            chosen_reason: AccessChoiceSelectedReason::NonIndexAccess,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }

    /// Construct one fail-closed snapshot for manually assembled index plans
    /// that never passed through planner-owned candidate projection.
    #[must_use]
    pub(in crate::db) const fn selected_index_unavailable() -> Self {
        Self {
            chosen_reason: AccessChoiceSelectedReason::SelectedIndexUnavailable,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }

    /// Construct one fail-closed snapshot when schema projection was not available.
    #[must_use]
    pub(in crate::db) const fn schema_unavailable() -> Self {
        Self {
            chosen_reason: AccessChoiceSelectedReason::SchemaUnavailable,
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }
}

///
/// AccessChoiceRankingReason
///
/// Shared ranking reason taxonomy for planner tie-break decisions.
/// Selection and rejection surfaces carry polarity separately so explain
/// output can reuse the same canonical ranking reason codes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessChoiceRankingReason {
    ExactMatchPreferred,
    OrderCompatiblePreferred,
    LexicographicTiebreak,
}

impl AccessChoiceRankingReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::ExactMatchPreferred => "exact_match_preferred",
            Self::OrderCompatiblePreferred => "order_compatible_preferred",
            Self::LexicographicTiebreak => "lexicographic_tiebreak",
        }
    }
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
    Ranked(AccessChoiceRankingReason),
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
            Self::Ranked(reason) => reason.code(),
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
    Ranked(AccessChoiceRankingReason),
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
            Self::Ranked(reason) => reason.code(),
        }
    }

    #[must_use]
    pub(in crate::db) fn render_for_index(self, index_name: &'static str) -> String {
        let reason = self.code();
        let mut out = String::with_capacity("index:".len() + index_name.len() + 1 + reason.len());
        out.push_str("index:");
        out.push_str(index_name);
        out.push('=');
        out.push_str(reason);
        out
    }
}

///
/// AccessChoiceFamily
///
/// AccessChoiceFamily groups the planner-visible access candidate shapes that
/// share one explain ranking policy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AccessChoiceFamily {
    NonIndex,
    Prefix,
    MultiLookup,
    Range,
}

///
/// RangeCompareKind
///
/// RangeCompareKind classifies the single-clause range forms that map to the
/// planner's range-family explain projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RangeCompareKind {
    StartsWith,
    Ordered,
}

///
/// RangeFieldConstraint
///
/// RangeFieldConstraint accumulates the normalized equality-vs-range state for
/// one index field during ordered range scoring.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RangeFieldConstraint {
    pub(super) eq_value: Option<crate::value::Value>,
    pub(super) has_range: bool,
}

///
/// CandidateEvaluation
///
/// CandidateEvaluation is the fail-closed evaluator result for one index
/// candidate under the chosen access family.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CandidateEvaluation {
    Eligible(CandidateScore),
    Rejected(AccessChoiceRejectedReason),
}
