//! Module: db::query::plan::access_choice::model
//! Responsibility: define access-choice scoring and rejection models used by
//! planner candidate evaluation.
//! Does not own: candidate enumeration or final execution-plan assembly.
//! Boundary: keeps planner access-choice data structures separate from evaluator logic.

use crate::db::query::plan::PlannedNonIndexAccessReason;

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
    pub(in crate::db) candidates: Vec<AccessChoiceCandidateExplainSummary>,
    pub(in crate::db) alternatives: Vec<&'static str>,
    pub(in crate::db) rejected: Vec<String>,
}

impl AccessChoiceExplainSnapshot {
    /// Construct one planner snapshot for composite non-index access paths
    /// whose concrete winner family is not distinguished more precisely.
    #[must_use]
    pub(in crate::db) const fn non_index_access() -> Self {
        Self {
            chosen_reason: AccessChoiceSelectedReason::NonIndexAccess,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }

    /// Construct one fail-closed snapshot for manually assembled index plans
    /// that never passed through planner-owned candidate projection.
    #[must_use]
    pub(in crate::db) const fn selected_index_not_projected() -> Self {
        Self {
            chosen_reason: AccessChoiceSelectedReason::SelectedIndexNotProjected,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }

    /// Construct one planner-owned snapshot from one frozen non-index winner
    /// reason already chosen during access planning.
    #[must_use]
    pub(in crate::db) const fn from_planned_non_index_reason(
        reason: PlannedNonIndexAccessReason,
    ) -> Self {
        let chosen_reason = match reason {
            PlannedNonIndexAccessReason::IntentKeyAccessOverride => {
                AccessChoiceSelectedReason::IntentKeyAccessOverride
            }
            PlannedNonIndexAccessReason::PlannerPrimaryKeyLookup => {
                AccessChoiceSelectedReason::PlannerPrimaryKeyLookup
            }
            PlannedNonIndexAccessReason::PlannerKeySetAccess => {
                AccessChoiceSelectedReason::PlannerKeySetAccess
            }
            PlannedNonIndexAccessReason::PlannerPrimaryKeyRange => {
                AccessChoiceSelectedReason::PlannerPrimaryKeyRange
            }
            PlannedNonIndexAccessReason::EmptyChildAccessPreferred => {
                AccessChoiceSelectedReason::EmptyChildAccessPreferred
            }
            PlannedNonIndexAccessReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
                AccessChoiceSelectedReason::ConflictingPrimaryKeyChildrenAccessPreferred
            }
            PlannedNonIndexAccessReason::SingletonPrimaryKeyChildAccessPreferred => {
                AccessChoiceSelectedReason::SingletonPrimaryKeyChildAccessPreferred
            }
            PlannedNonIndexAccessReason::RequiredOrderPrimaryKeyRangePreferred => {
                AccessChoiceSelectedReason::RequiredOrderPrimaryKeyRangePreferred
            }
            PlannedNonIndexAccessReason::LimitZeroWindow => {
                AccessChoiceSelectedReason::LimitZeroWindow
            }
            PlannedNonIndexAccessReason::ConstantFalsePredicate => {
                AccessChoiceSelectedReason::ConstantFalsePredicate
            }
            PlannedNonIndexAccessReason::PlannerFullScanFallback => {
                AccessChoiceSelectedReason::PlannerFullScanFallback
            }
            PlannedNonIndexAccessReason::PlannerCompositeNonIndex => {
                AccessChoiceSelectedReason::PlannerCompositeNonIndex
            }
        };

        Self {
            chosen_reason,
            candidates: Vec::new(),
            alternatives: Vec::new(),
            rejected: Vec::new(),
        }
    }
}

///
/// AccessChoiceResidualBurden
///
/// AccessChoiceResidualBurden classifies the bounded residual-work categories
/// surfaced by `0.106.1` route ranking and verbose explain output.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessChoiceResidualBurden {
    None,
    PredicateOnly,
    ScalarExpression,
}

impl AccessChoiceResidualBurden {
    #[must_use]
    pub(in crate::db) const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::PredicateOnly => "predicate_only",
            Self::ScalarExpression => "scalar_expression",
        }
    }
}

///
/// AccessChoiceCandidateExplainSummary
///
/// AccessChoiceCandidateExplainSummary carries one planner-owned eligible
/// access-candidate summary for verbose explain rendering.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessChoiceCandidateExplainSummary {
    pub(in crate::db) label: String,
    pub(in crate::db) exact: bool,
    pub(in crate::db) filtered: bool,
    pub(in crate::db) range_bound_count: usize,
    pub(in crate::db) order_compatible: bool,
    pub(in crate::db) residual_burden: AccessChoiceResidualBurden,
    pub(in crate::db) residual_predicate_terms: usize,
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
    FilteredPredicatePreferred,
    StrongerRangeBoundsPreferred,
    ResidualBurdenPreferred,
    OrderCompatiblePreferred,
    LexicographicTiebreak,
}

impl AccessChoiceRankingReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::ExactMatchPreferred => "exact_match_preferred",
            Self::FilteredPredicatePreferred => "filtered_predicate_preferred",
            Self::StrongerRangeBoundsPreferred => "stronger_range_bounds_preferred",
            Self::ResidualBurdenPreferred => "residual_burden_preferred",
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
    IntentKeyAccessOverride,
    PlannerPrimaryKeyLookup,
    PlannerKeySetAccess,
    PlannerPrimaryKeyRange,
    ByKeyAccess,
    ByKeysAccess,
    PrimaryKeyRangeAccess,
    EmptyChildAccessPreferred,
    ConflictingPrimaryKeyChildrenAccessPreferred,
    SingletonPrimaryKeyChildAccessPreferred,
    RequiredOrderPrimaryKeyRangePreferred,
    LimitZeroWindow,
    ConstantFalsePredicate,
    PlannerFullScanFallback,
    PlannerCompositeNonIndex,
    FullScanAccess,
    SelectedIndexNotProjected,
    SingleCandidate,
    BestPrefixLen,
    Ranked(AccessChoiceRankingReason),
}

impl AccessChoiceSelectedReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::NonIndexAccess => "non_index_access",
            Self::IntentKeyAccessOverride => "intent_key_access_override",
            Self::PlannerPrimaryKeyLookup => "planner_primary_key_lookup",
            Self::PlannerKeySetAccess => "planner_key_set_access",
            Self::PlannerPrimaryKeyRange => "planner_primary_key_range",
            Self::ByKeyAccess => "by_key_access",
            Self::ByKeysAccess => "by_keys_access",
            Self::PrimaryKeyRangeAccess => "primary_key_range_access",
            Self::EmptyChildAccessPreferred => "empty_child_access_preferred",
            Self::ConflictingPrimaryKeyChildrenAccessPreferred => {
                "conflicting_primary_key_children_access_preferred"
            }
            Self::SingletonPrimaryKeyChildAccessPreferred => {
                "singleton_primary_key_child_access_preferred"
            }
            Self::RequiredOrderPrimaryKeyRangePreferred => {
                "required_order_primary_key_range_preferred"
            }
            Self::LimitZeroWindow => "limit_zero_window",
            Self::ConstantFalsePredicate => "constant_false_predicate",
            Self::PlannerFullScanFallback => "planner_full_scan_fallback",
            Self::PlannerCompositeNonIndex => "planner_composite_non_index",
            Self::FullScanAccess => "full_scan_access",
            Self::SelectedIndexNotProjected => "selected_index_not_projected",
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
    pub(super) range_bound_count: u8,
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
