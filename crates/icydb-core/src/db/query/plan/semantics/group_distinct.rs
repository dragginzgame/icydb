//! Module: query::plan::semantics::group_distinct
//! Responsibility: grouped DISTINCT admissibility and grouped aggregate policy semantics.
//! Does not own: grouped runtime enforcement or load-stage execution mechanics.
//! Boundary: provides planner-shared grouped DISTINCT policy reasoning contracts.

use crate::db::query::{
    builder::aggregate::{avg, count_by, sum},
    plan::{
        AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingExpr, GroupPlan, GroupSpec,
        GroupedExecutionConfig, validate::GroupPlanError,
    },
};
use crate::error::InternalError;

///
/// GroupDistinctPolicyReason
///
/// Canonical grouped DISTINCT policy rejection reasons shared across planner
/// and executor defensive boundaries.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupDistinctPolicyReason {
    DistinctHavingUnsupported,
    DistinctAdjacencyEligibilityRequired,
    GlobalDistinctHavingUnsupported,
    GlobalDistinctRequiresSingleAggregate,
    GlobalDistinctRequiresFieldTargetAggregate,
    GlobalDistinctRequiresDistinctAggregateTerminal,
    GlobalDistinctUnsupportedAggregateKind,
}

///
/// GroupDistinctAdmissibility
///
/// Shared grouped DISTINCT policy contract used to keep planner semantics and
/// executor defensive assertions aligned.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupDistinctAdmissibility {
    Allowed,
    Disallowed(GroupDistinctPolicyReason),
}

///
/// GlobalDistinctFieldAggregate
///
/// Canonical semantic projection of the supported global DISTINCT field-target
/// grouped aggregate shape.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GlobalDistinctFieldAggregate<'a> {
    kind: AggregateKind,
    target_field: &'a str,
}

impl<'a> GlobalDistinctFieldAggregate<'a> {
    /// Borrow grouped aggregate kind.
    #[must_use]
    pub(crate) const fn kind(self) -> AggregateKind {
        self.kind
    }

    /// Borrow grouped aggregate target field.
    #[must_use]
    pub(crate) const fn target_field(self) -> &'a str {
        self.target_field
    }
}

impl GroupDistinctPolicyReason {
    /// Construct one grouped DISTINCT HAVING-unsupported reason.
    #[must_use]
    pub(crate) const fn distinct_having_unsupported() -> Self {
        Self::DistinctHavingUnsupported
    }

    /// Construct one grouped DISTINCT adjacency-eligibility-required reason.
    #[must_use]
    pub(crate) const fn distinct_adjacency_eligibility_required() -> Self {
        Self::DistinctAdjacencyEligibilityRequired
    }

    /// Construct one global DISTINCT HAVING-unsupported reason.
    #[must_use]
    pub(crate) const fn global_distinct_having_unsupported() -> Self {
        Self::GlobalDistinctHavingUnsupported
    }

    /// Construct one global DISTINCT requires-single-aggregate reason.
    #[must_use]
    pub(crate) const fn global_distinct_requires_single_aggregate() -> Self {
        Self::GlobalDistinctRequiresSingleAggregate
    }

    /// Construct one global DISTINCT requires-field-target-aggregate reason.
    #[must_use]
    pub(crate) const fn global_distinct_requires_field_target_aggregate() -> Self {
        Self::GlobalDistinctRequiresFieldTargetAggregate
    }

    /// Construct one global DISTINCT requires-DISTINCT-terminal reason.
    #[must_use]
    pub(crate) const fn global_distinct_requires_distinct_aggregate_terminal() -> Self {
        Self::GlobalDistinctRequiresDistinctAggregateTerminal
    }

    /// Construct one global DISTINCT unsupported-aggregate-kind reason.
    #[must_use]
    pub(crate) const fn global_distinct_unsupported_aggregate_kind() -> Self {
        Self::GlobalDistinctUnsupportedAggregateKind
    }

    /// Return canonical executor invariant message text for this policy reason.
    #[must_use]
    pub(in crate::db) const fn invariant_message(self) -> &'static str {
        match self {
            Self::DistinctHavingUnsupported => "grouped HAVING with DISTINCT is unsupported",
            Self::DistinctAdjacencyEligibilityRequired => {
                "grouped DISTINCT requires ordered-group adjacency proof"
            }
            Self::GlobalDistinctHavingUnsupported => {
                "global DISTINCT grouped aggregate shape does not support HAVING"
            }
            Self::GlobalDistinctRequiresSingleAggregate => {
                "global DISTINCT grouped aggregate shape requires exactly one aggregate terminal"
            }
            Self::GlobalDistinctRequiresFieldTargetAggregate => {
                "global DISTINCT grouped aggregate shape requires field-target aggregate"
            }
            Self::GlobalDistinctRequiresDistinctAggregateTerminal => {
                "global DISTINCT grouped aggregate shape requires DISTINCT aggregate terminal"
            }
            Self::GlobalDistinctUnsupportedAggregateKind => {
                "global DISTINCT grouped aggregate shape supports COUNT/SUM/AVG only"
            }
        }
    }

    /// Convert this grouped DISTINCT policy reason into the executor-facing
    /// invariant used by global DISTINCT grouped route preparation.
    #[must_use]
    pub(in crate::db) fn into_global_distinct_prepare_internal_error(
        self,
        kind: AggregateKind,
    ) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "{}: found {kind:?}",
            self.invariant_message(),
        ))
    }

    /// Convert this grouped DISTINCT policy reason into the planner handoff
    /// invariant used when executor-only grouped DISTINCT shapes appear
    /// without prior planner validation.
    #[must_use]
    pub(in crate::db) fn into_planner_handoff_internal_error(self) -> InternalError {
        InternalError::planner_executor_invariant(format!(
            "planner grouped DISTINCT strategy handoff must be validated before executor handoff: {}",
            self.invariant_message()
        ))
    }

    /// Convert this grouped DISTINCT policy reason into the executor-facing
    /// invariant used when grouped route resolution reaches an executor-only
    /// grouped DISTINCT rejection path.
    #[must_use]
    pub(crate) fn into_grouped_route_internal_error(self) -> InternalError {
        InternalError::query_executor_invariant(self.invariant_message())
    }

    /// Project this grouped DISTINCT policy reason into a planner-domain
    /// grouped plan error.
    #[must_use]
    pub(in crate::db::query::plan) fn planner_group_plan_error(
        self,
        unsupported_kind: Option<AggregateKind>,
    ) -> GroupPlanError {
        match self {
            Self::DistinctHavingUnsupported => GroupPlanError::distinct_having_unsupported(),
            Self::DistinctAdjacencyEligibilityRequired => {
                GroupPlanError::distinct_adjacency_eligibility_required()
            }
            Self::GlobalDistinctHavingUnsupported
            | Self::GlobalDistinctRequiresSingleAggregate
            | Self::GlobalDistinctRequiresFieldTargetAggregate
            | Self::GlobalDistinctRequiresDistinctAggregateTerminal => {
                GroupPlanError::global_distinct_aggregate_shape_unsupported()
            }
            Self::GlobalDistinctUnsupportedAggregateKind => {
                let kind = unsupported_kind.map_or_else(
                    || "Unknown".to_string(),
                    |aggregate_kind| format!("{aggregate_kind:?}"),
                );

                GroupPlanError::distinct_aggregate_kind_unsupported(0, kind)
            }
        }
    }
}

/// Return grouped DISTINCT admissibility for scalar DISTINCT/HAVING policy flags.
#[must_use]
pub(crate) const fn grouped_distinct_admissibility(
    distinct: bool,
    has_having: bool,
) -> GroupDistinctAdmissibility {
    if distinct && has_having {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::distinct_having_unsupported(),
        );
    }
    if distinct {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::distinct_adjacency_eligibility_required(),
        );
    }

    GroupDistinctAdmissibility::Allowed
}

/// Return whether this grouped shape is a candidate for global DISTINCT
/// field-target aggregate handling.
#[must_use]
pub(crate) fn is_global_distinct_field_aggregate_candidate(
    group_fields: &[FieldSlot],
    aggregates: &[GroupAggregateSpec],
) -> bool {
    group_fields.is_empty()
        && !aggregates.is_empty()
        && aggregates
            .iter()
            .any(|aggregate| aggregate.target_field().is_some())
}

/// Return grouped DISTINCT admissibility for the global field-target aggregate
/// shape candidate.
#[must_use]
#[cfg(test)]
pub(crate) fn global_distinct_field_aggregate_admissibility(
    aggregates: &[GroupAggregateSpec],
    having_expr: Option<&GroupHavingExpr>,
) -> GroupDistinctAdmissibility {
    resolve_global_distinct_supported_aggregate(aggregates, having_expr)
        .map_or_else(GroupDistinctAdmissibility::Disallowed, |_| {
            GroupDistinctAdmissibility::Allowed
        })
}

/// Resolve one supported global DISTINCT field-target grouped aggregate shape.
pub(crate) fn resolve_global_distinct_field_aggregate<'a>(
    group_fields: &'a [FieldSlot],
    aggregates: &'a [GroupAggregateSpec],
    having_expr: Option<&'a GroupHavingExpr>,
) -> Result<Option<GlobalDistinctFieldAggregate<'a>>, GroupDistinctPolicyReason> {
    if !is_global_distinct_field_aggregate_candidate(group_fields, aggregates) {
        return Ok(None);
    }
    let aggregate = resolve_global_distinct_supported_aggregate(aggregates, having_expr)?;
    let target_field = aggregate
        .target_field()
        .ok_or(GroupDistinctPolicyReason::global_distinct_requires_field_target_aggregate())?;

    Ok(Some(GlobalDistinctFieldAggregate {
        kind: aggregate.kind(),
        target_field,
    }))
}

// Resolve the one supported global-DISTINCT aggregate terminal so the planner
// policy path and semantic projection path share the same shape contract.
fn resolve_global_distinct_supported_aggregate<'a>(
    aggregates: &'a [GroupAggregateSpec],
    having_expr: Option<&GroupHavingExpr>,
) -> Result<&'a GroupAggregateSpec, GroupDistinctPolicyReason> {
    if having_expr.is_some() {
        return Err(GroupDistinctPolicyReason::global_distinct_having_unsupported());
    }
    if aggregates.len() != 1 {
        return Err(GroupDistinctPolicyReason::global_distinct_requires_single_aggregate());
    }

    let aggregate = &aggregates[0];
    if aggregate.target_field().is_none() {
        return Err(GroupDistinctPolicyReason::global_distinct_requires_field_target_aggregate());
    }
    if !aggregate.distinct() {
        return Err(
            GroupDistinctPolicyReason::global_distinct_requires_distinct_aggregate_terminal(),
        );
    }
    if !aggregate
        .kind()
        .supports_global_distinct_without_group_keys()
    {
        return Err(GroupDistinctPolicyReason::global_distinct_unsupported_aggregate_kind());
    }

    Ok(aggregate)
}

/// Build one global DISTINCT grouped spec from canonical semantic aggregate shape.
pub(in crate::db) fn global_distinct_group_spec_for_semantic_aggregate(
    kind: AggregateKind,
    target_field: &str,
    execution: GroupedExecutionConfig,
) -> Result<GroupSpec, GroupDistinctPolicyReason> {
    let aggregate = match kind {
        AggregateKind::Count => count_by(target_field).distinct(),
        AggregateKind::Sum => sum(target_field).distinct(),
        AggregateKind::Avg => avg(target_field).distinct(),
        AggregateKind::Exists
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::First
        | AggregateKind::Last => {
            return Err(GroupDistinctPolicyReason::global_distinct_unsupported_aggregate_kind());
        }
    };

    Ok(GroupSpec::global_distinct_shape_from_aggregate_expr(
        &aggregate, execution,
    ))
}

impl GroupPlan {
    /// Return true when this grouped plan is the global DISTINCT aggregate shape.
    #[must_use]
    pub(in crate::db) fn is_global_distinct_aggregate_without_group_keys(&self) -> bool {
        resolve_global_distinct_field_aggregate(
            self.group.group_fields.as_slice(),
            self.group.aggregates.as_slice(),
            self.having_expr.as_ref(),
        )
        .ok()
        .flatten()
        .is_some()
    }
}
