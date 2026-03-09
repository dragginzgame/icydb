//! Module: query::plan::semantics::group_distinct
//! Responsibility: grouped DISTINCT admissibility and grouped aggregate policy semantics.
//! Does not own: grouped runtime enforcement or load-stage execution mechanics.
//! Boundary: provides planner-shared grouped DISTINCT policy reasoning contracts.

use crate::db::query::{
    builder::aggregate::{count_by, sum},
    plan::{
        AggregateKind, FieldSlot, GroupAggregateSpec, GroupHavingSpec, GroupPlan, GroupSpec,
        GroupedExecutionConfig,
    },
};

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
    /// Return canonical executor invariant message text for this policy reason.
    #[must_use]
    pub(in crate::db) const fn invariant_message(self) -> &'static str {
        match self {
            Self::DistinctHavingUnsupported => {
                "grouped HAVING with DISTINCT is not supported in this release"
            }
            Self::DistinctAdjacencyEligibilityRequired => {
                "grouped DISTINCT requires adjacency-based ordered-group eligibility proof in this release"
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
                "global DISTINCT grouped aggregate shape supports COUNT/SUM only"
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
            GroupDistinctPolicyReason::DistinctHavingUnsupported,
        );
    }
    if distinct {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::DistinctAdjacencyEligibilityRequired,
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
pub(crate) fn global_distinct_field_aggregate_admissibility(
    aggregates: &[GroupAggregateSpec],
    having: Option<&GroupHavingSpec>,
) -> GroupDistinctAdmissibility {
    if having.is_some() {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctHavingUnsupported,
        );
    }
    if aggregates.len() != 1 {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctRequiresSingleAggregate,
        );
    }

    let aggregate = &aggregates[0];
    if aggregate.target_field().is_none() {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate,
        );
    }
    if !aggregate.distinct() {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctRequiresDistinctAggregateTerminal,
        );
    }
    if !aggregate
        .kind()
        .supports_global_distinct_without_group_keys()
    {
        return GroupDistinctAdmissibility::Disallowed(
            GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind,
        );
    }

    GroupDistinctAdmissibility::Allowed
}

/// Resolve one supported global DISTINCT field-target grouped aggregate shape.
pub(crate) fn resolve_global_distinct_field_aggregate<'a>(
    group_fields: &'a [FieldSlot],
    aggregates: &'a [GroupAggregateSpec],
    having: Option<&'a GroupHavingSpec>,
) -> Result<Option<GlobalDistinctFieldAggregate<'a>>, GroupDistinctPolicyReason> {
    if !is_global_distinct_field_aggregate_candidate(group_fields, aggregates) {
        return Ok(None);
    }
    match global_distinct_field_aggregate_admissibility(aggregates, having) {
        GroupDistinctAdmissibility::Allowed => {}
        GroupDistinctAdmissibility::Disallowed(reason) => return Err(reason),
    }
    let aggregate = &aggregates[0];
    let target_field = aggregate
        .target_field()
        .ok_or(GroupDistinctPolicyReason::GlobalDistinctRequiresFieldTargetAggregate)?;

    Ok(Some(GlobalDistinctFieldAggregate {
        kind: aggregate.kind(),
        target_field,
    }))
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
        AggregateKind::Exists
        | AggregateKind::Min
        | AggregateKind::Max
        | AggregateKind::First
        | AggregateKind::Last => {
            return Err(GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind);
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
            self.having.as_ref(),
        )
        .ok()
        .flatten()
        .is_some()
    }
}
