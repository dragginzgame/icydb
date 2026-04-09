//! Module: db::query::plan::semantics::grouped_strategy
//! Responsibility: module-local ownership and contracts for db::query::plan::semantics::grouped_strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::AccessPlan,
    query::plan::{AccessPlannedQuery, FieldSlot, GroupAggregateSpec, OrderSpec},
};

// Keep the raw grouped family selector internal so downstream code consumes the
// planner-owned `GroupedPlanStrategy` artifact instead of rebuilding behavior
// from a parallel hint surface.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedPlanFamily {
    HashGroup,
    OrderedGroup,
}

///
/// GroupedPlanFallbackReason
///
/// Planner-authored grouped fallback taxonomy.
/// These reasons explain why grouped planning failed closed from the ordered
/// grouped family to the hash grouped family before route/runtime projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedPlanFallbackReason {
    DistinctGroupingNotAdmitted,
    ResidualPredicateBlocksGroupedOrder,
    AggregateStreamingNotSupported,
    HavingBlocksGroupedOrder,
    GroupKeyOrderUnavailable,
}

impl GroupedPlanFallbackReason {
    /// Return the stable planner-owned fallback reason code.
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::DistinctGroupingNotAdmitted => "distinct_grouping_not_admitted",
            Self::ResidualPredicateBlocksGroupedOrder => "residual_predicate_blocks_grouped_order",
            Self::AggregateStreamingNotSupported => "aggregate_streaming_not_supported",
            Self::HavingBlocksGroupedOrder => "having_blocks_grouped_order",
            Self::GroupKeyOrderUnavailable => "group_key_order_unavailable",
        }
    }
}

///
/// GroupedPlanStrategy
///
/// Planner-owned grouped strategy artifact carried into executor and explain.
/// This artifact now carries the planner-selected grouped family plus the
/// stable planner fallback reason when ordered grouped execution is not
/// admitted. Runtime and explain must project from this structure instead of
/// re-deriving grouped admission semantics downstream.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupedPlanStrategy {
    family: GroupedPlanFamily,
    fallback_reason: Option<GroupedPlanFallbackReason>,
}

impl GroupedPlanStrategy {
    /// Construct one hash-group planner strategy artifact with one planner-authored fallback reason.
    #[must_use]
    pub(crate) const fn hash_group(reason: GroupedPlanFallbackReason) -> Self {
        Self {
            family: GroupedPlanFamily::HashGroup,
            fallback_reason: Some(reason),
        }
    }

    /// Construct one ordered-group planner strategy artifact.
    #[must_use]
    pub(crate) const fn ordered_group() -> Self {
        Self {
            family: GroupedPlanFamily::OrderedGroup,
            fallback_reason: None,
        }
    }

    /// Return whether the planner selected the ordered grouped family.
    #[must_use]
    pub(crate) const fn is_ordered_group(self) -> bool {
        matches!(self.family, GroupedPlanFamily::OrderedGroup)
    }

    /// Return whether the planner admitted the ordered grouped family.
    #[must_use]
    pub(crate) const fn streaming_admitted(self) -> bool {
        self.is_ordered_group()
    }

    /// Return the stable planner-authored fallback reason when ordered grouped execution was not admitted.
    #[must_use]
    pub(crate) const fn fallback_reason(self) -> Option<GroupedPlanFallbackReason> {
        self.fallback_reason
    }
}

/// Project one planner-owned grouped strategy from one access-planned query.
#[must_use]
pub(in crate::db) fn grouped_plan_strategy(
    plan: &AccessPlannedQuery,
) -> Option<GroupedPlanStrategy> {
    // Phase 1: reject planner-level grouped shapes that cannot preserve ordered grouping semantics.
    let grouped = plan.grouped_plan()?;
    if grouped.scalar.distinct {
        return Some(GroupedPlanStrategy::hash_group(
            GroupedPlanFallbackReason::DistinctGroupingNotAdmitted,
        ));
    }
    if grouped.scalar.predicate.is_some() {
        return Some(GroupedPlanStrategy::hash_group(
            GroupedPlanFallbackReason::ResidualPredicateBlocksGroupedOrder,
        ));
    }
    if !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice()) {
        return Some(GroupedPlanStrategy::hash_group(
            GroupedPlanFallbackReason::AggregateStreamingNotSupported,
        ));
    }
    if !crate::db::query::plan::semantics::group_having::grouped_having_streaming_compatible(
        grouped.having.as_ref(),
    ) {
        return Some(GroupedPlanStrategy::hash_group(
            GroupedPlanFallbackReason::HavingBlocksGroupedOrder,
        ));
    }

    // Phase 2: require logical ORDER BY alignment and physical access-order proof for ordered grouping.
    if !grouped_order_prefix_matches_group_fields(
        grouped.scalar.order.as_ref(),
        grouped.group.group_fields.as_slice(),
    ) {
        return Some(GroupedPlanStrategy::hash_group(
            GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
        ));
    }
    if grouped_access_path_proves_group_order(grouped.group.group_fields.as_slice(), &plan.access) {
        return Some(GroupedPlanStrategy::ordered_group());
    }

    Some(GroupedPlanStrategy::hash_group(
        GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
    ))
}

fn grouped_aggregates_streaming_compatible(aggregates: &[GroupAggregateSpec]) -> bool {
    aggregates
        .iter()
        .all(GroupAggregateSpec::streaming_compatible_v1)
}

fn grouped_order_prefix_matches_group_fields(
    order: Option<&OrderSpec>,
    group_fields: &[FieldSlot],
) -> bool {
    let Some(order) = order else {
        return true;
    };
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

fn grouped_access_path_proves_group_order<K>(
    group_fields: &[FieldSlot],
    access: &AccessPlan<K>,
) -> bool {
    // Derive grouped-order evidence from the normalized executable access contract so
    // planner strategy hints do not branch on raw AccessPath variants directly.
    let executable = access.resolve_strategy();
    let Some(path) = executable.as_path() else {
        return false;
    };
    let Some((index, prefix_len)) = path.index_prefix_details() else {
        return false;
    };
    let required_end = prefix_len.saturating_add(group_fields.len());
    if required_end > index.fields().len() {
        return false;
    }

    group_fields
        .iter()
        .zip(index.fields()[prefix_len..required_end].iter())
        .all(|(group_field, index_field)| group_field.field() == *index_field)
}
