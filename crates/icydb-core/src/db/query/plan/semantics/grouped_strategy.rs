//! Module: db::query::plan::semantics::grouped_strategy
//! Responsibility: module-local ownership and contracts for db::query::plan::semantics::grouped_strategy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::AccessPlan,
    query::plan::{AccessPlannedQuery, AggregateKind, FieldSlot, GroupAggregateSpec, OrderSpec},
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
/// GroupedPlanAggregateFamily
///
/// Planner-owned grouped aggregate-family profile.
/// This is intentionally coarse: it captures which grouped aggregate family the
/// planner admitted so runtime can select grouped execution paths without
/// rebuilding family policy from raw aggregate expressions again.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedPlanAggregateFamily {
    CountRowsOnly,
    FieldTargetRows,
    StorageKeyTerminals,
    Mixed,
}

impl GroupedPlanAggregateFamily {
    /// Return the stable planner-owned aggregate-family code.
    #[must_use]
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::CountRowsOnly => "count_rows_only",
            Self::FieldTargetRows => "field_target_rows",
            Self::StorageKeyTerminals => "storage_key_terminals",
            Self::Mixed => "mixed",
        }
    }
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
    aggregate_family: GroupedPlanAggregateFamily,
    fallback_reason: Option<GroupedPlanFallbackReason>,
}

impl GroupedPlanStrategy {
    /// Construct one hash-group planner strategy artifact with one planner-authored fallback reason.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn hash_group(reason: GroupedPlanFallbackReason) -> Self {
        Self::hash_group_with_aggregate_family(reason, GroupedPlanAggregateFamily::CountRowsOnly)
    }

    /// Construct one hash-group planner strategy artifact with one explicit grouped aggregate-family profile.
    #[must_use]
    pub(crate) const fn hash_group_with_aggregate_family(
        reason: GroupedPlanFallbackReason,
        aggregate_family: GroupedPlanAggregateFamily,
    ) -> Self {
        Self {
            family: GroupedPlanFamily::HashGroup,
            aggregate_family,
            fallback_reason: Some(reason),
        }
    }

    /// Construct one ordered-group planner strategy artifact.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn ordered_group() -> Self {
        Self::ordered_group_with_aggregate_family(GroupedPlanAggregateFamily::CountRowsOnly)
    }

    /// Construct one ordered-group planner strategy artifact with one explicit grouped aggregate-family profile.
    #[must_use]
    pub(crate) const fn ordered_group_with_aggregate_family(
        aggregate_family: GroupedPlanAggregateFamily,
    ) -> Self {
        Self {
            family: GroupedPlanFamily::OrderedGroup,
            aggregate_family,
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

    /// Return the planner-owned grouped aggregate-family profile.
    #[must_use]
    pub(crate) const fn aggregate_family(self) -> GroupedPlanAggregateFamily {
        self.aggregate_family
    }

    /// Return whether the planner admitted the dedicated grouped `COUNT(*)` family.
    #[must_use]
    pub(crate) const fn is_single_count_rows(self) -> bool {
        matches!(
            self.aggregate_family,
            GroupedPlanAggregateFamily::CountRowsOnly
        )
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
    let aggregate_family = grouped_plan_aggregate_family(grouped.group.aggregates.as_slice());
    if grouped.scalar.distinct {
        return Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::DistinctGroupingNotAdmitted,
            aggregate_family,
        ));
    }
    if plan.has_residual_predicate() {
        return Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::ResidualPredicateBlocksGroupedOrder,
            aggregate_family,
        ));
    }
    if !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice()) {
        return Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::AggregateStreamingNotSupported,
            aggregate_family,
        ));
    }
    if !crate::db::query::plan::semantics::group_having::grouped_having_streaming_compatible(
        grouped.having.as_ref(),
    ) {
        return Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::HavingBlocksGroupedOrder,
            aggregate_family,
        ));
    }

    // Phase 2: require logical ORDER BY alignment and physical access-order proof for ordered grouping.
    if !grouped_order_prefix_matches_group_fields(
        grouped.scalar.order.as_ref(),
        grouped.group.group_fields.as_slice(),
    ) {
        return Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
            GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
            aggregate_family,
        ));
    }
    if grouped_access_path_proves_group_order(grouped.group.group_fields.as_slice(), &plan.access) {
        return Some(GroupedPlanStrategy::ordered_group_with_aggregate_family(
            aggregate_family,
        ));
    }

    Some(GroupedPlanStrategy::hash_group_with_aggregate_family(
        GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
        aggregate_family,
    ))
}

fn grouped_plan_aggregate_family(aggregates: &[GroupAggregateSpec]) -> GroupedPlanAggregateFamily {
    if matches!(
        aggregates,
        [GroupAggregateSpec {
            kind: AggregateKind::Count,
            target_field: None,
            distinct: false,
        }]
    ) {
        return GroupedPlanAggregateFamily::CountRowsOnly;
    }

    if aggregates.iter().all(|aggregate| {
        aggregate.target_field().is_some()
            && matches!(
                aggregate.kind(),
                AggregateKind::Count | AggregateKind::Sum | AggregateKind::Avg
            )
    }) {
        return GroupedPlanAggregateFamily::FieldTargetRows;
    }

    if aggregates.iter().all(|aggregate| {
        aggregate.target_field().is_none()
            && matches!(
                aggregate.kind(),
                AggregateKind::Exists
                    | AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::First
                    | AggregateKind::Last
            )
    }) {
        return GroupedPlanAggregateFamily::StorageKeyTerminals;
    }

    GroupedPlanAggregateFamily::Mixed
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
    //
    // Both index-prefix and index-range shapes can preserve grouped key order:
    // - `IndexPrefix` proves one leading equality prefix plus ordered suffix traversal.
    // - `IndexRange` proves one ordered range traversal after its equality prefix.
    //
    // Grouped planning only needs the stable `(index, prefix_len)` contract here,
    // not the raw range bounds themselves.
    let executable = access.resolve_strategy();
    let Some(path) = executable.as_path() else {
        return false;
    };
    let Some((index, prefix_len)) = path
        .index_prefix_details()
        .or_else(|| path.index_range_details())
    else {
        return false;
    };
    let index_fields = index.fields();
    let mut cursor = 0usize;

    // Equality-bound prefix fields are fixed constants during traversal, so
    // grouped-order proof may skip them until the next declared grouped key.
    // Any gap beyond the equality prefix remains unfixed and therefore blocks
    // ordered grouping.
    for group_field in group_fields {
        while cursor < prefix_len
            && cursor < index_fields.len()
            && index_fields[cursor] != group_field.field()
        {
            cursor = cursor.saturating_add(1);
        }
        if cursor >= index_fields.len() || index_fields[cursor] != group_field.field() {
            return false;
        }
        cursor = cursor.saturating_add(1);
    }

    true
}
