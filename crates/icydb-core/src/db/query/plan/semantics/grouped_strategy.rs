//! Module: db::query::plan::semantics::grouped_strategy
//! Responsibility: derive grouped-query execution semantics from grouped
//! projection, aggregate, and ordering contracts.
//! Does not own: grouped executor runtime implementation.
//! Boundary: keeps grouped planning semantics explicit before executor handoff.

use crate::db::{
    access::AccessPlan,
    query::plan::{
        AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupedPlanAggregateFamily, OrderSpec,
        expr::{
            GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
            classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
            grouped_top_k_order_term_requires_heap,
        },
    },
};

// Keep the raw grouped family selector internal so downstream code consumes the
// planner-owned `GroupedPlanStrategy` artifact instead of rebuilding behavior
// from a parallel hint surface.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedPlanFamily {
    Hash,
    Ordered,
    TopK,
}

///
/// GroupedPlanFallbackReason
///
/// Planner-authored grouped fallback taxonomy.
/// These reasons explain why grouped planning failed closed from the ordered
/// grouped family to the hash grouped family before route/runtime projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedPlanFallbackReason {
    DistinctGroupingNotAdmitted,
    ResidualFilterBlocksGroupedOrder,
    AggregateStreamingNotSupported,
    HavingBlocksGroupedOrder,
    GroupKeyOrderPrefixMismatch,
    GroupKeyOrderExpressionNotAdmissible,
    GroupKeyOrderUnavailable,
}

impl GroupedPlanFallbackReason {
    /// Return the stable planner-owned fallback reason code.
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::DistinctGroupingNotAdmitted => "distinct_grouping_not_admitted",
            Self::ResidualFilterBlocksGroupedOrder => "residual_filter_blocks_grouped_order",
            Self::AggregateStreamingNotSupported => "aggregate_streaming_not_supported",
            Self::HavingBlocksGroupedOrder => "having_blocks_grouped_order",
            Self::GroupKeyOrderPrefixMismatch => "group_key_order_prefix_mismatch",
            Self::GroupKeyOrderExpressionNotAdmissible => {
                "group_key_order_expression_not_admissible"
            }
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
pub(in crate::db) struct GroupedPlanStrategy {
    family: GroupedPlanFamily,
    aggregate_family: GroupedPlanAggregateFamily,
    fallback_reason: Option<GroupedPlanFallbackReason>,
}

impl GroupedPlanStrategy {
    /// Return the stable planner-owned grouped strategy code.
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self.family {
            GroupedPlanFamily::Hash => "hash_group",
            GroupedPlanFamily::Ordered => "ordered_group",
            GroupedPlanFamily::TopK => "top_k_group",
        }
    }

    /// Construct one hash-group planner strategy artifact with one planner-authored fallback reason.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn hash_group(reason: GroupedPlanFallbackReason) -> Self {
        Self::hash_group_with_aggregate_family(reason, GroupedPlanAggregateFamily::CountRowsOnly)
    }

    /// Construct one hash-group planner strategy artifact with one explicit grouped aggregate-family profile.
    #[must_use]
    pub(in crate::db) const fn hash_group_with_aggregate_family(
        reason: GroupedPlanFallbackReason,
        aggregate_family: GroupedPlanAggregateFamily,
    ) -> Self {
        Self {
            family: GroupedPlanFamily::Hash,
            aggregate_family,
            fallback_reason: Some(reason),
        }
    }

    /// Construct one ordered-group planner strategy artifact.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn ordered_group() -> Self {
        Self::ordered_group_with_aggregate_family(GroupedPlanAggregateFamily::CountRowsOnly)
    }

    /// Construct one ordered-group planner strategy artifact with one explicit grouped aggregate-family profile.
    #[must_use]
    pub(in crate::db) const fn ordered_group_with_aggregate_family(
        aggregate_family: GroupedPlanAggregateFamily,
    ) -> Self {
        Self {
            family: GroupedPlanFamily::Ordered,
            aggregate_family,
            fallback_reason: None,
        }
    }

    /// Construct one bounded grouped Top-K planner strategy artifact with one explicit grouped aggregate-family profile.
    #[must_use]
    pub(in crate::db) const fn top_k_group_with_aggregate_family(
        aggregate_family: GroupedPlanAggregateFamily,
    ) -> Self {
        Self {
            family: GroupedPlanFamily::TopK,
            aggregate_family,
            fallback_reason: None,
        }
    }

    /// Return whether the planner selected the ordered grouped family.
    #[must_use]
    pub(in crate::db) const fn is_ordered_group(self) -> bool {
        matches!(self.family, GroupedPlanFamily::Ordered)
    }

    /// Return whether the planner selected the bounded grouped Top-K family.
    #[must_use]
    pub(in crate::db) const fn is_top_k_group(self) -> bool {
        matches!(self.family, GroupedPlanFamily::TopK)
    }

    /// Return whether the planner admitted the ordered grouped family.
    #[must_use]
    pub(in crate::db) const fn ordered_group_admitted(self) -> bool {
        self.is_ordered_group()
    }

    /// Return the planner-owned grouped aggregate-family profile.
    #[must_use]
    pub(in crate::db) const fn aggregate_family(self) -> GroupedPlanAggregateFamily {
        self.aggregate_family
    }

    /// Return whether the planner admitted the dedicated grouped `COUNT(*)` family.
    #[must_use]
    pub(in crate::db) const fn is_single_count_rows(self) -> bool {
        matches!(
            self.aggregate_family,
            GroupedPlanAggregateFamily::CountRowsOnly
        )
    }

    /// Return the stable planner-authored fallback reason when ordered grouped execution was not admitted.
    #[must_use]
    pub(in crate::db) const fn fallback_reason(self) -> Option<GroupedPlanFallbackReason> {
        self.fallback_reason
    }
}

/// Project one planner-owned grouped strategy from one access-planned query.
#[must_use]
pub(in crate::db) fn grouped_plan_strategy(
    plan: &AccessPlannedQuery,
) -> Option<GroupedPlanStrategy> {
    // Phase 1: project the grouped ORDER BY lane early so aggregate-streaming
    // compatibility only gates the canonical ordered-group family. The bounded
    // Top-K family runs through grouped fold/finalize instead of ordered
    // grouped streaming, so widened aggregate-input expressions must not get
    // rejected here before the planner can reserve that lane.
    let grouped = plan.grouped_plan()?;
    let aggregate_family =
        GroupedPlanAggregateFamily::from_grouped_aggregates(grouped.group.aggregates.as_slice());
    let order_strategy_projection = grouped_order_strategy_projection(
        grouped.scalar.order.as_ref(),
        grouped.group.group_fields.as_slice(),
    );

    if grouped.scalar.distinct {
        return Some(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::DistinctGroupingNotAdmitted,
            aggregate_family,
        ));
    }

    // Reserve the bounded Top-K lane before checking residual-filter streaming
    // compatibility. Residual filters still block the older canonical
    // ordered-group path, but post-aggregate Top-K runs through grouped fold
    // and finalize rather than direct ordered streaming.
    if matches!(
        order_strategy_projection,
        GroupedOrderStrategyProjection::TopK
    ) {
        return Some(GroupedPlanStrategy::top_k_group_with_aggregate_family(
            aggregate_family,
        ));
    }

    if plan.has_residual_filter_expr() || plan.has_residual_filter_predicate() {
        return Some(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::ResidualFilterBlocksGroupedOrder,
            aggregate_family,
        ));
    }
    if !matches!(
        order_strategy_projection,
        GroupedOrderStrategyProjection::TopK
    ) && !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice())
    {
        return Some(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::AggregateStreamingNotSupported,
            aggregate_family,
        ));
    }
    if !crate::db::query::plan::semantics::group_having::grouped_having_streaming_compatible(
        grouped.having_expr.as_ref(),
    ) {
        return Some(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::HavingBlocksGroupedOrder,
            aggregate_family,
        ));
    }

    // Phase 2: require logical ORDER BY alignment and physical access-order proof for ordered grouping.
    match order_strategy_projection {
        GroupedOrderStrategyProjection::Canonical => {}
        GroupedOrderStrategyProjection::TopK => unreachable!(
            "bounded grouped Top-K lane should be reserved before streaming-only fallback checks"
        ),
        GroupedOrderStrategyProjection::HashFallback(reason) => {
            return Some(hash_group_fallback_strategy(reason, aggregate_family));
        }
    }
    if grouped_access_path_proves_group_order(grouped.group.group_fields.as_slice(), &plan.access) {
        return Some(GroupedPlanStrategy::ordered_group_with_aggregate_family(
            aggregate_family,
        ));
    }

    Some(hash_group_fallback_strategy(
        GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
        aggregate_family,
    ))
}

fn grouped_aggregates_streaming_compatible(aggregates: &[GroupAggregateSpec]) -> bool {
    aggregates
        .iter()
        .all(GroupAggregateSpec::streaming_compatible_v1)
}

// Lift the repeated hash-group fallback constructor so grouped strategy
// selection reads as planner policy gates instead of repeated artifact wiring.
const fn hash_group_fallback_strategy(
    reason: GroupedPlanFallbackReason,
    aggregate_family: GroupedPlanAggregateFamily,
) -> GroupedPlanStrategy {
    GroupedPlanStrategy::hash_group_with_aggregate_family(reason, aggregate_family)
}

///
/// GroupedOrderStrategyProjection
///
/// Planner-local grouped order-strategy projection result.
/// This keeps `0.87` canonical grouped-key proof and `0.88` Top-K reservation
/// under one owner so grouped strategy selection does not fork those decisions
/// through parallel helper trees.
///
enum GroupedOrderStrategyProjection {
    Canonical,
    TopK,
    HashFallback(GroupedPlanFallbackReason),
}

fn grouped_order_strategy_projection(
    order: Option<&OrderSpec>,
    group_fields: &[FieldSlot],
) -> GroupedOrderStrategyProjection {
    let Some(order) = order else {
        return GroupedOrderStrategyProjection::Canonical;
    };
    let grouped_field_names = group_fields
        .iter()
        .map(FieldSlot::field)
        .collect::<Vec<_>>();
    let top_k_required = order
        .fields
        .iter()
        .any(|term| grouped_top_k_order_term_requires_heap(term.expr()));

    if top_k_required {
        return grouped_top_k_strategy_projection(order, grouped_field_names.as_slice());
    }

    grouped_canonical_order_strategy_projection(order, group_fields)
}

fn grouped_canonical_order_strategy_projection(
    order: &OrderSpec,
    group_fields: &[FieldSlot],
) -> GroupedOrderStrategyProjection {
    if order.fields.len() < group_fields.len() {
        return GroupedOrderStrategyProjection::HashFallback(
            GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
        );
    }

    // Phase 1: walk the user-declared grouped ORDER BY list once and keep
    // canonical grouped-key proof separate from the broader grouped Top-K
    // expression family admitted by the `0.88` planner lane.
    for (index, term) in order.fields.iter().enumerate() {
        if index < group_fields.len() {
            match classify_grouped_order_term_for_field(term.expr(), group_fields[index].field()) {
                GroupedOrderTermAdmissibility::Preserves(_) => {}
                GroupedOrderTermAdmissibility::PrefixMismatch => {
                    return GroupedOrderStrategyProjection::HashFallback(
                        GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
                    );
                }
                GroupedOrderTermAdmissibility::UnsupportedExpression => {
                    return GroupedOrderStrategyProjection::HashFallback(
                        GroupedPlanFallbackReason::GroupKeyOrderExpressionNotAdmissible,
                    );
                }
            }
        }
    }

    GroupedOrderStrategyProjection::Canonical
}

fn grouped_top_k_strategy_projection(
    order: &OrderSpec,
    grouped_field_names: &[&str],
) -> GroupedOrderStrategyProjection {
    for term in &order.fields {
        match classify_grouped_top_k_order_term(term.expr(), grouped_field_names) {
            GroupedTopKOrderTermAdmissibility::Admissible => {}
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference => {
                return GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
                );
            }
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression => {
                return GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderExpressionNotAdmissible,
                );
            }
        }
    }

    GroupedOrderStrategyProjection::TopK
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
    let executable = access.executable_contract();
    let Some(path) = executable.as_path() else {
        return false;
    };
    let Some(details) = path
        .index_prefix_details()
        .or_else(|| path.index_range_details())
    else {
        return false;
    };
    let prefix_len = details.slot_arity();
    let mut cursor = 0usize;

    // Equality-bound prefix fields are fixed constants during traversal, so
    // grouped-order proof may skip them until the next declared grouped key.
    // Any gap beyond the equality prefix remains unfixed and therefore blocks
    // ordered grouping.
    for group_field in group_fields {
        while cursor < prefix_len
            && cursor < details.key_arity()
            && details.key_field_at(cursor) != Some(group_field.field())
        {
            cursor = cursor.saturating_add(1);
        }
        if cursor >= details.key_arity()
            || details.key_field_at(cursor) != Some(group_field.field())
        {
            return false;
        }
        cursor = cursor.saturating_add(1);
    }

    true
}
