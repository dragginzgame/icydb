//! Module: db::query::plan::semantics::grouped_strategy
//! Responsibility: derive grouped-query execution semantics from grouped
//! projection, aggregate, and ordering contracts.
//! Does not own: grouped executor runtime implementation.
//! Boundary: keeps grouped planning semantics explicit before executor handoff.

#[cfg(test)]
mod tests;

use crate::db::{
    QueryError,
    access::AccessPlan,
    query::plan::{
        AccessPlannedQuery, GroupAggregateSpec, GroupFieldSet, GroupPlan,
        GroupedPlanAggregateFamily, OrderSpec,
        expr::{
            GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
            try_classify_grouped_order_term_for_field, try_classify_grouped_top_k_order_term,
            try_grouped_top_k_order_term_requires_heap,
        },
    },
    query::preparation::PreparationWork,
};

use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

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
    GroupKeyOrderDirectionMismatch,
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
            Self::GroupKeyOrderDirectionMismatch => "group_key_order_direction_mismatch",
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
    plan.grouped_plan().map(|grouped| {
        match derive_grouped_plan_strategy(plan, grouped, &mut |_| {
            Ok::<_, std::convert::Infallible>(())
        }) {
            Ok(strategy) => strategy,
            Err(never) => match never {},
        }
    })
}

/// Project grouped diagnostic strategy under the current request allowance.
/// This never substitutes an identity result after diagnostic exhaustion.
pub(in crate::db) fn grouped_plan_strategy_for_explain(
    plan: &AccessPlannedQuery,
    grouped: &GroupPlan,
    work: &PreparationWork<'_>,
) -> Result<GroupedPlanStrategy, QueryError> {
    derive_grouped_plan_strategy(plan, grouped, &mut |steps| {
        work.charge(Resource::PredicateExpressionSteps, steps)
    })
}

// One borrowed evaluator owns selection for ordinary identity and diagnostics.
// The required observer runs before each visit/comparison and can stop the walk.
fn derive_grouped_plan_strategy<E>(
    plan: &AccessPlannedQuery,
    grouped: &GroupPlan,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedPlanStrategy, E> {
    observe(1)?;
    // Phase 1: project the grouped ORDER BY lane early so aggregate-streaming
    // compatibility only gates the canonical ordered-group family. The bounded
    // Top-K family runs through grouped fold/finalize instead of ordered
    // grouped streaming, so widened aggregate-input expressions must not get
    // rejected here before the planner can reserve that lane.
    let aggregate_family = GroupedPlanAggregateFamily::try_from_grouped_aggregates(
        grouped.group.aggregates.as_slice(),
        observe,
    )?;
    let order_strategy_projection = grouped_order_strategy_projection(
        grouped.scalar.order.as_ref(),
        &grouped.group.group_fields,
        observe,
    )?;

    if grouped.scalar.distinct {
        return Ok(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::DistinctGroupingNotAdmitted,
            aggregate_family,
        ));
    }

    // Reserve the bounded Top-K lane before checking residual-filter streaming
    // compatibility. Residual filters still block the direct ordered-group
    // path, but post-aggregate Top-K runs through grouped fold
    // and finalize rather than direct ordered streaming.
    if matches!(
        order_strategy_projection,
        GroupedOrderStrategyProjection::TopK
    ) {
        return Ok(GroupedPlanStrategy::top_k_group_with_aggregate_family(
            aggregate_family,
        ));
    }

    // Path-aware ordered admission reaches this point only after the route
    // planner proves the selected index stream complete for the query. Its
    // residual predicate filters rows without disturbing group-key order;
    // preserve the older direct-only fallback rule outside that proof.
    if plan.has_any_residual_filter() && grouped.group.group_fields.as_path_aware().is_none() {
        return Ok(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::ResidualFilterBlocksGroupedOrder,
            aggregate_family,
        ));
    }
    if !matches!(
        order_strategy_projection,
        GroupedOrderStrategyProjection::TopK
    ) && !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice(), observe)?
    {
        return Ok(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::AggregateStreamingNotSupported,
            aggregate_family,
        ));
    }
    if !crate::db::query::plan::semantics::group_having::grouped_having_streaming_compatible(
        grouped.having_expr.as_ref(),
        observe,
    )? {
        return Ok(hash_group_fallback_strategy(
            GroupedPlanFallbackReason::HavingBlocksGroupedOrder,
            aggregate_family,
        ));
    }

    // Phase 2: require logical ORDER BY alignment and physical access-order proof for ordered grouping.
    match order_strategy_projection {
        GroupedOrderStrategyProjection::Canonical => {}
        GroupedOrderStrategyProjection::TopK => {
            return Ok(GroupedPlanStrategy::top_k_group_with_aggregate_family(
                aggregate_family,
            ));
        }
        GroupedOrderStrategyProjection::HashFallback(reason) => {
            return Ok(hash_group_fallback_strategy(reason, aggregate_family));
        }
    }
    if grouped_access_path_proves_group_order(&grouped.group.group_fields, &plan.access, observe)? {
        return Ok(GroupedPlanStrategy::ordered_group_with_aggregate_family(
            aggregate_family,
        ));
    }

    Ok(hash_group_fallback_strategy(
        GroupedPlanFallbackReason::GroupKeyOrderUnavailable,
        aggregate_family,
    ))
}

fn grouped_aggregates_streaming_compatible<E>(
    aggregates: &[GroupAggregateSpec],
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<bool, E> {
    for aggregate in aggregates {
        observe(1)?;
        if !aggregate.streaming_compatible() {
            return Ok(false);
        }
    }
    Ok(true)
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
/// This keeps canonical grouped-key proof and Top-K reservation under one
/// owner so grouped strategy selection does not fork those decisions through
/// parallel helper trees.
///
enum GroupedOrderStrategyProjection {
    Canonical,
    TopK,
    HashFallback(GroupedPlanFallbackReason),
}

fn grouped_order_strategy_projection<E>(
    order: Option<&OrderSpec>,
    group_fields: &GroupFieldSet,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedOrderStrategyProjection, E> {
    let Some(order) = order else {
        return Ok(GroupedOrderStrategyProjection::Canonical);
    };
    for term in &order.fields {
        observe(1)?;
        if try_grouped_top_k_order_term_requires_heap(term.expr(), observe)? {
            return grouped_top_k_strategy_projection(order, group_fields, observe);
        }
    }

    grouped_canonical_order_strategy_projection(order, group_fields, observe)
}

fn grouped_canonical_order_strategy_projection<E>(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedOrderStrategyProjection, E> {
    observe(1)?;
    if order.fields.len() < group_fields.len() {
        return Ok(GroupedOrderStrategyProjection::HashFallback(
            GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
        ));
    }

    // Only the grouped-key prefix contributes to this proof. Heap detection
    // already inspected the complete ORDER BY list before choosing this lane.
    let mut canonical_direction = None;
    for (index, term) in order.fields.iter().take(group_fields.len()).enumerate() {
        observe(1)?;
        let direction = term.direction();
        if canonical_direction.is_some_and(|expected| expected != direction) {
            return Ok(GroupedOrderStrategyProjection::HashFallback(
                GroupedPlanFallbackReason::GroupKeyOrderDirectionMismatch,
            ));
        }
        canonical_direction.get_or_insert(direction);
        let Some(group_field) = group_fields.get(index) else {
            return Ok(GroupedOrderStrategyProjection::HashFallback(
                GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
            ));
        };
        match try_classify_grouped_order_term_for_field(term.expr(), group_field, observe)? {
            GroupedOrderTermAdmissibility::Preserves(_) => {}
            GroupedOrderTermAdmissibility::PrefixMismatch => {
                return Ok(GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
                ));
            }
            GroupedOrderTermAdmissibility::UnsupportedExpression => {
                return Ok(GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderExpressionNotAdmissible,
                ));
            }
        }
    }

    Ok(GroupedOrderStrategyProjection::Canonical)
}

fn grouped_top_k_strategy_projection<E>(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedOrderStrategyProjection, E> {
    for term in &order.fields {
        observe(1)?;
        match try_classify_grouped_top_k_order_term(term.expr(), group_fields, observe)? {
            GroupedTopKOrderTermAdmissibility::Admissible => {}
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference => {
                return Ok(GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderPrefixMismatch,
                ));
            }
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression => {
                return Ok(GroupedOrderStrategyProjection::HashFallback(
                    GroupedPlanFallbackReason::GroupKeyOrderExpressionNotAdmissible,
                ));
            }
        }
    }

    Ok(GroupedOrderStrategyProjection::TopK)
}

fn grouped_access_path_proves_group_order<K, E>(
    group_fields: &GroupFieldSet,
    access: &AccessPlan<K>,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1)?;
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
        return Ok(false);
    };
    let Some(details) = path
        .index_prefix_details()
        .or_else(|| path.index_range_details())
    else {
        return Ok(false);
    };
    let prefix_len = details.slot_arity();
    let mut cursor = 0usize;

    // Equality-bound prefix fields are fixed constants during traversal, so
    // grouped-order proof may skip them until the next declared grouped key.
    // Any gap beyond the equality prefix remains unfixed and therefore blocks
    // ordered grouping.
    for group_field in group_fields.iter() {
        observe(1)?;
        let comparison_steps = 1_u64.saturating_add(group_field.field().len() as u64);
        while cursor < prefix_len && cursor < details.key_arity() {
            observe(comparison_steps)?;
            if details.key_field_at(cursor) == Some(group_field.field()) {
                break;
            }
            cursor = cursor.saturating_add(1);
        }
        if cursor >= details.key_arity() {
            return Ok(false);
        }
        observe(comparison_steps)?;
        if details.key_field_at(cursor) != Some(group_field.field()) {
            return Ok(false);
        }
        cursor = cursor.saturating_add(1);
    }

    Ok(true)
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(GroupedPlanStrategy);
