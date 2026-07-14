//! Module: executor::planning::route::contracts::execution
//! Responsibility: executor route execution-shape contracts and observability payloads.
//! Does not own: route planning decisions or planner capability derivation policy.
//! Boundary: exposes stable execution-shape DTOs consumed by route, load, and runtime code.

mod plan;

use crate::db::{direction::Direction, query::plan::GroupedPlanStrategy};

pub(in crate::db::executor) use plan::ExecutionRoutePlan;

///
/// LoadOrderRouteMode
///
/// Canonical route-owned load ordering mode for one executable load shape.
/// `DirectStreaming` means the access path already preserves the final order
/// and the route may derive bounded streaming hints directly.
/// `MaterializedBoundary` means access order is still meaningful, but the
/// shared materialized boundary must own the final windowing/sort behavior.
/// `MaterializedFallback` means the route must fail closed and materialize
/// without ordered streaming assumptions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum LoadOrderRouteMode {
    DirectStreaming,
    MaterializedBoundary,
    MaterializedFallback,
}

impl LoadOrderRouteMode {
    /// Return the stable observability code for this load-order route mode.
    #[must_use]
    pub(in crate::db::executor) const fn code(self) -> &'static str {
        match self {
            Self::DirectStreaming => "direct_streaming",
            Self::MaterializedBoundary => "materialized_boundary",
            Self::MaterializedFallback => "materialized_fallback",
        }
    }

    /// Return whether this mode supports direct streaming load execution.
    #[must_use]
    pub(in crate::db::executor) const fn allows_streaming_load(self) -> bool {
        matches!(self, Self::DirectStreaming)
    }

    /// Return whether this mode preserves ordered grouped projection inputs.
    #[must_use]
    pub(in crate::db::executor) const fn allows_ordered_group_projection(self) -> bool {
        matches!(self, Self::DirectStreaming)
    }

    /// Return whether this mode supports top-N seek execution.
    #[must_use]
    pub(in crate::db::executor) const fn allows_top_n_seek(self) -> bool {
        matches!(self, Self::DirectStreaming)
    }
}

///
/// GroupedExecutionModeContext
///
/// Route-owned capability bundle for projecting planner-owned grouped strategy
/// into one canonical grouped execution mode. This keeps grouped execution-mode
/// selection on one explicit route input bundle instead of loose booleans.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedExecutionModeContext {
    direction: Direction,
    desc_physical_reverse_supported: bool,
    ordered_group_projection_safe: bool,
}

impl GroupedExecutionModeContext {
    /// Build grouped execution-mode context from route-derived facts.
    #[must_use]
    pub(in crate::db::executor) const fn from_route_inputs(
        direction: Direction,
        desc_physical_reverse_supported: bool,
        ordered_group_projection_safe: bool,
    ) -> Self {
        Self {
            direction,
            desc_physical_reverse_supported,
            ordered_group_projection_safe,
        }
    }
}

///
/// LoadOrderRouteReason
///
/// Canonical route-owned explanation for why one ordered load route stayed
/// direct, required the shared materialized boundary, or failed closed to the
/// canonical materialized fallback path.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum LoadOrderRouteReason {
    None,
    RequiresMaterializedSort,
    ResidualFilterBlocksDirectStreaming,
    DistinctRequiresMaterialization,
    DescendingNonUniqueSecondaryPrefixNotAdmitted,
}

impl LoadOrderRouteReason {
    /// Return the stable observability code for this load-order route reason.
    #[must_use]
    pub(in crate::db::executor) const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RequiresMaterializedSort => "requires_materialized_sort",
            Self::ResidualFilterBlocksDirectStreaming => "residual_filter_blocks_direct_streaming",
            Self::DistinctRequiresMaterialization => "distinct_requires_materialization",
            Self::DescendingNonUniqueSecondaryPrefixNotAdmitted => {
                "descending_non_unique_secondary_prefix_not_admitted"
            }
        }
    }
}

///
/// LoadOrderRouteDecision
///
/// Route-owned paired load-order decision payload.
/// This keeps the chosen load-order mode and its explanation code under
/// one owner so route capability derivation and observability cannot drift.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct LoadOrderRouteDecision {
    mode: LoadOrderRouteMode,
    reason: LoadOrderRouteReason,
}

impl LoadOrderRouteDecision {
    /// Build a direct-streaming ordered-load route decision.
    #[must_use]
    pub(in crate::db::executor) const fn direct_streaming() -> Self {
        Self {
            mode: LoadOrderRouteMode::DirectStreaming,
            reason: LoadOrderRouteReason::None,
        }
    }

    /// Build a materialized-boundary ordered-load route decision.
    #[must_use]
    pub(in crate::db::executor) const fn materialized_boundary(
        reason: LoadOrderRouteReason,
    ) -> Self {
        Self {
            mode: LoadOrderRouteMode::MaterializedBoundary,
            reason,
        }
    }

    /// Build a materialized-fallback ordered-load route decision.
    #[must_use]
    pub(in crate::db::executor) const fn materialized_fallback(
        reason: LoadOrderRouteReason,
    ) -> Self {
        Self {
            mode: LoadOrderRouteMode::MaterializedFallback,
            reason,
        }
    }

    /// Return the selected ordered-load route mode.
    #[must_use]
    pub(in crate::db::executor) const fn mode(self) -> LoadOrderRouteMode {
        self.mode
    }

    /// Return the reason attached to the ordered-load route mode.
    #[must_use]
    pub(in crate::db::executor) const fn reason(self) -> LoadOrderRouteReason {
        self.reason
    }
}

///
/// RouteExecutionMode
///
/// Canonical route-level execution shape selected by the routing gate.
/// Keeps streaming-vs-materialized decisions explicit and testable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteExecutionMode {
    Streaming,
    Materialized,
}

///
/// GroupedExecutionMode
///
/// Canonical grouped execution mode label selected by route planning.
/// Variants are route-owned and runtime-truthful: they describe only the
/// grouped execution mode that survived planner semantics plus route
/// capability gating.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedExecutionMode {
    HashMaterialized,
    OrderedMaterialized,
}

impl GroupedExecutionMode {
    /// Project planner grouped strategy plus route facts into a grouped execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn from_planner_strategy(
        plan_strategy: GroupedPlanStrategy,
        projection: GroupedExecutionModeContext,
    ) -> Self {
        if plan_strategy.is_top_k_group() {
            return Self::HashMaterialized;
        }

        let direction_compatible = !matches!(projection.direction, Direction::Desc)
            || projection.desc_physical_reverse_supported;
        let ordered_route_eligible = plan_strategy.ordered_group_admitted()
            && direction_compatible
            && projection.ordered_group_projection_safe;

        if ordered_route_eligible {
            Self::OrderedMaterialized
        } else {
            Self::HashMaterialized
        }
    }

    /// Return the stable observability code for this grouped execution mode.
    #[must_use]
    pub(in crate::db::executor) const fn code(self) -> &'static str {
        match self {
            Self::HashMaterialized => "hash_materialized",
            Self::OrderedMaterialized => "ordered_materialized",
        }
    }
}

///
/// ScanHintPlan
///
/// Canonical scan-hint payload produced by route planning.
/// Keeps bounded fetch/budget hints under one boundary.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct ScanHintPlan {
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) load_scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) index_prefix_child_expansion: Option<IndexPrefixChildExpansionHint>,
}

///
/// IndexPrefixChildExpansionBudget
///
/// Route-owned bounded selectivity budget for sparse prefix-family child
/// expansion. The default floor avoids under-expanding small first pages, while
/// the hard ceiling prevents a bounded page from turning a large `IN` route into
/// an unbounded child-stream fanout.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct IndexPrefixChildExpansionBudget {
    max_child_prefixes: usize,
}

impl IndexPrefixChildExpansionBudget {
    const DEFAULT_PREFIXES: usize = 32;
    const MAX_PREFIXES: usize = 128;

    #[must_use]
    pub(in crate::db::executor) const fn from_fetch_limit(fetch_limit: Option<usize>) -> Self {
        let Some(fetch_limit) = fetch_limit else {
            return Self {
                max_child_prefixes: Self::DEFAULT_PREFIXES,
            };
        };
        if fetch_limit < Self::DEFAULT_PREFIXES {
            return Self {
                max_child_prefixes: Self::DEFAULT_PREFIXES,
            };
        }
        if fetch_limit > Self::MAX_PREFIXES {
            return Self {
                max_child_prefixes: Self::MAX_PREFIXES,
            };
        }

        Self {
            max_child_prefixes: fetch_limit,
        }
    }

    #[must_use]
    const fn max_child_prefixes(self) -> usize {
        self.max_child_prefixes
    }
}

///
/// IndexPrefixChildExpansionHint
///
/// Route-owned contract for sparse prefix-family execution. It says a
/// multi-lookup prefix can be expanded by metadata into exact child prefixes
/// whose remaining suffix is the primary-key order suffix.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct IndexPrefixChildExpansionHint {
    target_prefix_len: usize,
    budget: IndexPrefixChildExpansionBudget,
}

impl IndexPrefixChildExpansionHint {
    #[must_use]
    pub(in crate::db::executor) const fn new(
        target_prefix_len: usize,
        budget: IndexPrefixChildExpansionBudget,
    ) -> Self {
        Self {
            target_prefix_len,
            budget,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn target_prefix_len(self) -> usize {
        self.target_prefix_len
    }

    #[must_use]
    pub(in crate::db::executor) const fn max_child_prefixes(self) -> usize {
        self.budget.max_child_prefixes()
    }
}

///
/// IndexRangeLimitSpec
///
/// Canonical route decision payload for index-range limit pushdown.
/// Encodes the bounded fetch size after all eligibility gates pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct IndexRangeLimitSpec {
    pub(in crate::db::executor) fetch: usize,
}

///
/// AggregateSeekSpec
///
/// Canonical route contract for aggregate index-edge seek execution.
/// Encodes seek edge (`first`/`last`) and bounded fetch budget in one payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateSeekSpec {
    First { fetch: usize },
    Last { fetch: usize },
}

impl AggregateSeekSpec {
    /// Return the bounded fetch size for this aggregate seek.
    #[must_use]
    pub(in crate::db::executor) const fn fetch(self) -> usize {
        match self {
            Self::First { fetch } | Self::Last { fetch } => fetch,
        }
    }
}

///
/// TopNSeekSpec
///
/// Canonical route contract for ordered load `LIMIT` seek windows.
/// Encodes the bounded fetch size for one top-N access pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct TopNSeekSpec {
    fetch: usize,
}

impl TopNSeekSpec {
    /// Build one top-N seek spec from a bounded fetch size.
    #[must_use]
    pub(in crate::db::executor::planning::route) const fn new(fetch: usize) -> Self {
        Self { fetch }
    }

    /// Return the bounded fetch size for this top-N seek.
    #[must_use]
    pub(in crate::db::executor) const fn fetch(self) -> usize {
        self.fetch
    }
}
