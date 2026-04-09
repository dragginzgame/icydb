//! Module: executor::route::contracts::execution
//! Responsibility: executor route execution-shape contracts and observability payloads.
//! Does not own: route planning decisions or planner capability derivation policy.
//! Boundary: exposes stable execution-shape DTOs consumed by route/load/runtime seams.

mod observability;
mod plan;

use crate::db::executor::route::contracts::shape::RouteShapeKind;

pub(in crate::db::executor) use observability::{
    GroupedRouteDecisionOutcome, GroupedRouteObservability, GroupedRouteRejectionReason,
};
pub(in crate::db::executor) use plan::ExecutionRoutePlan;

///
/// LoadOrderRouteContract
///
/// Canonical route-owned load ordering contract for one executable load shape.
/// `DirectStreaming` means the access path already preserves the final order
/// contract and the route may derive bounded streaming hints directly.
/// `MaterializedBoundary` means access order is still meaningful, but the
/// shared materialized boundary must own the final windowing/sort behavior.
/// `MaterializedFallback` means the route must fail closed and materialize
/// without ordered streaming assumptions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum LoadOrderRouteContract {
    DirectStreaming,
    MaterializedBoundary,
    MaterializedFallback,
}

impl LoadOrderRouteContract {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::DirectStreaming => "direct_streaming",
            Self::MaterializedBoundary => "materialized_boundary",
            Self::MaterializedFallback => "materialized_fallback",
        }
    }

    #[must_use]
    pub(in crate::db) const fn allows_streaming_load(self) -> bool {
        matches!(self, Self::DirectStreaming)
    }

    #[must_use]
    pub(in crate::db) const fn allows_top_n_seek(self) -> bool {
        matches!(self, Self::DirectStreaming)
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
pub(in crate::db) enum LoadOrderRouteReason {
    None,
    RequiresMaterializedSort,
    ResidualPredicateBlocksDirectStreaming,
    DistinctRequiresMaterialization,
    DescendingNonUniqueSecondaryPrefixNotAdmitted,
}

impl LoadOrderRouteReason {
    #[must_use]
    pub(in crate::db) const fn code(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RequiresMaterializedSort => "requires_materialized_sort",
            Self::ResidualPredicateBlocksDirectStreaming => {
                "residual_predicate_blocks_direct_streaming"
            }
            Self::DistinctRequiresMaterialization => "distinct_requires_materialization",
            Self::DescendingNonUniqueSecondaryPrefixNotAdmitted => {
                "descending_non_unique_secondary_prefix_not_admitted"
            }
        }
    }
}

///
/// RouteExecutionMode
///
/// Canonical route-level execution shape selected by the routing gate.
/// Keeps streaming-vs-materialized decisions explicit and testable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum RouteExecutionMode {
    Streaming,
    Materialized,
}

///
/// GroupedExecutionStrategy
///
/// Canonical grouped execution strategy label selected by route planning.
/// Variants are runtime-truthful and explicitly mark materialized execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedExecutionStrategy {
    HashMaterialized,
    OrderedMaterialized,
}

impl GroupedExecutionStrategy {
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
    #[must_use]
    pub(in crate::db::executor::route) const fn new(fetch: usize) -> Self {
        Self { fetch }
    }

    #[must_use]
    pub(in crate::db::executor) const fn fetch(self) -> usize {
        self.fetch
    }
}

///
/// ExecutionModeRouteCase
///
/// Canonical route-case partition for execution-mode decisions.
/// This keeps streaming/materialized branching explicit under one gate.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ExecutionModeRouteCase {
    Load,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
}

///
/// ExecutionRouteShape
///
/// Canonical executor-facing route shape descriptor.
/// This carries only shape axes (kind + mode + mode case) so runtime consumers
/// can make shape decisions without reaching through the full route payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ExecutionRouteShape {
    route_shape_kind: RouteShapeKind,
    execution_mode: RouteExecutionMode,
    execution_mode_case: ExecutionModeRouteCase,
}

impl ExecutionRouteShape {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn execution_mode_case(self) -> ExecutionModeRouteCase {
        self.execution_mode_case
    }

    #[must_use]
    pub(in crate::db::executor) const fn execution_mode(self) -> RouteExecutionMode {
        self.execution_mode
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_streaming(self) -> bool {
        matches!(self.execution_mode, RouteExecutionMode::Streaming)
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_materialized(self) -> bool {
        matches!(self.execution_mode, RouteExecutionMode::Materialized)
    }
}
