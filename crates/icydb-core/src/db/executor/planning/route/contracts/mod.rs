//! Module: executor::planning::route::contracts
//! Responsibility: route-owned contracts, capability facts, and precedence constants.
//! Does not own: capability derivation algorithms or route planning flow.
//! Boundary: shared immutable route types consumed by route submodules and executor runtime.

mod capability_facts;
mod execution;
mod shape;

#[cfg(feature = "sql")]
pub(in crate::db::executor) use crate::db::executor::ContinuationMode;
pub(in crate::db::executor) use crate::db::executor::{AccessWindow, RouteContinuationPlan};
pub(in crate::db::executor) use capability_facts::RouteCapabilityFacts;
pub use execution::RouteExecutionMode;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use execution::RouteExplainFacts;
pub(in crate::db::executor) use execution::{
    AggregateSeekSpec, ExecutionRoutePlan, GroupedExecutionMode, GroupedExecutionModeContext,
    IndexPrefixChildExpansionBudget, IndexPrefixChildExpansionHint, IndexRangeLimitSpec,
    LoadOrderRouteDecision, LoadOrderRouteMode, LoadOrderRouteReason, ScanHintPlan, TopNSeekSpec,
};
pub(in crate::db) use shape::AggregateRouteShape;
pub(in crate::db::executor) use shape::{
    AGGREGATE_FAST_PATH_ORDER, FastPathOrder, GROUPED_AGGREGATE_FAST_PATH_ORDER,
    LOAD_FAST_PATH_ORDER, RouteShapeKind,
};
