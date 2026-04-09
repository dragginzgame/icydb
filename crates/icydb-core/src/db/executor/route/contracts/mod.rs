//! Module: db::executor::route::contracts
//! Responsibility: route-owned contracts, capability snapshots, and precedence constants.
//! Does not own: capability derivation algorithms or route planning flow.
//! Boundary: shared immutable route types consumed by route submodules and executor runtime.

mod capabilities;
mod execution;
mod shape;

pub(in crate::db::executor) use crate::db::executor::{
    AccessWindow, ContinuationMode, RouteContinuationPlan,
};
pub(in crate::db::executor) use capabilities::RouteCapabilities;
pub(in crate::db::executor) use execution::{
    AggregateSeekSpec, ExecutionModeRouteCase, ExecutionRoutePlan, ExecutionRouteShape,
    GroupedExecutionStrategy, GroupedRouteDecisionOutcome, IndexRangeLimitSpec, RouteExecutionMode,
    ScanHintPlan, TopNSeekSpec,
};
pub(in crate::db) use execution::{LoadOrderRouteContract, LoadOrderRouteReason};
// Keep this symbol on the route boundary so compile-fail privacy tests fail on
// module visibility, not unresolved-symbol drift.
pub(in crate::db::executor) use execution::GroupedRouteObservability;
pub(in crate::db) use shape::AggregateRouteShape;
pub(in crate::db::executor::route) use shape::RouteIntent;
pub(in crate::db::executor) use shape::{
    AGGREGATE_FAST_PATH_ORDER, FastPathOrder, GROUPED_AGGREGATE_FAST_PATH_ORDER,
    LOAD_FAST_PATH_ORDER, RouteShapeKind,
};
