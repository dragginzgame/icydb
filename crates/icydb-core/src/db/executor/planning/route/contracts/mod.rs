//! Module: db::executor::planning::route::contracts
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
#[cfg(test)]
pub(in crate::db::executor) use execution::GroupedRouteDecisionOutcome;
pub use execution::RouteExecutionMode;
pub(in crate::db::executor) use execution::{
    AggregateSeekSpec, ExecutionRoutePlan, GroupedExecutionMode, GroupedExecutionModeProjection,
    IndexRangeLimitSpec, ScanHintPlan, TopNSeekSpec,
};
pub(in crate::db) use execution::{LoadOrderRouteContract, LoadOrderRouteReason};
pub(in crate::db) use shape::AggregateRouteShape;
pub(in crate::db::executor::planning::route) use shape::RouteIntent;
pub(in crate::db::executor) use shape::{
    AGGREGATE_FAST_PATH_ORDER, FastPathOrder, GROUPED_AGGREGATE_FAST_PATH_ORDER,
    LOAD_FAST_PATH_ORDER, RouteShapeKind,
};
