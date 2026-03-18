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
#[cfg(test)]
pub(in crate::db::executor) use capabilities::FieldExtremaIneligibilityReason;
pub(in crate::db::executor) use capabilities::RouteCapabilities;
#[cfg(test)]
pub(in crate::db::executor) use capabilities::route_capability_flag_count_guard;
#[cfg(test)]
pub(in crate::db::executor) use execution::route_execution_mode_case_count_guard;
pub(in crate::db::executor) use execution::{
    AggregateSeekSpec, ExecutionModeRouteCase, ExecutionRoutePlan, ExecutionRouteShape,
    GroupedExecutionStrategy, GroupedRouteDecisionOutcome, IndexRangeLimitSpec, RouteExecutionMode,
    ScanHintPlan, TopNSeekSpec,
};
// Keep this symbol on the route boundary so compile-fail privacy tests fail on
// module visibility, not unresolved-symbol drift.
pub(in crate::db::executor) use execution::GroupedRouteObservability;
#[cfg(test)]
pub(in crate::db::executor) use shape::MUTATION_FAST_PATH_ORDER;
pub(in crate::db::executor::route) use shape::RouteIntent;
#[cfg(test)]
pub(in crate::db::executor) use shape::route_shape_kind_count_guard;
pub(in crate::db::executor) use shape::{
    AGGREGATE_FAST_PATH_ORDER, FastPathOrder, GROUPED_AGGREGATE_FAST_PATH_ORDER,
    LOAD_FAST_PATH_ORDER, RouteShapeKind, RoutedKeyStreamRequest,
};
