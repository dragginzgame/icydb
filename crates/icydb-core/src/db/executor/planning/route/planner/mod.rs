//! Module: executor::planning::route::planner
//! Responsibility: derive immutable execution-route plans from validated inputs.
//! Does not own: logical plan construction or physical key-stream execution.
//! Boundary: route planning contracts consumed by load/aggregate/mutation executors.

mod entrypoints;
mod execution;
mod feasibility;
mod intent;
mod stages;

#[cfg(feature = "sql-explain")]
pub(in crate::db::executor) use entrypoints::build_aggregate_execution_route_plan_for_explain;
pub(in crate::db::executor) use entrypoints::{RoutePlanRequest, build_execution_route_plan};
use feasibility::derive_execution_feasibility_stage_for_model;
#[cfg(feature = "sql-explain")]
use intent::derive_aggregate_route_intent_stage;
use intent::{derive_grouped_route_intent_stage, derive_load_route_intent_stage};
use stages::{
    RouteDerivationContext, RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
    build_execution_route_plan_from_stages,
};
