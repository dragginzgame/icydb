//! Module: db::executor::planning::route::planner
//! Responsibility: derive immutable execution-route plans from validated inputs.
//! Does not own: logical plan construction or physical key-stream execution.
//! Boundary: route planning contracts consumed by load/aggregate/mutation executors.

mod entrypoints;
mod execution;
mod feasibility;
mod intent;
mod stages;

pub(in crate::db::executor) use entrypoints::{RoutePlanRequest, build_execution_route_plan};
pub(in crate::db::executor::planning::route::planner) use feasibility::derive_execution_feasibility_stage_for_model;
pub(in crate::db::executor::planning::route::planner) use feasibility::derive_mutation_execution_feasibility_stage_for_model;
pub(in crate::db::executor::planning::route::planner) use intent::{
    derive_aggregate_route_intent_stage, derive_grouped_route_intent_stage,
    derive_load_route_intent_stage, derive_mutation_route_intent_stage,
};
pub(in crate::db::executor::planning::route::planner) use stages::{
    RouteDerivationContext, RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
    build_execution_route_plan_from_stages,
};
