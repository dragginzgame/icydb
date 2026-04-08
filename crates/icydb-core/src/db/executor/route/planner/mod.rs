//! Module: executor::route::planner
//! Responsibility: derive immutable execution-route plans from validated inputs.
//! Does not own: logical plan construction or physical key-stream execution.
//! Boundary: route planning contracts consumed by load/aggregate/mutation executors.

mod entrypoints;
mod execution;
mod feasibility;
mod intent;
mod stages;

pub(in crate::db::executor) use entrypoints::build_execution_route_plan_for_aggregate_spec_with_model;
pub(in crate::db::executor) use entrypoints::build_execution_route_plan_for_grouped_plan;
pub(in crate::db::executor) use entrypoints::build_execution_route_plan_for_load_with_model;
pub(in crate::db::executor) use entrypoints::build_execution_route_plan_for_mutation_with_model;
pub(in crate::db::executor) use entrypoints::build_initial_execution_route_plan_for_load_with_model;
pub(in crate::db::executor::route::planner) use execution::derive_route_execution_stage;
pub(in crate::db::executor::route::planner) use feasibility::derive_execution_feasibility_stage_for_model;
pub(in crate::db::executor::route::planner) use intent::derive_route_intent_stage;
pub(in crate::db::executor::route::planner) use stages::{
    RouteDerivationContext, RouteExecutionStage, RouteFeasibilityStage, RouteIntentStage,
};
