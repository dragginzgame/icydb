//! Module: executor::pipeline::grouped_runtime
//! Responsibility: grouped runtime route-stage assembly for grouped execution.
//! Does not own: grouped continuation contract authority, planner policy derivation, or grouped fold/output mechanics.
//! Boundary: grouped runtime module wiring and grouped route-stage construction.

#[cfg(feature = "query")]
mod route_stage;

#[cfg(feature = "query")]
pub(in crate::db::executor) use route_stage::resolve_grouped_route_for_plan;
