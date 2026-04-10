//! Module: executor::pipeline::grouped_runtime
//! Responsibility: grouped runtime projection/orchestration over continuation-owned contracts.
//! Does not own: grouped continuation contract authority, planner policy derivation, or route feasibility.
//! Boundary: pipeline-owned grouped runtime assembly and execution projection.

mod route_stage;
mod runtime;

pub(in crate::db::executor) use route_stage::resolve_grouped_route_for_plan;
pub(in crate::db::executor) use runtime::GroupedExecutionContext;
