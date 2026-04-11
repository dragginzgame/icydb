//! Module: db::executor::pipeline::contracts::grouped::route_stage
//! Re-exports grouped route-stage contracts shared between planning and
//! grouped runtime execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod payload;
mod projection;

pub(in crate::db::executor) use payload::{
    GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, IndexSpecBundle,
};
