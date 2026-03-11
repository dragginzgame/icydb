//! Module: db::executor::shared::load_contracts::grouped::route_stage
//! Responsibility: module-local ownership and contracts for db::executor::shared::load_contracts::grouped::route_stage.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod payload;
mod projection;

pub(in crate::db::executor) use payload::{
    GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, IndexSpecBundle,
};
pub(in crate::db::executor) use projection::GroupedRouteStageProjection;
