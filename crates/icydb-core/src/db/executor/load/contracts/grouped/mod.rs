//! Module: executor::load::contracts::grouped
//! Responsibility: grouped load-stage contracts and projection traits.
//! Does not own: planner semantic derivation or scalar load contracts.
//! Boundary: grouped planner/route/stream/fold payloads consumed by grouped runtime.

mod route_stage;
mod stages;

pub(in crate::db::executor::load) use route_stage::{
    GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, GroupedRouteStageProjection,
    IndexSpecBundle,
};
pub(in crate::db::executor::load) use stages::{GroupedFoldStage, GroupedStreamStage};
