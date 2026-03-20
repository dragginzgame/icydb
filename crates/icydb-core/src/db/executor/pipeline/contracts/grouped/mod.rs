//! Module: executor::pipeline::contracts::grouped
//! Responsibility: grouped load-stage contracts and route-stage projections.
//! Does not own: planner semantic derivation or scalar load contracts.
//! Boundary: grouped planner/route/stream/fold payloads consumed by grouped runtime.

mod route_stage;
mod stages;

pub(in crate::db::executor) use route_stage::{
    GroupedPlannerPayload, GroupedRoutePayload, GroupedRouteStage, IndexSpecBundle,
};
pub(in crate::db::executor) use stages::{
    GroupedFoldStage, GroupedRowRuntime, GroupedStreamStage, RowView, StructuralGroupedRowRuntime,
};
