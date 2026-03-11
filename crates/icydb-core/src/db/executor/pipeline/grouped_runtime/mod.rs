//! Module: executor::pipeline::grouped_runtime
//! Responsibility: grouped runtime projection/orchestration over continuation-owned contracts.
//! Does not own: grouped continuation contract authority, planner policy derivation, or route feasibility.
//! Boundary: pipeline-owned grouped runtime assembly and execution projection.

mod route_stage;
mod runtime;

pub(in crate::db::executor) use crate::db::executor::{
    GroupedContinuationCapabilities, GroupedContinuationContext, GroupedPaginationWindow,
};
pub(in crate::db::executor) use runtime::{GroupedExecutionContext, GroupedRuntimeProjection};
