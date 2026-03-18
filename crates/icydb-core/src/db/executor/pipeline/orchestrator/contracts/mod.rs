//! Module: executor::pipeline::orchestrator::contracts
//! Responsibility: canonical mode and surface contracts for load orchestration.
//! Does not own: stage orchestration loop or dispatch mechanics.
//! Boundary: defines stable load mode/surface semantics consumed by entrypoints and stages.

mod mode;
mod runtime;
mod surface;

pub(in crate::db::executor) use mode::{LoadExecutionMode, LoadTracingMode};
pub(in crate::db::executor) use runtime::LoadExecutionDescriptor;
pub(in crate::db::executor) use surface::{
    ErasedLoadExecutionSurface, ErasedLoadPayload, LoadExecutionSurface,
};
