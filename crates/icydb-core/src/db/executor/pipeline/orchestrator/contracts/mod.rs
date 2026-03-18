//! Module: executor::pipeline::orchestrator::contracts
//! Responsibility: canonical mode and erased-surface contracts for load orchestration.
//! Does not own: runtime orchestration mechanics.
//! Boundary: defines stable load mode/surface semantics consumed by entrypoints
//! and monomorphic runtime wiring.

mod mode;
mod surface;

pub(in crate::db::executor) use mode::{LoadExecutionMode, LoadTracingMode};
pub(in crate::db::executor) use surface::{ErasedLoadExecutionSurface, ErasedLoadPayload};
