//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: owns context artifacts and payload state contracts for load
//! runtime orchestration.
//! Does not own: terminal materialization logic or strategy dispatch.
//! Boundary: exports the runtime state seams shared by the monomorphic load path.

mod context;

pub(super) use context::{
    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload, LoadPayloadState,
};
